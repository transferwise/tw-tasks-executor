package com.transferwise.tasks.ext.kafkalistener;

import static com.transferwise.tasks.ext.kafkalistener.KafkaListenerExtTestConfiguration.TOPIC_A;
import static com.transferwise.tasks.ext.kafkalistener.KafkaListenerExtTestConfiguration.TOPIC_B;
import static org.assertj.core.api.Assertions.assertThat;

import com.transferwise.common.baseutils.ExceptionUtils;
import com.transferwise.tasks.BaseIntTest;
import io.micrometer.core.instrument.MeterRegistry;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;

@Slf4j
public class CoreKafkaListenerIntTest extends BaseIntTest {

  @Autowired
  private KafkaTemplate<String, String> kafkaTemplate;

  @Autowired
  private TestMessagesListeners testMessagesListeners;

  @Autowired
  private MeterRegistry meterRegistry;

  @AfterEach
  public void cleanup() {
    meterRegistry.clear();
  }

  @Test
  @SneakyThrows
  void testSendingMessagesToMultipleShards() {
    List<String> receivedValues = new ArrayList<>();

    ITestMessagesListener listener = message -> receivedValues.add(message.value());
    testMessagesListeners.addListener(listener);

    sendDirectMessage(TOPIC_A, "MessageA");
    sendDirectMessage(TOPIC_B, "MessageB");
    try {
      Awaitility.await().until(() -> receivedValues.size() == 2
          && receivedValues.contains("MessageA") && receivedValues.contains("MessageB"));

      assertThat(
          meterRegistry.get("twTasks.coreKafka.processedMessagesCount").tags("topic", TOPIC_A, "shard", "0").counter().count())
          .isEqualTo(1);
      assertThat(
          meterRegistry.get("twTasks.coreKafka.processedMessagesCount").tags("topic", TOPIC_B, "shard", "1").counter().count())
          .isEqualTo(1);
    } finally {
      testMessagesListeners.removeListener(listener);
    }
  }

  @Test
  @SneakyThrows
  void testThatOneTopicProcessingLagDoesNotAffectOthers() {
    AtomicInteger shard0Count = new AtomicInteger();
    AtomicInteger shard1Count = new AtomicInteger();

    CountDownLatch shard0Blocker = new CountDownLatch(1);
    ITestMessagesListener listener = record -> {
      if (record.topic().equals(TOPIC_A)) {
        if (shard0Count.get() == 0) {
          log.info("Blocking shard 0 processing.");
          if (!ExceptionUtils.doUnchecked(() -> shard0Blocker.await(10, TimeUnit.SECONDS))) {
            return;
          }
          log.info("Resuming shard 1 processing.");
        }
        shard0Count.incrementAndGet();
      } else if (record.topic().equals(TOPIC_B)) {
        shard1Count.incrementAndGet();
      } else {
        log.error("Unexpected record received: " + record.value());
      }
    };
    testMessagesListeners.addListener(listener);
    try {
      for (int i = 0; i < 1000; i++) {
        sendDirectMessage(TOPIC_A, "MessageA");
        sendDirectMessage(TOPIC_B, "MessageB");
      }

      Awaitility.await().atMost(Duration.ofSeconds(20)).until(() -> shard1Count.get() == 1000);

      shard0Blocker.countDown();

      Awaitility.await().until(() -> shard0Count.get() == 1000);

    } finally {
      testMessagesListeners.removeListener(listener);
    }
  }

  private void sendDirectMessage(String topic, String value) {
    kafkaTemplate.send(topic, value).whenComplete((result, t) -> {
      if (t != null) {
        log.error("Message sending failed.", t);
      } else {
        log.debug("Message sending succeeded: {}", value);
      }
    });
  }
}
