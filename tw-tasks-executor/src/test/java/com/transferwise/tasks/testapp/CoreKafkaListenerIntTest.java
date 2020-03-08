package com.transferwise.tasks.testapp;

import static com.transferwise.tasks.testapp.config.TestConfiguration.KAFKA_TEST_TOPIC_A;
import static org.awaitility.Awaitility.await;

import com.transferwise.tasks.BaseIntTest;
import com.transferwise.tasks.helpers.kafka.messagetotask.CoreKafkaListener;
import com.transferwise.tasks.impl.tokafka.test.IToKafkaTestHelper;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import java.time.Duration;
import java.util.Collections;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;

class CoreKafkaListenerIntTest extends BaseIntTest {

  @Autowired
  private IToKafkaTestHelper toKafkaTestHelper;

  @Autowired
  private MeterRegistry meterRegistry;

  @Autowired
  private CoreKafkaListener kafkaListener;

  private Thread pollingThread;

  @BeforeEach
  void setup() {
    pollingThread = new Thread(() -> kafkaListener.poll(Collections.singletonList(KAFKA_TEST_TOPIC_A)));
  }

  @AfterEach
  void cleanup() {
    pollingThread.interrupt();
  }

  @Test
  void shouldTrackTheNumberOfMessagesConsumedPerTopic() {
    int N = 10;
    for (int i = 0; i < N; i++) {
      String message = "Message" + i;
      toKafkaTestHelper.sendDirectKafkaMessage(KAFKA_TEST_TOPIC_A, message);
    }

    pollingThread.start();

    await().pollDelay(Duration.ofMillis(500)).until(() ->
        {
          Counter counter = meterRegistry.find("twTasks.coreKafka.processedMessagesCount")
              .tag("topic", KAFKA_TEST_TOPIC_A)
              .counter();
          return counter != null && counter.count() == N;
        }
    );
  }
}
