package com.transferwise.tasks.testapp;

import static org.awaitility.Awaitility.await;

import com.transferwise.common.context.UnitOfWorkManager;
import com.transferwise.tasks.BaseIntTest;
import com.transferwise.tasks.config.TwTasksKafkaConfiguration;
import com.transferwise.tasks.helpers.kafka.ConsistentKafkaConsumer;
import java.time.Duration;
import java.util.Collections;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;

@Slf4j
class KafkaIntTest extends BaseIntTest {

  @Autowired
  private KafkaTemplate<String, String> kafkaTemplate;
  @Autowired
  private TwTasksKafkaConfiguration kafkaConfiguration;
  @Autowired
  private UnitOfWorkManager unitOfWorkManager;

  private final Duration delayTimeout = Duration.ofMillis(5);

  @ParameterizedTest(name = "flaky messages accepter will not stop the processing, iteration {0}")
  @ValueSource(ints = {0, 1, 2, 3})
  void flakyMessagesAccepterWillNotStopTheProcessing(int iteration) {
    String topic = "toKafkaBatchTestTopic2";
    int n = 10;

    Map<String, AtomicInteger> messagesMap = new ConcurrentHashMap<>();
    for (int i = 0; i < n; i++) {
      messagesMap.put("Message " + iteration + ":" + i, new AtomicInteger());
    }

    for (int i = 0; i < n; i++) {
      kafkaTemplate.send(topic, String.valueOf(i), "Message " + iteration + ":" + i);
    }

    long start = System.currentTimeMillis();
    new ConsistentKafkaConsumer<String>()
        .setTopics(Collections.singletonList(topic))
        .setDelayTimeout(delayTimeout)
        .setShouldPollPredicate(() -> true)
        .setShouldFinishPredicate(
            () -> messagesMap.values().stream().noneMatch(v -> v.get() != 1) || System.currentTimeMillis() - start > 30000
        )
        .setKafkaPropertiesSupplier(() -> kafkaConfiguration.getKafkaProperties().buildConsumerProperties())
        .setRecordConsumer(record -> {
          if (Math.random() < 0.5) {
            throw new RuntimeException("Unlucky!");
          }
          if (!record.value().equals("Warmup")) {
            messagesMap.get(record.value()).incrementAndGet();
          }
        })
        .setUnitOfWorkManager(unitOfWorkManager)
        .consume();

    await().until(() ->
        messagesMap.values().stream().noneMatch(v -> v.get() != 1)
    );
  }
}
