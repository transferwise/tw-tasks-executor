package com.transferwise.tasks.testapp;

import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.api.Assertions.assertEquals;

import com.transferwise.common.context.UnitOfWorkManager;
import com.transferwise.tasks.BaseIntTest;
import com.transferwise.tasks.helpers.kafka.ConsistentKafkaConsumer;
import java.time.Duration;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.OffsetSpec.LatestSpec;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.kafka.KafkaProperties;
import org.springframework.kafka.core.KafkaTemplate;

@Slf4j
class ConsistentKafkaConsumerIntTest extends BaseIntTest {

  @Autowired
  private KafkaTemplate<String, String> kafkaTemplate;
  @Autowired
  private UnitOfWorkManager unitOfWorkManager;
  @Autowired
  private KafkaProperties kafkaProperties;

  private final Duration delayTimeout = Duration.ofMillis(5);

  @Test
  void allMessagesWillBeReceivedOnceOnRebalancing() throws Exception {
    int n = 10;
    String testTopic = "allMessagesWillBeReceivedOnceOnRebalancing";
    // Using a separate group reduces test times greatly as other kafka consumers don't have to go through
    // the expensive rebalancing.
    String groupId = "allMessagesWillBeReceivedOnceOnRebalancing";

    Map<String, AtomicInteger> messagesReceivedCounts = new HashMap<>();

    for (int i = 0; i < n; i++) {
      String message = "Message" + i;
      messagesReceivedCounts.put(message, new AtomicInteger(0));
      kafkaTemplate.send(testTopic, message);
    }

    AtomicBoolean shouldFinish = new AtomicBoolean(false);
    var consumerProps = kafkaProperties.buildConsumerProperties();
    consumerProps.put(CommonClientConfigs.GROUP_ID_CONFIG, groupId);

    ConsistentKafkaConsumer<String> consumer = new ConsistentKafkaConsumer<String>()
        .setKafkaPropertiesSupplier(() -> consumerProps)
        .setTopics(Collections.singletonList(testTopic))
        .setShouldFinishPredicate(shouldFinish::get)
        .setShouldPollPredicate(() -> !shouldFinish.get())
        .setUnitOfWorkManager(unitOfWorkManager)
        .setDelayTimeout(delayTimeout)
        .setAutoResetOffsetTo("earliest")
        .setRecordConsumer(consumerRecord -> log
            .info("Received message '{}': {}.", consumerRecord.value(), messagesReceivedCounts.get(consumerRecord.value()).incrementAndGet()));

    Thread consumerThread = new Thread(consumer::consume);
    consumerThread.start();

    await().until(() ->
        messagesReceivedCounts.values().stream().mapToInt(AtomicInteger::get).sum() >= n
    );

    shouldFinish.set(true);
    consumerThread.join();

    // we received all messages and only once
    for (AtomicInteger value : messagesReceivedCounts.values()) {
      assertEquals(1, value.get());
    }

    // another kafka consumer starts up
    String message = "Message" + n;
    messagesReceivedCounts.put(message, new AtomicInteger(0));
    kafkaTemplate.send(testTopic, message);

    shouldFinish.set(false);

    consumerThread = new Thread(consumer::consume);
    consumerThread.start();

    await().until(() ->
        messagesReceivedCounts.values().stream().mapToInt(AtomicInteger::get).sum() > n
    );

    shouldFinish.set(true);
    consumerThread.join();

    // we received no old messages again, but new message was received correctly
    for (AtomicInteger value : messagesReceivedCounts.values()) {
      assertEquals(1, value.get());
    }
    for (int i = 0; i < n + 1; i++) {
      assertEquals(1, messagesReceivedCounts.get("Message" + i).get());
    }
  }

  @ParameterizedTest(name = "flaky messages accepter will not stop the processing, iteration {0}")
  @ValueSource(ints = {0, 1, 2, 3})
  void flakyMessagesAccepterWillNotStopTheProcessing(int iteration) throws Exception {
    String topic = "toKafkaBatchTestTopic2";
    String groupId = "flakyMessagesAccepterWillNotStopTheProcessing" + iteration;
    int n = 10;

    AdminClient adminClient = AdminClient.create(kafkaProperties.buildAdminProperties());
    try {
      TopicPartition tp = new TopicPartition(topic, 0);
      long latestOffset = adminClient.listOffsets(Map.of(tp, new LatestSpec())).partitionResult(tp).get().offset();
      adminClient.alterConsumerGroupOffsets(groupId, Map.of(tp, new OffsetAndMetadata(latestOffset))).all().get();
    } finally {
      adminClient.close();
    }

    Map<String, AtomicInteger> messagesMap = new ConcurrentHashMap<>();
    for (int i = 0; i < n; i++) {
      messagesMap.put("Message " + iteration + ":" + i, new AtomicInteger());
    }

    for (int i = 0; i < n; i++) {
      kafkaTemplate.send(topic, String.valueOf(i), "Message " + iteration + ":" + i);
    }

    long start = System.currentTimeMillis();
    var consumerProps = kafkaProperties.buildConsumerProperties();
    consumerProps.put(CommonClientConfigs.GROUP_ID_CONFIG, groupId);
    new ConsistentKafkaConsumer<String>()
        .setTopics(Collections.singletonList(topic))
        .setDelayTimeout(delayTimeout)
        .setShouldPollPredicate(() -> true)
        .setShouldFinishPredicate(
            () -> messagesMap.values().stream().noneMatch(v -> v.get() != 1) || System.currentTimeMillis() - start > 30000
        )
        .setKafkaPropertiesSupplier(() -> consumerProps)
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
