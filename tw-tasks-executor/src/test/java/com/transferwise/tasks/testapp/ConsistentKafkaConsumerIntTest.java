package com.transferwise.tasks.testapp;

import com.transferwise.tasks.BaseIntTest;
import com.transferwise.tasks.config.TwTasksKafkaConfiguration;
import com.transferwise.tasks.helpers.kafka.ConsistentKafkaConsumer;
import com.transferwise.tasks.impl.tokafka.test.IToKafkaTestHelper;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.api.Assertions.assertEquals;

@Slf4j
class ConsistentKafkaConsumerIntTest extends BaseIntTest {

  @Autowired
  private IToKafkaTestHelper toKafkaTestHelper;
  @Autowired
  private TwTasksKafkaConfiguration kafkaConfiguration;

  @Test
  void allMessagesWillBeReceivedOnceOnRebalancing() throws Exception {
    int N = 10;
    Map<String, AtomicInteger> messagesReceivedCounts = new HashMap<>();

    String testTopic = "ConsistentKafkaConsumerIntSpec";
    for (int i = 0; i < N; i++) {
      String message = "Message" + i;
      messagesReceivedCounts.put(message, new AtomicInteger(0));
      toKafkaTestHelper.sendDirectKafkaMessage(testTopic, message);
    }

    AtomicBoolean shouldFinish = new AtomicBoolean(false);

    ConsistentKafkaConsumer<String> consumer = new ConsistentKafkaConsumer<String>()
        .setKafkaPropertiesSupplier(() -> kafkaConfiguration.getKafkaProperties().buildConsumerProperties())
        .setTopics(Collections.singletonList(testTopic))
        .setShouldFinishPredicate(shouldFinish::get)
        .setShouldPollPredicate(() -> !shouldFinish.get())
        .setRecordConsumer(consumerRecord -> {
          log.info("Received message '{}': {}.", consumerRecord.value(), messagesReceivedCounts.get(consumerRecord.value()).incrementAndGet());
        });

    Thread consumerThread = new Thread(consumer::consume);
    consumerThread.start();

    await().until(() ->
        messagesReceivedCounts.values().stream().mapToInt(AtomicInteger::get).sum() >= N
    );

    shouldFinish.set(true);
    consumerThread.join();

    // we received all messages and only once
    for (AtomicInteger value: messagesReceivedCounts.values()) {
      assertEquals(1, value.get());
    }

    // another kafka consumer starts up
    String message = "Message" + N;
    messagesReceivedCounts.put(message, new AtomicInteger(0));
    toKafkaTestHelper.sendDirectKafkaMessage(testTopic, message);

    shouldFinish.set(false);

    consumerThread = new Thread(consumer::consume);
    consumerThread.start();

    await().until(() ->
        messagesReceivedCounts.values().stream().mapToInt(AtomicInteger::get).sum() > N
    );

    shouldFinish.set(true);
    consumerThread.join();

    // we received no old messages again, but new message was received correctly
    for (AtomicInteger value : messagesReceivedCounts.values()) {
      assertEquals(1, value.get());
    }
    for (int i = 0; i < N + 1; i++) {
      assertEquals(1, messagesReceivedCounts.get("Message" + i).get());
    }
  }
}
