package com.transferwise.tasks.testapp;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.api.Assertions.assertEquals;

import com.transferwise.common.baseutils.transactionsmanagement.ITransactionsHelper;
import com.transferwise.tasks.BaseIntTest;
import com.transferwise.tasks.config.TwTasksKafkaConfiguration;
import com.transferwise.tasks.helpers.kafka.ConsistentKafkaConsumer;
import com.transferwise.tasks.helpers.kafka.ITopicPartitionsManager;
import com.transferwise.tasks.impl.tokafka.IToKafkaSenderService;
import com.transferwise.tasks.impl.tokafka.IToKafkaSenderService.SendMessagesRequest;
import java.time.Duration;
import java.util.Collections;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.springframework.beans.factory.annotation.Autowired;

@Slf4j
class KafkaIntTest extends BaseIntTest {

  @Autowired
  private TwTasksKafkaConfiguration kafkaConfiguration;
  @Autowired
  private IToKafkaSenderService toKafkaSenderService;
  @Autowired
  private ITransactionsHelper transactionsHelper;
  @Autowired
  private ITopicPartitionsManager topicPartitionsManager;

  @Test
  void sendingAMessageToKafkaWorks() {
    String topic = "ToKafkaTest";
    StringBuilder sb = new StringBuilder();
    // 10MB, We want to see what happens to big message.
    for (int i = 0; i < 999 * 1000; i++) {
      sb.append("Hello Worl");
    }
    String payload = sb.toString();

    transactionsHelper.withTransaction().asNew().call(() -> {
      toKafkaSenderService.sendMessage(
          new IToKafkaSenderService.SendMessageRequest()
              .setTopic(topic)
              .setPayloadString(payload)
      );
      return null;
    });

    AtomicInteger messagesReceivedCount = new AtomicInteger();
    long start = System.currentTimeMillis();
    new ConsistentKafkaConsumer<String>()
        .setTopics(Collections.singletonList(topic))
        .setDelayTimeout(Duration.ofSeconds(1))
        .setShouldPollPredicate(() -> true)
        .setShouldFinishPredicate(() ->
            messagesReceivedCount.get() == 1 || System.currentTimeMillis() - start > 30000
        )
        .setKafkaPropertiesSupplier(() -> kafkaConfiguration.getKafkaProperties().buildConsumerProperties())
        .setRecordConsumer(record -> {
          if (record.value().equals(payload)) {
            messagesReceivedCount.incrementAndGet();
          }
        })
        .consume();

    assertEquals(1, messagesReceivedCount.get());
  }

  @ParameterizedTest(name = "sending batch messages to kafka works, iteration {0}")
  @ValueSource(ints = {0, 1, 2, 3})
  void sendingBatchMessagesToKafkaWorks(int iteration) {
    String topic = "toKafkaBatchTestTopic";
    int n = 1000;

    Map<String, AtomicInteger> messagesMap = new ConcurrentHashMap<>();
    for (int i = 0; i < n; i++) {
      messagesMap.put("Message " + iteration + ":" + i, new AtomicInteger());
    }

    transactionsHelper.withTransaction().asNew().call(() -> {
      SendMessagesRequest messages = new SendMessagesRequest().setTopic(topic);
      for (int i = 0; i < n; i++) {
        messages.add(
            new IToKafkaSenderService.SendMessagesRequest.Message()
                .setPayloadString("Message " + iteration + ":" + i)
                .setKey(String.valueOf(i))
        );
      }
      toKafkaSenderService.sendMessages(messages);
      return null;
    });

    long start = System.currentTimeMillis();
    new ConsistentKafkaConsumer<String>()
        .setTopics(Collections.singletonList(topic))
        .setDelayTimeout(Duration.ofSeconds(1))
        .setShouldPollPredicate(() -> true)
        .setShouldFinishPredicate(
            () -> messagesMap.values().stream().noneMatch(v -> v.get() != 1) || System.currentTimeMillis() - start > 30000
        )
        .setKafkaPropertiesSupplier(() -> kafkaConfiguration.getKafkaProperties().buildConsumerProperties())
        .setRecordConsumer(record -> {
          if (record.offset() < iteration * 1000 || record.offset() > (iteration + 1) * 1000 - 1) {
            throw new IllegalStateException("Unexpected offset detected for iteration " + iteration + ": " + iteration);
          }
          if (!record.value().equals("Warmup")) {
            messagesMap.get(record.value()).incrementAndGet();
          }
        })
        .consume();

    await().until(() ->
        messagesMap.values().stream().noneMatch(v -> v.get() != 1)
    );
  }

  /**
   * Works well with `org.apache.kafka.clients.producer.RoundRobinPartitioner`.
   * 
   * <p>Can be flaky with `org.apache.kafka.clients.producer.internals.DefaultPartitioner`.
   */
  @ParameterizedTest(name = "sending batch messages to Kafka works with 5 partitions, iteration {0}")
  @ValueSource(ints = {0, 1, 2, 3})
  @SneakyThrows
  void sendingBatchMessagesToKafkaWorksWith5Partitions(int iteration) {
    String topic = "toKafkaBatchTestTopic5Partitions";
    int n = 10000;

    Map<String, AtomicInteger> messagesMap = new ConcurrentHashMap<>();
    for (int i = 0; i < n; i++) {
      messagesMap.put("Message " + iteration + ":" + i, new AtomicInteger());
    }

    Map<Integer, AtomicInteger> partitionsMap = new ConcurrentHashMap<>();

    transactionsHelper.withTransaction().asNew().call(() -> {
      SendMessagesRequest messages = new IToKafkaSenderService.SendMessagesRequest().setTopic(topic);
      for (int i = 0; i < n; i++) {
        messages.add(
            new IToKafkaSenderService.SendMessagesRequest.Message()
                .setPayloadString("Message " + iteration + ":" + i)
        );
      }
      toKafkaSenderService.sendMessages(messages);
      return null;
    });

    long start = System.currentTimeMillis();
    new ConsistentKafkaConsumer<String>()
        .setTopics(Collections.singletonList(topic))
        .setDelayTimeout(Duration.ofSeconds(1))
        .setShouldPollPredicate(() -> true)
        .setShouldFinishPredicate(
            () -> messagesMap.values().stream().noneMatch(v -> v.get() != 1) || System.currentTimeMillis() - start > 30000
        )
        .setKafkaPropertiesSupplier(() -> kafkaConfiguration.getKafkaProperties().buildConsumerProperties())
        .setRecordConsumer(record -> {
          if (!record.value().equals("Warmup")) {
            messagesMap.get(record.value()).incrementAndGet();
          }
          partitionsMap.computeIfAbsent(record.partition(), k -> new AtomicInteger()).incrementAndGet();
        })
        .consume();

    await().until(() ->
        messagesMap.values().stream().noneMatch(v -> v.get() != 1)
    );
    for (int i = 0; i < 5; i++) {
      assertThat(partitionsMap.get(i)).as("Partition " + i + " is not empty.").isNotNull();
      assertThat(partitionsMap.get(i).get()).as("Partition " + i + " is not empty.").isNotNull().isGreaterThan(0);
    }
  }

  @ParameterizedTest(name = "flaky messages accepter will not stop the processing, iteration {0}")
  @ValueSource(ints = {0, 1, 2, 3})
  void flakyMessagesAccepterWillNotStopTheProcessing(int iteration) {
    String topic = "toKafkaBatchTestTopic2";
    int n = 10;

    Map<String, AtomicInteger> messagesMap = new ConcurrentHashMap<>();
    for (int i = 0; i < n; i++) {
      messagesMap.put("Message " + iteration + ":" + i, new AtomicInteger());
    }

    transactionsHelper.withTransaction().asNew().call(() -> {
      SendMessagesRequest messages = new IToKafkaSenderService.SendMessagesRequest().setTopic(topic);
      for (int i = 0; i < n; i++) {
        messages.add(
            new IToKafkaSenderService.SendMessagesRequest.Message()
                .setPayloadString("Message " + iteration + ":" + i)
                .setKey(String.valueOf(i))
        );
      }
      toKafkaSenderService.sendMessages(messages);
      return null;
    });

    long start = System.currentTimeMillis();
    new ConsistentKafkaConsumer<String>()
        .setTopics(Collections.singletonList(topic))
        .setDelayTimeout(Duration.ofSeconds(1))
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
        .consume();

    await().until(() ->
        messagesMap.values().stream().noneMatch(v -> v.get() != 1)
    );
  }
}
