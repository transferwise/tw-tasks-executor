package com.transferwise.tasks.triggering;

import static org.awaitility.Awaitility.await;

import com.google.common.collect.ImmutableSet;
import com.transferwise.tasks.BaseIntTest;
import com.transferwise.tasks.TasksProperties;
import java.time.Duration;
import java.time.Instant;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;

public class SeekToDurationOnRebalanceIntTest extends BaseIntTest {

  private static final String TOPIC = "SeekToDurationOnRebalanceIntTest";

  @Autowired
  private TasksProperties tasksProperties;

  private AdminClient adminClient;
  private KafkaConsumer<String, String> kafkaConsumer;
  private KafkaProducer<String, String> kafkaProducer;

  @BeforeEach
  void setup() throws Exception {
    Map<String, Object> configs = new HashMap<>();
    configs.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, tasksProperties.getTriggering().getKafka().getBootstrapServers());

    adminClient = AdminClient.create(configs);
    NewTopic topic = new NewTopic(TOPIC, 3, (short) 1);
    adminClient.createTopics(Collections.singletonList(topic)).all().get(5, TimeUnit.SECONDS);

    Map<String, Object> consumerProperties = new HashMap<>(configs);
    consumerProperties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "none");
    consumerProperties.put(ConsumerConfig.GROUP_ID_CONFIG, "SeekToDurationOnRebalanceConsumer");
    consumerProperties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
    consumerProperties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
    kafkaConsumer = new KafkaConsumer<>(consumerProperties);

    Map<String, Object> producerProperties = new HashMap<>(configs);
    producerProperties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
    producerProperties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
    kafkaProducer = new KafkaProducer<>(producerProperties);
  }

  @AfterEach
  void cleanup() {
    cleanWithoutException(() -> {
      kafkaConsumer.close();
    });
    cleanWithoutException(() -> {
      adminClient.deleteTopics(Collections.singletonList(TOPIC));
      adminClient.close();
    });
    cleanWithoutException(() -> {
      kafkaProducer.close();
    });
  }

  @Test
  void resetsOffsetsToDurationOnRebalance() {
    Instant now = Instant.now();
    sendKafkaMessage(TOPIC, 0, now.minus(Duration.ofDays(10)).toEpochMilli(), "key", "1"); // older, won't be consumed
    sendKafkaMessage(TOPIC, 0, now.minus(Duration.ofDays(3)).toEpochMilli(), "key", "2");
    sendKafkaMessage(TOPIC, 0, now.minus(Duration.ofDays(2)).toEpochMilli(), "key", "3");
    sendKafkaMessage(TOPIC, 1, now.minus(Duration.ofDays(7)).toEpochMilli(), "key", "4"); // older, won't be consumed
    sendKafkaMessage(TOPIC, 1, now.minus(Duration.ofDays(6)).toEpochMilli(), "key", "5"); // older, won't be consumed
    sendKafkaMessage(TOPIC, 1, now.minus(Duration.ofDays(2)).toEpochMilli(), "key", "6");
    sendKafkaMessage(TOPIC, 1, now.minus(Duration.ofDays(1)).toEpochMilli(), "key", "7");
    sendKafkaMessage(TOPIC, 2, now.minus(Duration.ofHours(1)).toEpochMilli(), "key", "8");
    sendKafkaMessage(TOPIC, 2, now.minus(Duration.ofHours(1)).toEpochMilli(), "key", "9");
    sendKafkaMessage(TOPIC, 2, now.minus(Duration.ofHours(1)).toEpochMilli(), "key", "10");

    kafkaConsumer.subscribe(Collections.singletonList(TOPIC), new SeekToDurationOnRebalance(kafkaConsumer, Duration.ofDays(4).negated()));

    Set<String> values = new HashSet<>();
    await().until(
        () -> {
          for (ConsumerRecord<String, String> record : kafkaConsumer.poll(Duration.ofSeconds(5))) {
            values.add(record.value());
          }
          return values.equals(ImmutableSet.of("2", "3", "6", "7", "8", "9", "10"));
        }
    );
  }

  private void sendKafkaMessage(String topic, int partition, long timestamp, String key, String value) {
    kafkaProducer.send(new ProducerRecord<>(topic, partition, timestamp, key, value));
  }
}
