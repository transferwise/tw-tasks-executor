package com.transferwise.tasks.triggering;

import static org.awaitility.Awaitility.await;

import com.google.common.collect.ImmutableSet;
import com.transferwise.tasks.BaseIntTest;
import com.transferwise.tasks.config.TwTasksKafkaConfiguration;
import java.time.Duration;
import java.time.Instant;
import java.util.Collections;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;

public class SeekToDurationOnRebalanceIntTest extends BaseIntTest {

  private static final String TOPIC = "SeekToDurationOnRebalanceIntTest";

  @Autowired
  private TwTasksKafkaConfiguration kafkaConfiguration;

  private AdminClient adminClient;
  private KafkaConsumer<String, String> kafkaConsumer;

  @BeforeEach
  void setup() throws Exception {
    adminClient = AdminClient.create(kafkaConfiguration.getKafkaProperties().buildAdminProperties());
    NewTopic topic = new NewTopic(TOPIC, 3, (short) 1);
    adminClient.createTopics(Collections.singletonList(topic)).all().get(5, TimeUnit.SECONDS);

    Map<String, Object> consumerProperties = kafkaConfiguration.getKafkaProperties().buildConsumerProperties();
    consumerProperties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "none");
    consumerProperties.put(ConsumerConfig.GROUP_ID_CONFIG, "SeekToDurationOnRebalanceConsumer");
    kafkaConsumer = new KafkaConsumer<>(consumerProperties);
  }

  @AfterEach
  void cleanup() {
    kafkaConsumer.close();
    adminClient.deleteTopics(Collections.singletonList(TOPIC));
    adminClient.close();
  }

  @Test
  void resetsOffsetsToDurationOnRebalance() {
    KafkaTemplate<String, String> kafkaTemplate = kafkaConfiguration.getKafkaTemplate();

    Instant now = Instant.now();
    kafkaTemplate.send(TOPIC, 0, now.minus(Duration.ofDays(10)).toEpochMilli(), "key", "1"); // older, won't be consumed
    kafkaTemplate.send(TOPIC, 0, now.minus(Duration.ofDays(3)).toEpochMilli(), "key", "2");
    kafkaTemplate.send(TOPIC, 0, now.minus(Duration.ofDays(2)).toEpochMilli(), "key", "3");
    kafkaTemplate.send(TOPIC, 1, now.minus(Duration.ofDays(7)).toEpochMilli(), "key", "4"); // older, won't be consumed
    kafkaTemplate.send(TOPIC, 1, now.minus(Duration.ofDays(6)).toEpochMilli(), "key", "5"); // older, won't be consumed
    kafkaTemplate.send(TOPIC, 1, now.minus(Duration.ofDays(2)).toEpochMilli(), "key", "6");
    kafkaTemplate.send(TOPIC, 1, now.minus(Duration.ofDays(1)).toEpochMilli(), "key", "7");
    kafkaTemplate.send(TOPIC, 2, now.minus(Duration.ofHours(1)).toEpochMilli(), "key", "8");
    kafkaTemplate.send(TOPIC, 2, now.minus(Duration.ofHours(1)).toEpochMilli(), "key", "9");
    kafkaTemplate.send(TOPIC, 2, now.minus(Duration.ofHours(1)).toEpochMilli(), "key", "10");

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
}
