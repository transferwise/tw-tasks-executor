package com.transferwise.tasks.triggering;

import static org.awaitility.Awaitility.await;

import com.google.common.collect.ImmutableSet;
import com.transferwise.tasks.BaseIntTest;
import com.transferwise.tasks.TasksProperties;
import com.transferwise.tasks.domain.BaseTask;
import java.time.Duration;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import lombok.SneakyThrows;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;

public class KafkaTasksExecutionTriggererIntTest extends BaseIntTest {

  private static final String TOPIC = "KafkaTestExecutionTriggerIntTest" + RandomStringUtils.randomAlphanumeric(5);

  private KafkaConsumer<String, String> kafkaConsumer;
  private AdminClient adminClient;
  @Autowired
  private TasksProperties tasksProperties;
  @Autowired
  protected ITasksExecutionTriggerer tasksExecutionTriggerer;
  private KafkaTasksExecutionTriggerer subject;

  @BeforeEach
  @SneakyThrows
  void setup() {
    subject = (KafkaTasksExecutionTriggerer) tasksExecutionTriggerer;

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
  }

  @Test
  void name() {
    BaseTask baseTask = new BaseTask().setId(UUID.randomUUID()).setVersion(1).setType("type").setPriority(2);

    subject.trigger(baseTask);

    kafkaConsumer.subscribe(Collections.singletonList(TOPIC), new SeekToDurationOnRebalanceListener(kafkaConsumer, Duration.ofDays(4).negated()));

    Set<String> values = new HashSet<>();
    await().until(
        () -> {
          for (ConsumerRecord<String, String> record : kafkaConsumer.poll(Duration.ofSeconds(5))) {
            values.add(record.value());
          }
          return values.size() > 0;
        }
    );

    Assertions.assertThat(values).hasSize(1);
  }
}
