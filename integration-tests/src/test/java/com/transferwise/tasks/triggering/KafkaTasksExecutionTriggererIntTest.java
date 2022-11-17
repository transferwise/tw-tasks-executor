package com.transferwise.tasks.triggering;

import static org.awaitility.Awaitility.await;

import com.transferwise.tasks.BaseIntTest;
import com.transferwise.tasks.ITaskDataSerializer;
import com.transferwise.tasks.ITasksService;
import com.transferwise.tasks.ITasksService.AddTaskRequest;
import com.transferwise.tasks.TasksProperties;
import com.transferwise.tasks.domain.BaseTask;
import com.transferwise.tasks.handler.SimpleTaskConcurrencyPolicy;
import com.transferwise.tasks.handler.SimpleTaskProcessingPolicy;
import com.transferwise.tasks.helpers.kafka.partitionkey.IPartitionKeyStrategy;
import java.time.Duration;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;

@Slf4j
public class KafkaTasksExecutionTriggererIntTest extends BaseIntTest {

  private static final String TOPIC = "KafkaTestExecutionTriggerIntTest" + RandomStringUtils.randomAlphanumeric(5);

  private KafkaConsumer<String, String> kafkaConsumer;
  private AdminClient adminClient;
  @Autowired
  protected ITasksService tasksService;
  @Autowired
  protected ITaskDataSerializer taskDataSerializer;
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
    NewTopic topic = new NewTopic(TOPIC, 1, (short) 1);
    adminClient.createTopics(Collections.singletonList(topic)).all().get(5, TimeUnit.SECONDS);

    Map<String, Object> consumerProperties = new HashMap<>(configs);
    consumerProperties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "none");
    consumerProperties.put(ConsumerConfig.GROUP_ID_CONFIG, "KafkaTestExecutionTriggerIntTest");
    consumerProperties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
    consumerProperties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
    kafkaConsumer = new KafkaConsumer<>(consumerProperties);
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
  }

  @Test
  void name() {
    final int submittingThreadsCount = 2;
    final int taskProcessingConcurrency = 2;

    testTaskHandlerAdapter.setProcessor(resultRegisteringSyncTaskProcessor);
    testTaskHandlerAdapter.setConcurrencyPolicy(new SimpleTaskConcurrencyPolicy(taskProcessingConcurrency));
    testTaskHandlerAdapter.setProcessingPolicy(new SimpleTaskProcessingPolicy()
        .setProcessingBucket(TOPIC)
        .setPartitionKeyStrategy(new TestPartitionKeyStrategy()));

    // when
    ExecutorService executorService = Executors.newFixedThreadPool(submittingThreadsCount);

    executorService.submit(() -> {
      try {
        var taskRequest = new AddTaskRequest()
            .setData(taskDataSerializer.serialize("Hello World!"))
            .setType("test")
            .setUniqueKey(UUID.randomUUID().toString());

        tasksService.addTask(taskRequest);
      } catch (Throwable t) {
        log.error(t.getMessage(), t);
      }
    });

    kafkaConsumer.subscribe(Collections.singletonList(TOPIC));

    Set<String> keys = new HashSet<>();
    await().until(
        () -> {
          for (ConsumerRecord<String, String> record : kafkaConsumer.poll(Duration.ofSeconds(10))) {
            keys.add(record.key());
          }
          return keys.size() > 0;
        }
    );

    Assertions.assertThat(keys).hasSize(1);

  }

  static class TestPartitionKeyStrategy implements IPartitionKeyStrategy {
    private static final UUID key = UUID.fromString("7a1a43c9-35af-4bea-9349-a1f344c8185c");
    @Override
    public String createPartitionKey(BaseTask task) {
      return key.toString();
    }
  }
}
