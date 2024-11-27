package com.transferwise.tasks.triggering;

import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.transferwise.common.baseutils.UuidUtils;
import com.transferwise.tasks.BaseIntTest;
import com.transferwise.tasks.ITaskDataSerializer;
import com.transferwise.tasks.ITasksService;
import com.transferwise.tasks.ITasksService.AddTaskRequest;
import com.transferwise.tasks.TasksProperties;
import com.transferwise.tasks.domain.BaseTask;
import com.transferwise.tasks.handler.SimpleTaskProcessingPolicy;
import com.transferwise.tasks.helpers.kafka.partitionkey.IPartitionKeyStrategy;
import java.time.Duration;
import java.time.ZonedDateTime;
import java.time.temporal.ChronoUnit;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.UUID;
import lombok.SneakyThrows;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;

class KafkaTasksExecutionTriggererIntTest extends BaseIntTest {

  public static final String PARTITION_KEY = "7a1a43c9-35af-4bea-9349-a1f344c8185c";
  private static final String BUCKET_ID = "manualStart";
  private static final String TASK_TYPE = "test";

  private KafkaConsumer<String, String> kafkaConsumer;
  @Autowired
  protected ITaskDataSerializer taskDataSerializer;
  @Autowired
  private TasksProperties tasksProperties;
  @Autowired
  protected KafkaProducer<String, String> kafkaProducer;

  @BeforeEach
  @SneakyThrows
  void setup() {
    Map<String, Object> configs = new HashMap<>();
    configs.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, tasksProperties.getTriggering().getKafka().getBootstrapServers());

    Map<String, Object> consumerProperties = new HashMap<>(configs);
    consumerProperties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
    consumerProperties.put(ConsumerConfig.GROUP_ID_CONFIG, "KafkaTestExecutionTriggerIntTest");
    consumerProperties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
    consumerProperties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
    kafkaConsumer = new KafkaConsumer<>(consumerProperties, new StringDeserializer(), new StringDeserializer());

    final var topicName = "twTasks." + tasksProperties.getGroupId() + ".executeTask" + "." + BUCKET_ID;
    kafkaConsumer.subscribe(Collections.singletonList(topicName));
  }

  @AfterEach
  void cleanup() {
    cleanWithoutException(() -> {
      kafkaConsumer.close();
    });
  }

  @Test
  void shouldUsePartitionKeyStrategyWhenCustomStrategyDefinedInProcessor() {
    final var data = "Hello World!";
    final var taskId = UuidUtils.generatePrefixCombUuid();

    testTaskHandlerAdapter.setProcessor(resultRegisteringSyncTaskProcessor)
        .setProcessingPolicy(new SimpleTaskProcessingPolicy()
            .setProcessingBucket(BUCKET_ID)
            .setMaxProcessingDuration(Duration.of(1, ChronoUnit.HOURS))
            .setPartitionKeyStrategy(new TestPartitionKeyStrategy()));

    // when
    final var uniqueKey = UUID.fromString("323efb9c-341e-47a6-a1fe-b38c62c25b37");
    final var taskRequest = new AddTaskRequest()
        .setTaskId(taskId)
        .setData(taskDataSerializer.serialize(data))
        .setType(TASK_TYPE)
        .setUniqueKey(uniqueKey.toString())
        .setRunAfterTime(ZonedDateTime.now().plusHours(1));

    transactionsHelper.withTransaction().asNew().call(() -> testTasksService.addTask(taskRequest));
    await().until(() -> testTasksService.getWaitingTasks(TASK_TYPE, null).size() > 0);

    assertTrue(transactionsHelper.withTransaction().asNew().call(() ->
        testTasksService.resumeTask(new ITasksService.ResumeTaskRequest().setTaskId(taskId).setVersion(0))
    ));

    var keys = new HashSet<>();
    await().until(
        () -> {
          for (var record : kafkaConsumer.poll(Duration.ofSeconds(10))) {
            keys.add(record.key());
          }
          return keys.size() > 0;
        }
    );

    Assertions.assertTrue(keys.contains(PARTITION_KEY));
  }

  @Test
  void handlesPoisonPills() {
    // setup:
    testTaskHandlerAdapter.setProcessor(resultRegisteringSyncTaskProcessor)
        .setProcessingPolicy(new SimpleTaskProcessingPolicy()
            .setProcessingBucket(BUCKET_ID)
            .setMaxProcessingDuration(Duration.of(1, ChronoUnit.HOURS))
            .setPartitionKeyStrategy(new TestPartitionKeyStrategy()));


    // when
    int tasksToFire = 10;
    for (int i = 0; i < tasksToFire; i++) {
      publishPosionPill();
      addTask();
      publishPosionPill();
    }
    testTasksService.startTasksProcessing(BUCKET_ID);

    await().until(
        () -> resultRegisteringSyncTaskProcessor.getTaskResults().size() == tasksToFire
    );

  }

  @SneakyThrows
  private void addTask() {
    UUID taskId = UuidUtils.generatePrefixCombUuid();
    final var taskRequest = new AddTaskRequest()
        .setTaskId(taskId)
        .setData(taskDataSerializer.serialize("Hello World!"))
        .setType(TASK_TYPE);

    transactionsHelper.withTransaction().asNew().call(() -> testTasksService.addTask(taskRequest));
  }

  @SneakyThrows
  private void publishPosionPill() {
    final var topicName = "twTasks." + tasksProperties.getGroupId() + ".executeTask." + BUCKET_ID;
    kafkaProducer.send(new ProducerRecord<>(topicName, "poison-pill")).get();
  }

  static class TestPartitionKeyStrategy implements IPartitionKeyStrategy {

    private static final UUID KEY = UUID.fromString(PARTITION_KEY);

    @Override
    public String createPartitionKey(BaseTask task) {
      return KEY.toString();
    }
  }
}
