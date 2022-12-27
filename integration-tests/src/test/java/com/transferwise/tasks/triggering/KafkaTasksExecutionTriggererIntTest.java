package com.transferwise.tasks.triggering;

import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.transferwise.common.baseutils.UuidUtils;
import com.transferwise.tasks.BaseIntTest;
import com.transferwise.tasks.ITaskDataSerializer;
import com.transferwise.tasks.ITasksService;
import com.transferwise.tasks.ITasksService.AddTaskRequest;
import com.transferwise.tasks.TasksProperties;
import com.transferwise.tasks.dao.ITaskDao;
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
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
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
class KafkaTasksExecutionTriggererIntTest extends BaseIntTest {

  private String topic;
  public static final String PARTITION_KEY = "7a1a43c9-35af-4bea-9349-a1f344c8185c";
  private static final String BUCKET_ID = "manualStart";

  private KafkaConsumer<String, String> kafkaConsumer;
  private AdminClient adminClient;
  //  @Autowired
  //  protected ITasksService tasksService;
  @Autowired
  private ITaskDao taskDao;
  @Autowired
  protected ITaskDataSerializer taskDataSerializer;
  @Autowired
  private TasksProperties tasksProperties;
  //  @Autowired
  //  protected ITasksExecutionTriggerer tasksExecutionTriggerer;
  //  private KafkaTasksExecutionTriggerer subject;

  @BeforeEach
  @SneakyThrows
  void setup() {
    topic = "twTasks." + tasksProperties.getGroupId() + ".executeTask" + "." + BUCKET_ID;
    //    subject = (KafkaTasksExecutionTriggerer) tasksExecutionTriggerer;

    transactionsHelper.withTransaction().asNew().call(() -> {
      testTasksService.reset();
      return null;
    });

    Map<String, Object> configs = new HashMap<>();
    configs.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, tasksProperties.getTriggering().getKafka().getBootstrapServers());

    adminClient = AdminClient.create(configs);
    NewTopic newTopic = new NewTopic(topic, 1, (short) 1);
    adminClient.createTopics(Collections.singletonList(newTopic)).all().get(5, TimeUnit.SECONDS);

    Map<String, Object> consumerProperties = new HashMap<>(configs);
    consumerProperties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
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
      adminClient.deleteTopics(Collections.singletonList(topic));
      adminClient.close();
    });
  }

  @Test
  void name() {
    String data = "Hello World!";
    String taskType = "test";
    UUID taskId = UuidUtils.generatePrefixCombUuid();
//    testTaskHandlerAdapter.setProcessor(testTaskHandlerAdapter.setProcessor((ISyncTaskProcessor) task -> {
//      assertThat(task.getData()).isEqualTo(data.getBytes(StandardCharsets.UTF_8));
//      return new ProcessResult().setResultCode(ResultCode.DONE);
//    });

    testTaskHandlerAdapter.setProcessor(resultRegisteringSyncTaskProcessor)
        .setProcessingPolicy(new SimpleTaskProcessingPolicy()
        .setProcessingBucket(BUCKET_ID)
        .setMaxProcessingDuration(Duration.of(1, ChronoUnit.HOURS))
        .setPartitionKeyStrategy(new TestPartitionKeyStrategy()));

    // when
    UUID uniqueKey = UUID.fromString("323efb9c-341e-47a6-a1fe-b38c62c25b37");
    var taskRequest = new AddTaskRequest()
        .setTaskId(taskId)
        .setData(taskDataSerializer.serialize(data))
        .setType(taskType)
        .setUniqueKey(uniqueKey.toString())
        .setRunAfterTime(ZonedDateTime.now().plusHours(1));

    transactionsHelper.withTransaction().asNew().call(() -> testTasksService.addTask(taskRequest));
    log.info("Added task with id {}", taskId);
    await().until(() -> testTasksService.getWaitingTasks(taskType, null).size() > 0);

    log.info("Resuming task with id {}", taskId);
    assertTrue(transactionsHelper.withTransaction().asNew().call(() ->
        testTasksService.resumeTask(new ITasksService.ResumeTaskRequest().setTaskId(taskId).setVersion(0))
    ));


//    await().until(() -> resultRegisteringSyncTaskProcessor.getTaskResults().get(taskId) != null);


    //    taskDao.setStatus(taskId, TaskStatus.PROCESSING, 0);

    //    testTasksService.startTasksProcessing(BUCKET_ID);

    //    testTasksService.resumeProcessing();

    //    testTasksService.resumeTask(
    //        new ITasksService.ResumeTaskRequest()
    //            .setTaskId(taskId)
    //            .setVersion(taskDao.getTaskVersion(taskId)).setForce(true)
    //    );
    //    await().timeout(30, TimeUnit.SECONDS).until(() ->
    //        testTasksService.getFinishedTasks(taskType, null).size() == 1
    //    );

    //    await().until(() -> resultRegisteringSyncTaskProcessor.getTaskResults().get(taskId) != null);


    kafkaConsumer.subscribe(Collections.singletonList(topic));

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
    Assertions.assertThat(keys.iterator().next()).isEqualTo(PARTITION_KEY);

  }

  static class TestPartitionKeyStrategy implements IPartitionKeyStrategy {
    private static final UUID key = UUID.fromString(PARTITION_KEY);
    @Override
    public String createPartitionKey(BaseTask task) {
      log.info("{} created kafka message key {}", this.getClass().getSimpleName(), key);
      return key.toString();
    }
  }
}
