package com.transferwise.tasks.helpers.kafka.messagetotask;

import com.transferwise.tasks.ITasksService;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;

/**
 * Recovery policy that creates a task of the given type (default {@value DEFAULT_CORRUPTED_MESSAGE_TASK_TYPE}) wrapping the original "corrupted"
 * content in {@link CorruptedKafkaMessage}. The task of created type requires manual intervention, which basically means that a dedicated handler
 * resolving corrupted messages needs to be implemented.
 */
@Slf4j
@RequiredArgsConstructor
public class CreateTaskForCorruptedMessageRecoveryStrategy implements CorruptedMessageRecoveryStrategy {

  public static final String DEFAULT_CORRUPTED_MESSAGE_TASK_TYPE = "CORRUPTED_KAFKA_MESSAGE";

  private final ITasksService tasksService;
  private final String type;

  public CreateTaskForCorruptedMessageRecoveryStrategy(ITasksService tasksService) {
    this(tasksService, DEFAULT_CORRUPTED_MESSAGE_TASK_TYPE);
  }

  @Override
  public void recover(Class<?> messageType, ConsumerRecord<String, String> record, Exception exception) {
    log.error("Failed to deserialize message from topic {}, creating a {} task for it", record.topic(), type, exception);
    tasksService.addTask(
        new ITasksService.AddTaskRequest()
            .setType(type)
            .setDataObject(new CorruptedKafkaMessage(record.topic(), record.value()))
    );
  }

  @Data
  @AllArgsConstructor
  @NoArgsConstructor
  public static class CorruptedKafkaMessage {

    private String topic;
    private String corruptedData;
  }
}
