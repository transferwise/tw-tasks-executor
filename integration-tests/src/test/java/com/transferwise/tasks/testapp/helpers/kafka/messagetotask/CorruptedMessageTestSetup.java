package com.transferwise.tasks.testapp.helpers.kafka.messagetotask;

import com.transferwise.tasks.handler.SimpleTaskConcurrencyPolicy;
import com.transferwise.tasks.handler.SimpleTaskProcessingPolicy;
import com.transferwise.tasks.handler.TaskHandlerAdapter;
import com.transferwise.tasks.handler.interfaces.ISyncTaskProcessor;
import com.transferwise.tasks.handler.interfaces.ITaskHandler;
import com.transferwise.tasks.helpers.kafka.messagetotask.IKafkaMessageHandler;
import com.transferwise.tasks.helpers.kafka.messagetotask.KafkaMessageHandlerFactory;
import java.time.Duration;
import lombok.Data;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class CorruptedMessageTestSetup {

  public static final String KAFKA_TOPIC_WITH_CORRUPTED_MESSAGES = "topicWithCorruptedMessages";
  public static final String TASK_TYPE = "TASK_FROM_CORRUPTED_TOPIC";

  @Data
  static class TestMessage {

    String data;
  }

  @Bean
  IKafkaMessageHandler<String> newHandlerForCorruptedMessagesTopic(KafkaMessageHandlerFactory factory) {
    return factory.createDefaultResilientHandler(
        TestMessage.class,
        (testMessage, record, addTaskRequest) -> {
          addTaskRequest.setType(TASK_TYPE);
          addTaskRequest.setDataString(record.value());
          return true;
        },
        KAFKA_TOPIC_WITH_CORRUPTED_MESSAGES
    );
  }

  @Bean
  ITaskHandler newTaskHandlerForMessageFromTopicContainingCorruptedMessages() {
    return new TaskHandlerAdapter(
        task -> task.getType().equals(TASK_TYPE),
        (ISyncTaskProcessor) task -> new ISyncTaskProcessor.ProcessResult().setResultCode(ISyncTaskProcessor.ProcessResult.ResultCode.DONE)
    )
        .setConcurrencyPolicy(new SimpleTaskConcurrencyPolicy(1))
        .setProcessingPolicy(new SimpleTaskProcessingPolicy().setMaxProcessingDuration(Duration.ofSeconds(10)));
  }
}
