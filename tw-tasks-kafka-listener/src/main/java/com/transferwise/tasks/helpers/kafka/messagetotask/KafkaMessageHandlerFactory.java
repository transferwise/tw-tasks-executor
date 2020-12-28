package com.transferwise.tasks.helpers.kafka.messagetotask;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.transferwise.tasks.ITasksService;
import com.transferwise.tasks.utils.TriConsumer;
import lombok.RequiredArgsConstructor;
import org.apache.kafka.clients.consumer.ConsumerRecord;

/**
 * Factory for ready to use {@link IKafkaMessageHandler kafka message handlers}.
 */
@RequiredArgsConstructor
public class KafkaMessageHandlerFactory {

  private final ITasksService tasksService;
  private final ObjectMapper objectMapper;

  /**
   * Creates default handler, the behaviour of the handler is the same to {@link KafkaMessageToTaskConverter}. When the handler fails to deserialize a
   * message it will infinitely attempt to deserialize the same message over and over again, the processing of the other messages in the topic will be
   * stuck.
   */
  public <T> IKafkaMessageHandler<String> createDefaultHandler(
      Class<T> dataObjClass,
      TriConsumer<T, ConsumerRecord<String, String>, ITasksService.AddTaskRequest> consumer,
      IKafkaMessageHandler.Topic... topics
  ) {
    return createResilientHandler(
        new InfiniteRetryCorruptedMessageRecoveryStrategy(),
        dataObjClass,
        consumer,
        topics
    );
  }

  /**
   * Creates a resilient handler, every corrupted message will be wrapped and processed separately as described in {@link
   * CorruptedMessageRecoveryStrategy}.
   *
   * @see ResilientKafkaMessageHandler
   */
  public <T> IKafkaMessageHandler<String> createDefaultResilientHandler(
      Class<T> dataObjClass,
      TriConsumer<T, ConsumerRecord<String, String>, ITasksService.AddTaskRequest> consumer,
      IKafkaMessageHandler.Topic... topics
  ) {
    return createResilientHandler(
        new CreateTaskForCorruptedMessageRecoveryStrategy(tasksService, objectMapper),
        dataObjClass,
        consumer,
        topics
    );
  }

  /**
   * Creates a resilient handler, every corrupted message will be wrapped and processed separately as described in {@link
   * CorruptedMessageRecoveryStrategy}.
   *
   * @see ResilientKafkaMessageHandler
   */
  public <T> IKafkaMessageHandler<String> createDefaultResilientHandler(
      Class<T> dataObjClass,
      TriConsumer<T, ConsumerRecord<String, String>, ITasksService.AddTaskRequest> consumer,
      String topicAddress
  ) {
    return createDefaultResilientHandler(
        dataObjClass,
        consumer,
        new IKafkaMessageHandler.Topic().setAddress(topicAddress)
    );
  }

  /**
   * Creates a resilient handler, the decision on how to deal with corrupted messages is delegated to the given {@code recoveryStrategy}.
   *
   * @see ResilientKafkaMessageHandler
   */
  public <T> IKafkaMessageHandler<String> createResilientHandler(
      CorruptedMessageRecoveryStrategy recoveryStrategy,
      Class<T> dataObjClass,
      TriConsumer<T, ConsumerRecord<String, String>, ITasksService.AddTaskRequest> consumer,
      IKafkaMessageHandler.Topic... topics
  ) {
    return new ResilientKafkaMessageHandler<>(
        objectMapper,
        tasksService,
        recoveryStrategy,
        dataObjClass,
        consumer,
        topics
    );
  }
}
