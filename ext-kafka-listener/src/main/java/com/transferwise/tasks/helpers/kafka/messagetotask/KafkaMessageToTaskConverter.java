package com.transferwise.tasks.helpers.kafka.messagetotask;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.transferwise.common.baseutils.ExceptionUtils;
import com.transferwise.tasks.ITasksService;
import com.transferwise.tasks.utils.TriConsumer;
import java.util.Arrays;
import java.util.List;
import java.util.function.Predicate;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.beans.factory.annotation.Autowired;

/**
 * Use to translate Kafka messages into tasks.
 */
public class KafkaMessageToTaskConverter<T> implements IKafkaMessageHandler<String> {

  @Autowired
  protected ITasksService tasksService;
  @Autowired
  protected ObjectMapper objectMapper;

  private List<Topic> topics;
  private Class<T> dataObjClass;
  private TriConsumer<T, ConsumerRecord<String, String>, ITasksService.AddTaskRequest> consumer;
  private Predicate<String> handlesPredicate = checkedTopic -> {
    for (Topic topic : topics) {
      if (checkedTopic.endsWith(topic.getAddress())) {
        return true;
      }
    }
    return false;
  };

  /**
   * The consumer method must return true for the task to be created. If it returns false, the message will be ignored, no task created. This can be
   * used to filter out messages.
   */
  public KafkaMessageToTaskConverter(Class<T> dataObjClass,
      TriConsumer<T, ConsumerRecord<String, String>, ITasksService.AddTaskRequest> consumer,
      Topic... topics) {
    this.topics = Arrays.asList(topics);
    this.dataObjClass = dataObjClass;
    this.consumer = consumer;
  }

  @Override
  public List<Topic> getTopics() {
    return topics;
  }

  @Override
  public boolean handles(String topic) {
    return handlesPredicate.test(topic);
  }

  @Override
  public void handle(ConsumerRecord<String, String> record) {
    T dataObj = ExceptionUtils.doUnchecked(() -> objectMapper.readValue(String.valueOf(record.value()), this.dataObjClass));

    ITasksService.AddTaskRequest addTaskRequest = new ITasksService.AddTaskRequest();
    if (consumer.accept(dataObj, record, addTaskRequest)) {
      tasksService.addTask(addTaskRequest);
    }
  }
}
