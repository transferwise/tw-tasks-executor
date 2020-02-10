package com.transferwise.tasks.helpers.kafka.messagetotask;

import com.transferwise.tasks.ITasksService;
import java.util.Arrays;
import java.util.List;
import java.util.function.BiConsumer;
import java.util.function.Predicate;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.beans.factory.annotation.Autowired;

/**
 * This message to task converter can be used in cases when you are not completely sure that messages in topic have particular structure and want to
 * handle transformation yourself. {@link KafkaMessageToTaskConverter} will try to transform message in the loop with delay, blocking processing of
 * all messages that came after the one that is being processed. Effectively meaning, blocking endless loop which will never resolve.
 */
public class KafkaMessageTypeIgnorantToTaskConverter implements IKafkaMessageHandler<String> {

  @Autowired
  protected ITasksService tasksService;

  private List<Topic> topics;
  private BiConsumer<ConsumerRecord<String, String>, ITasksService.AddTaskRequest> consumer;
  private Predicate<String> handlesPredicate = checkedTopic -> {
    for (Topic topic : topics) {
      if (checkedTopic.endsWith(topic.getAddress())) {
        return true;
      }
    }
    return false;
  };

  public KafkaMessageTypeIgnorantToTaskConverter(BiConsumer<ConsumerRecord<String, String>, ITasksService.AddTaskRequest> consumer, Topic... topics) {
    this.topics = Arrays.asList(topics);
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
    ITasksService.AddTaskRequest addTaskRequest = new ITasksService.AddTaskRequest();
    consumer.accept(record, addTaskRequest);

    tasksService.addTask(addTaskRequest);
  }
}
