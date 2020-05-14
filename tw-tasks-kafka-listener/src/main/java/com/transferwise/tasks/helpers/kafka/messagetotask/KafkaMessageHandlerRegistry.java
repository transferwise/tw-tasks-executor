package com.transferwise.tasks.helpers.kafka.messagetotask;

import java.util.ArrayList;
import java.util.List;
import javax.annotation.PostConstruct;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections4.CollectionUtils;
import org.springframework.beans.factory.annotation.Autowired;

@Slf4j
public class KafkaMessageHandlerRegistry<T> implements IKafkaMessageHandlerRegistry<T> {

  @Autowired(required = false)
  private List<IKafkaMessageHandler<T>> kafkaMessageHandlers;

  @PostConstruct
  @SuppressWarnings("checkstyle:MultipleStringLiterals")
  public void init() {
    if (!isEmpty()) {
      for (IKafkaMessageHandler<T> kafkaMessageHandler : kafkaMessageHandlers) {
        log.info("Registering Kafka message handler '" + kafkaMessageHandler + "'.");
      }
    }
  }

  @Override
  public List<IKafkaMessageHandler<T>> getForTopic(String topic) {
    if (isEmpty()) {
      return null;
    }
    List<IKafkaMessageHandler<T>> handlers = new ArrayList<>();
    for (IKafkaMessageHandler<T> kafkaMessageHandler : kafkaMessageHandlers) {
      if (kafkaMessageHandler.handles(topic)) {
        handlers.add(kafkaMessageHandler);
      }
    }
    return handlers;
  }

  @Override
  public List<IKafkaMessageHandler<T>> getForTopicOrFail(String topic) {
    List<IKafkaMessageHandler<T>> topicKafkaMessageHandlers = getForTopic(topic);
    if (topicKafkaMessageHandlers.isEmpty()) {
      throw new IllegalStateException("No handler found for topic '" + topic + "'.");
    }
    return topicKafkaMessageHandlers;
  }

  @Override
  public boolean isEmpty() {
    return CollectionUtils.isEmpty(kafkaMessageHandlers);
  }

  @Override
  public List<IKafkaMessageHandler<T>> getKafkaMessageHandlers() {
    return kafkaMessageHandlers;
  }
}
