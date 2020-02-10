package com.transferwise.tasks.helpers.kafka.messagetotask;

import java.util.List;

public interface IKafkaMessageHandlerRegistry<T> {

  List<IKafkaMessageHandler<T>> getForTopic(String topic);

  List<IKafkaMessageHandler<T>> getForTopicOrFail(String topic);

  boolean isEmpty();

  List<IKafkaMessageHandler<T>> getKafkaMessageHandlers();
}
