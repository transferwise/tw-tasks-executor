package com.transferwise.tasks.helpers.kafka.meters;

import org.apache.kafka.clients.consumer.Consumer;

public interface IKafkaListenerMetricsTemplate {

  void registerKafkaCoreMessageProcessing(int shard, String topic);

  void registerFailedCommit();

  @SuppressWarnings("rawtypes")
  void registerKafkaConsumer(Consumer consumer, long shard);
}
