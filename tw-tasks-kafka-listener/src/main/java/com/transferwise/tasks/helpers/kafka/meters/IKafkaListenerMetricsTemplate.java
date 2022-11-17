package com.transferwise.tasks.helpers.kafka.meters;

import org.apache.kafka.clients.consumer.Consumer;

public interface IKafkaListenerMetricsTemplate {

  void registerKafkaCoreMessageProcessing(int shard, String topic);

  void registerCommit(long shard, String topic, int partition, boolean sync, boolean success);

  @SuppressWarnings("rawtypes")
  AutoCloseable registerKafkaConsumer(Consumer consumer, long shard);
}
