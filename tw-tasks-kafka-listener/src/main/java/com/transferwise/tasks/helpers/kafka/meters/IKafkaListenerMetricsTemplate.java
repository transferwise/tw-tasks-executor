package com.transferwise.tasks.helpers.kafka.meters;

public interface IKafkaListenerMetricsTemplate {

  void registerKafkaCoreMessageProcessing(int shard, String topic);

  void registerFailedCommit();
}
