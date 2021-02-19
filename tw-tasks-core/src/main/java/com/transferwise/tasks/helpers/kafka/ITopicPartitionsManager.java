package com.transferwise.tasks.helpers.kafka;

import org.apache.kafka.clients.admin.AdminClient;

public interface ITopicPartitionsManager {

  AdminClient createKafkaAdminClient();

  void setPartitionsCount(String topic, int partitionsCount);
}
