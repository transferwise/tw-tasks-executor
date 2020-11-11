package com.transferwise.tasks.helpers.kafka;

import org.apache.kafka.clients.admin.AdminClient;
import org.springframework.boot.autoconfigure.kafka.KafkaProperties;

public interface ITopicPartitionsManager {

  AdminClient createKafkaAdminClient(KafkaProperties kafkaProperties);
  void setPartitionsCount(String topic, int partitionsCount);
}
