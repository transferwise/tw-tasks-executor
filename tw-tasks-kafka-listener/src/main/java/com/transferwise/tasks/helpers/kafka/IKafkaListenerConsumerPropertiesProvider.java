package com.transferwise.tasks.helpers.kafka;

import java.util.Map;

public interface IKafkaListenerConsumerPropertiesProvider {

  Map<String, Object> getProperties(int shard);
}
