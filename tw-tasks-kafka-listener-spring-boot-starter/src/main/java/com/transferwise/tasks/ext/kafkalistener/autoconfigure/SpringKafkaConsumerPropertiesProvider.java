package com.transferwise.tasks.ext.kafkalistener.autoconfigure;

import com.transferwise.tasks.helpers.kafka.IKafkaListenerConsumerPropertiesProvider;
import java.util.Map;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.kafka.KafkaProperties;

public class SpringKafkaConsumerPropertiesProvider implements IKafkaListenerConsumerPropertiesProvider {

  @Autowired
  private KafkaProperties kafkaProperties;

  @Override
  public Map<String, Object> getProperties(int shard) {
    return kafkaProperties.buildConsumerProperties();
  }
}
