package com.transferwise.tasks.ext.kafkalistener.autoconfigure;

import com.transferwise.tasks.helpers.kafka.IKafkaListenerConsumerPropertiesProvider;
import java.util.Map;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.CooperativeStickyAssignor;
import org.apache.kafka.clients.consumer.RangeAssignor;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.kafka.KafkaProperties;

public class SpringKafkaConsumerPropertiesProvider implements IKafkaListenerConsumerPropertiesProvider {

  @Autowired
  private KafkaProperties kafkaProperties;

  @Override
  public Map<String, Object> getProperties(int shard) {
    var props = kafkaProperties.buildConsumerProperties();
    props.put(ConsumerConfig.PARTITION_ASSIGNMENT_STRATEGY_CONFIG, CooperativeStickyAssignor.class.getName());

    return props;
  }
}
