package com.transferwise.tasks.ext.kafkalistener.autoconfigure;

import com.transferwise.tasks.helpers.kafka.IKafkaListenerConsumerPropertiesProvider;
import com.vdurmont.semver4j.Semver;
import java.util.Map;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.CooperativeStickyAssignor;
import org.apache.kafka.clients.consumer.RangeAssignor;
import org.apache.kafka.common.utils.AppInfoParser;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.kafka.KafkaProperties;

@Slf4j
public class SpringKafkaConsumerPropertiesProvider implements IKafkaListenerConsumerPropertiesProvider {

  @Autowired
  private KafkaProperties kafkaProperties;

  @Override
  public Map<String, Object> getProperties(int shard) {
    var props = kafkaProperties.buildConsumerProperties(null);

    try {
      var kafkaClientsVersion = new Semver(AppInfoParser.getVersion());
      if (kafkaClientsVersion.isGreaterThanOrEqualTo("3.0.0")) {
        props.put(ConsumerConfig.PARTITION_ASSIGNMENT_STRATEGY_CONFIG,
            CooperativeStickyAssignor.class.getName() + "," + RangeAssignor.class.getName());
      }
    } catch (Exception e) {
      log.error("Could not understand Kafka client version.", e);
    }

    return props;
  }
}
