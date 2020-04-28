package com.transferwise.tasks.helpers.kafka;

import com.transferwise.tasks.TasksProperties;
import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;

/**
 * Additional to {@link TasksProperties} configuration of kafka listener extension.
 */
@Data
@ConfigurationProperties(prefix = "tw-tasks.ext.kafka-listener")
public class TwTasksExtKafkaListenerProperties {

  /**
   * When listening Kafka Topics, it is possible to specify the topics replication factor and partitions count, which is applied on application
   * startup.
   *
   * <p>It may add a small additional time for startup, which is sometimes preferred to be avoided (integration tests, development).
   *
   * <p>Using this option, the automatic topic configuration can be turned off.
   */
  private Boolean topicsConfiguringEnabled;
}
