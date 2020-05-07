package com.transferwise.tasks.helpers.kafka;

import com.transferwise.tasks.TasksProperties;
import lombok.Data;

/**
 * Additional to {@link TasksProperties} configuration of kafka listener extension.
 */
@Data
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
