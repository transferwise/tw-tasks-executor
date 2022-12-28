package com.transferwise.tasks.helpers.kafka.partitionkey;

import com.transferwise.tasks.domain.BaseTask;
import java.util.concurrent.ThreadLocalRandom;
import lombok.extern.slf4j.Slf4j;

/**
 * Provides a non-null random key, so triggers will be evenly spread around partitions.
 * Otherwise, with a null key, the Kafka client would start doing some kind of batch partitioning.
 *
 */
@Slf4j
public class RandomPartitionKeyStrategy implements IPartitionKeyStrategy {

  /**
   * Provides a random {@link String} partition key to send messages to internal Kafka topic.
   *
   * @param task a Task
   * @return a {@link String} to be used as partition key
   */
  @Override
  public String createPartitionKey(BaseTask task) {
    String key = String.valueOf((char) ThreadLocalRandom.current().nextInt(0xFFFF));
    log.info("{} created kafka message key {}", this.getClass().getSimpleName(), key);
    return key;
  }
}
