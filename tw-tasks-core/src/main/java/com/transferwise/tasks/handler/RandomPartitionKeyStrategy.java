package com.transferwise.tasks.handler;

import com.transferwise.tasks.domain.BaseTask;
import com.transferwise.tasks.handler.interfaces.IPartitionKeyStrategy;
import java.util.concurrent.ThreadLocalRandom;

/**
 * Provides a non-null random key, so triggers will be evenly spread around partitions.
 * Otherwise, with a null key, the Kafka client would start doing some kind of batch partitioning.
 *
 */
public class RandomPartitionKeyStrategy implements IPartitionKeyStrategy {

  /**
   * Provides a random {@link String} partition key to send messages to internal Kafka topic
   *
   * @param task a Task
   * @return a {@link String} to be used as partition key
   */
  @Override
  public String getPartitionKey(BaseTask task) {
    return String.valueOf((char) ThreadLocalRandom.current().nextInt(0xFFFF));
  }
}
