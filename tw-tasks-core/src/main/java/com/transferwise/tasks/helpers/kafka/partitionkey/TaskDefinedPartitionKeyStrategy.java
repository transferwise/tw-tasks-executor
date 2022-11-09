package com.transferwise.tasks.helpers.kafka.partitionkey;

import com.transferwise.tasks.domain.BaseTask;

public class TaskDefinedPartitionKeyStrategy implements IPartitionKeyStrategy {

  /**
   * Provides the {@link String} partition key defined in the {@link BaseTask}.
   *
   * @param task a Task containing a partition key
   * @return a possibly null {@link String} to be used as partition key
   */
  @Override
  public String getPartitionKey(BaseTask task) {
    return task.getKey();
  }
}
