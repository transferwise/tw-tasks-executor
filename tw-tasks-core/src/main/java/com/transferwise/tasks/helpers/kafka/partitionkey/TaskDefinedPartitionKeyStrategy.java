package com.transferwise.tasks.helpers.kafka.partitionkey;

import com.transferwise.tasks.domain.BaseTask;

public class TaskDefinedPartitionKeyStrategy implements IPartitionKeyStrategy {

  @Override
  public String getPartitionKey(BaseTask task) {
    return task.getKey();
  }
}
