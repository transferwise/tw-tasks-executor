package com.transferwise.tasks.handler;

import com.transferwise.tasks.domain.BaseTask;
import com.transferwise.tasks.handler.interfaces.IPartitionKeyStrategy;

public class CustomKeyPartitionKeyStrategy implements IPartitionKeyStrategy {

  @Override
  public String getPartitionKey(BaseTask task) {
    return task.getKey();
  }
}
