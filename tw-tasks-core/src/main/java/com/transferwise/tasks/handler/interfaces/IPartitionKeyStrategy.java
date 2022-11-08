package com.transferwise.tasks.handler.interfaces;

import com.transferwise.tasks.domain.BaseTask;

public interface IPartitionKeyStrategy {

  String getPartitionKey(BaseTask task);

}
