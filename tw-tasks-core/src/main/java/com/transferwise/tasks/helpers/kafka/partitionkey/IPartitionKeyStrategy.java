package com.transferwise.tasks.helpers.kafka.partitionkey;

import com.transferwise.tasks.domain.BaseTask;

public interface IPartitionKeyStrategy {

  String getPartitionKey(BaseTask task);

}
