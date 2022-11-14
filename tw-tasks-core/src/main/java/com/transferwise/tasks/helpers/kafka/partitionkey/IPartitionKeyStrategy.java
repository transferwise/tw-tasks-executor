package com.transferwise.tasks.helpers.kafka.partitionkey;

import com.transferwise.tasks.domain.BaseTask;

/**
 * Defines a strategy to provide a partition key. The partition key will be used by tw-tasks to send messages to Kafka while triggering a job.
 */
public interface IPartitionKeyStrategy {

  String createPartitionKey(BaseTask task);

}
