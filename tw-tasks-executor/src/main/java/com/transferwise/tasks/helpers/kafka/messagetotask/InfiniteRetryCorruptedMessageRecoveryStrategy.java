package com.transferwise.tasks.helpers.kafka.messagetotask;

import org.apache.kafka.clients.consumer.ConsumerRecord;

/**
 * Doesn't do any recovery, the message deserialization will be retried again.
 */
public class InfiniteRetryCorruptedMessageRecoveryStrategy implements CorruptedMessageRecoveryStrategy {

  @Override
  public void recover(Class<?> messageType, ConsumerRecord<String, String> record, Exception exception) throws Exception {
    throw exception;
  }
}
