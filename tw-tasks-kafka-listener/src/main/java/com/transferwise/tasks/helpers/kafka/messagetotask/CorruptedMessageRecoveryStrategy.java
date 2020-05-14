package com.transferwise.tasks.helpers.kafka.messagetotask;

import org.apache.kafka.clients.consumer.ConsumerRecord;

/**
 * The strategy used to recover corrupted message (the one that cannot be deserialized) while processing it with {@link
 * ResilientKafkaMessageHandler}.
 */
public interface CorruptedMessageRecoveryStrategy {

  /**
   * Recovers from infinite processing of the same kafka message.
   *
   * @param messageType the type of the message deserialization was failed for
   * @param record      the record that was failed to be deserialized
   * @param exception   the exception appeared as deserialization failure
   * @throws Exception in case recovery is not sufficient and the same record needs to be reprocessed again
   */
  void recover(Class<?> messageType, ConsumerRecord<String, String> record, Exception exception) throws Exception;
}
