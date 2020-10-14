package com.transferwise.tasks.ext.kafkalistener;

import org.apache.kafka.clients.consumer.ConsumerRecord;

@FunctionalInterface
public interface ITestMessagesListener {

  void messageReceived(ConsumerRecord<String, String> message);
}
