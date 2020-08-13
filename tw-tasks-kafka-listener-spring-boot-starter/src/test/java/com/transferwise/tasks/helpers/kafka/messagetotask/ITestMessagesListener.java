package com.transferwise.tasks.helpers.kafka.messagetotask;

import org.apache.kafka.clients.consumer.ConsumerRecord;

@FunctionalInterface
public interface ITestMessagesListener {

  void messageReceived(ConsumerRecord<String, String> message);
}
