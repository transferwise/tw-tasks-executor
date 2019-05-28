package com.transferwise.tasks.helpers.kafka;

public interface ITopicPartitionsManager {
    void setPartitionsCount(String topic, int partitionsCount);
}
