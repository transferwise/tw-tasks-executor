package com.transferwise.tasks.helpers.kafka;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class NoOpTopicPartitionsManager implements ITopicPartitionsManager {
    @Override
    public void setPartitionsCount(String topic, int partitionsCount) {
        log.warn("Not trying to set partitions count to " + partitionsCount + " for topic '" + topic + "'.");
    }
}
