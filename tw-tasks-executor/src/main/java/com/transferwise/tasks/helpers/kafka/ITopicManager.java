package com.transferwise.tasks.helpers.kafka;

import kafka.utils.ZkUtils;

import java.util.Collection;
import java.util.function.Consumer;
import java.util.function.Function;

public interface ITopicManager {
    Collection<String> getAllTopics();

    TopicInfo getTopicInfo(ZkUtils zkUtils, String name);

    void createTopic(ZkUtils zkUtils, String name, int numPartitions);

    void deleteTopic(ZkUtils zkUtils, String name);

    void addPartitions(ZkUtils zkUtils, String name, int numPartitions);

    @SuppressWarnings("checkstyle:magicnumber")
    void setPartitions(ZkUtils zkUtils, String name, int numPartitions);

    <T> T callWithTopic(String topic, Function<ZkUtils, T> f);

    void runWithTopic(String topic, Consumer<ZkUtils> c);
}
