package com.transferwise.tasks.helpers.kafka;

import com.transferwise.tasks.TasksProperties;
import org.apache.curator.framework.CuratorFramework;

public class ZkUtilsTopicPartitionsManager implements ITopicPartitionsManager {
    private ITopicManager topicManager;

    public ZkUtilsTopicPartitionsManager(CuratorFramework curatorFramework, TasksProperties tasksProperties) {
        this.topicManager = new TopicManager(curatorFramework, tasksProperties);
    }

    @Override
    public void setPartitionsCount(String topic, int partitionsCount) {
        topicManager.runWithTopic(topic, zk -> topicManager.setPartitions(zk, topic, partitionsCount));
    }
}
