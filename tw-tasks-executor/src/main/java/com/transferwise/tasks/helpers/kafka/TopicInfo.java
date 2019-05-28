package com.transferwise.tasks.helpers.kafka;

import java.util.List;
import java.util.stream.Collectors;

@SuppressWarnings("checkstyle:magicnumber")
public class TopicInfo {

    private final String name;

    private final String partitionIds;

    private final int numPartitions;

    public TopicInfo(String name, List<Integer> partitions) {
        this.name = name;
        this.partitionIds = partitions.stream()
            .map(partition -> Integer.toString(partition, 10))
            .collect(Collectors.joining(","));
        this.numPartitions = partitions.size();
    }

    public String getName() {
        return name;
    }

    public String getPartitionIds() {
        return partitionIds;
    }

    public int getNumPartitions() {
        return numPartitions;
    }

}
