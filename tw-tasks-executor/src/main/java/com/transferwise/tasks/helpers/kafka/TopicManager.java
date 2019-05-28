package com.transferwise.tasks.helpers.kafka;

import com.transferwise.common.baseutils.ExceptionUtils;
import com.transferwise.tasks.TasksProperties;
import kafka.admin.AdminUtils;
import kafka.admin.RackAwareMode;
import kafka.utils.ZKStringSerializer$;
import kafka.utils.ZkUtils;
import org.I0Itec.zkclient.ZkClient;
import org.I0Itec.zkclient.ZkConnection;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.locks.InterProcessMutex;
import org.apache.kafka.common.errors.TopicExistsException;
import org.apache.kafka.common.requests.MetadataResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.collection.Iterable;
import scala.collection.Iterator;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Collectors;

public class TopicManager implements ITopicManager {
    private CuratorFramework curatorFramework;

    private TasksProperties tasksProperties;

    private static final Logger log = LoggerFactory.getLogger(TopicManager.class);

    private static final RackAwareMode RACK_AWARE_MODE = new RackAwareMode.Disabled$();

    public TopicManager(CuratorFramework curatorFramework, TasksProperties tasksProperties) {
        this.curatorFramework = curatorFramework;
        this.tasksProperties = tasksProperties;
    }

    @Override
    public Collection<String> getAllTopics() {
        return withZookeeper(zkUtils -> cast(zkUtils.getAllTopics()));
    }

    @Override
    public TopicInfo getTopicInfo(ZkUtils zkUtils, String name) {
        final MetadataResponse.TopicMetadata metadata = AdminUtils.fetchTopicMetadataFromZk(name, zkUtils);
        final List<Integer> partitions = metadata.partitionMetadata().stream()
            .map(MetadataResponse.PartitionMetadata::partition)
            .sorted()
            .collect(Collectors.toList());
        return new TopicInfo(name, partitions);
    }

    @Override
    public void createTopic(ZkUtils zkUtils, String name, int numPartitions) {
        AdminUtils.createTopic(zkUtils, name, numPartitions, tasksProperties.getTopicReplicationFactor(), new Properties(), RACK_AWARE_MODE);
        log.info("Created Topic (name: {}, numPartitions: {}, replicationFactor: {})", name, numPartitions, tasksProperties.getTopicReplicationFactor());
    }

    @Override
    public void deleteTopic(ZkUtils zkUtils, String name) {
        AdminUtils.deleteTopic(zkUtils, name);
        log.info("Deleted Topic (name: {})", name);
    }

    @Override
    public void addPartitions(ZkUtils zkUtils, String name, int numPartitions) {
        AdminUtils.addPartitions(zkUtils, name, numPartitions, "", true, RACK_AWARE_MODE);
        log.info("Altered topic (name: {}, numPartitions: {}, replicationFactor: {})", name, numPartitions, tasksProperties.getTopicReplicationFactor());
    }

    @Override
    @SuppressWarnings("checkstyle:magicnumber")
    public void setPartitions(ZkUtils zkUtils, String name, int numPartitions) {
        TopicInfo topicInfo = getTopicInfo(zkUtils, name);
        if (topicInfo.getNumPartitions() == 0) { // Topic does not exist
            try {
                createTopic(zkUtils, name, numPartitions);
            } catch (TopicExistsException e) {
                setPartitions(zkUtils, name, numPartitions);
            }
        } else {
            if (topicInfo.getNumPartitions() < numPartitions) {
                addPartitions(zkUtils, name, numPartitions);
            }
        }
    }

    private <T> List<T> cast(Iterable<T> iterable) {
        final List<T> l = new ArrayList<>();
        for (Iterator<T> it = iterable.iterator(); it.hasNext(); ) {
            l.add(it.next());
        }
        return l;
    }

    private ZkUtils createZkUtils() {
        final ZkConnection connection = new ZkConnection(tasksProperties.getZookeeperConnectString(), 5000);
        final ZkClient client = new ZkClient(connection, 10000, ZKStringSerializer$.MODULE$);
        return new ZkUtils(client, connection, false);
    }

    private <T> T withZookeeper(Function<ZkUtils, T> f) {
        ZkUtils zkUtils = null;
        try {
            zkUtils = createZkUtils();
            return f.apply(zkUtils);
        } finally {
            if (zkUtils != null) {
                try {
                    zkUtils.close();
                } catch (Exception e) {
                    log.warn("Failed to close ZkUtils", e);
                }
            }
        }
    }

    @Override
    @SuppressWarnings("checkstyle:MagicNumber")
    public <T> T callWithTopic(String topic, Function<ZkUtils, T> f) {
        return ExceptionUtils.doUnchecked(() -> {
            String lockPath = "/tw/kafka/locks/" + topic + "/admin";
            InterProcessMutex clusterWideMutex = new InterProcessMutex(curatorFramework, lockPath);

            if (clusterWideMutex.acquire(10000, TimeUnit.MILLISECONDS)) {
                try {
                    return withZookeeper(f);
                } finally {
                    clusterWideMutex.release();
                }
            }

            throw new IllegalStateException("Could not take a lock for '" + lockPath + ".");
        });
    }

    @Override
    public void runWithTopic(String topic, Consumer<ZkUtils> c) {
        callWithTopic(topic, zk -> {
            c.accept(zk);
            return null;
        });
    }
}
