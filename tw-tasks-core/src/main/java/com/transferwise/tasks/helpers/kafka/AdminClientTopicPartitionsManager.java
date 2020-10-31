package com.transferwise.tasks.helpers.kafka;

import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.transferwise.common.baseutils.ExceptionUtils;
import com.transferwise.tasks.TasksProperties;
import com.transferwise.tasks.config.TwTasksKafkaConfiguration;
import java.time.Duration;
import java.util.Collections;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.DescribeTopicsResult;
import org.apache.kafka.clients.admin.NewPartitions;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.common.errors.UnknownTopicOrPartitionException;
import org.springframework.beans.factory.annotation.Autowired;

@Slf4j
public class AdminClientTopicPartitionsManager implements ITopicPartitionsManager {

  private static final int COMMANDS_TIMEOUT_S = 30;

  @Autowired
  private TwTasksKafkaConfiguration kafkaConfiguration;
  @Autowired
  private TasksProperties tasksProperties;

  @Override
  public void setPartitionsCount(String topic, int partitionsCount) {
    ExceptionUtils.doUnchecked(() -> {
      AdminClient adminClient = AdminClient.create(kafkaConfiguration.getKafkaProperties().buildAdminProperties());
      //noinspection TryFinallyCanBeTryWithResources
      try {
        DescribeTopicsResult describeTopicsResult = adminClient.describeTopics(Collections.singletonList(topic));
        TopicDescription topicDescription = null;
        try {
          topicDescription = describeTopicsResult.all().get(COMMANDS_TIMEOUT_S, TimeUnit.SECONDS).get(topic);
        } catch (ExecutionException e) {
          if (e.getCause() == null || !(e.getCause() instanceof UnknownTopicOrPartitionException)) {
            throw e;
          }
        }
        if (topicDescription == null) {
          short topicReplicationFactor = tasksProperties.getTopicReplicationFactor();
          log.info(
              "Asking Kafka to create topic '" + topic + "', with " + partitionsCount + " partitions and replication of " + topicReplicationFactor
                  + ".");
          adminClient.createTopics(Collections.singletonList(new NewTopic(topic, partitionsCount, topicReplicationFactor)));
        } else {
          int currentPartitionsCount = topicDescription.partitions().size();
          if (currentPartitionsCount < partitionsCount) {
            log.info("Asking Kafka to increase partitions count for topic '" + topic + "' from " + currentPartitionsCount + " to " + partitionsCount
                + ".");
            adminClient.createPartitions(Maps.asMap(Sets.newHashSet(topic), (k) -> NewPartitions.increaseTo(partitionsCount)));
          }
        }
      } finally {
        adminClient.close(Duration.ofSeconds(COMMANDS_TIMEOUT_S));
      }
    });
  }
}
