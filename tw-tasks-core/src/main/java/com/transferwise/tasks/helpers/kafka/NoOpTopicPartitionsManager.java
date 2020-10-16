package com.transferwise.tasks.helpers.kafka;

import com.transferwise.common.baseutils.ExceptionUtils;
import com.transferwise.tasks.config.TwTasksKafkaConfiguration;
import java.time.Duration;
import java.util.Arrays;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.DescribeTopicsResult;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.common.errors.UnknownTopicOrPartitionException;
import org.springframework.beans.factory.annotation.Autowired;

@Slf4j
public class NoOpTopicPartitionsManager implements ITopicPartitionsManager {

  private static final int COMMANDS_TIMEOUT_S = 30;

  @Autowired
  private TwTasksKafkaConfiguration kafkaConfiguration;

  @Override
  public void setPartitionsCount(String topic, int partitionsCount) {
    ExceptionUtils.doUnchecked(() -> {
      AdminClient adminClient = AdminClient.create(kafkaConfiguration.getKafkaProperties().buildAdminProperties());
      try {
        DescribeTopicsResult describeTopicsResult = adminClient.describeTopics(Arrays.asList(topic));
        TopicDescription topicDescription = null;
        try {
          topicDescription = describeTopicsResult.all().get(COMMANDS_TIMEOUT_S, TimeUnit.SECONDS).get(topic);
        } catch (ExecutionException e) {
          if (e.getCause() == null || !(e.getCause() instanceof UnknownTopicOrPartitionException)) {
            throw e;
          }
        }
        if (topicDescription == null) {
          log.warn("Topic '" + topic + "' doesn't exist.");
        } else {
          int currentPartitionsCount = topicDescription.partitions().size();
          if (currentPartitionsCount < partitionsCount) {
            log.warn("Topic '" + topic + "' has " + currentPartitionsCount + " instead of configured " + partitionsCount + ".");
          }
        }
      } finally {
        adminClient.close(Duration.ofSeconds(COMMANDS_TIMEOUT_S));
      }
    });
  }
}
