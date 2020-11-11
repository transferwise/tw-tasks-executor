package com.transferwise.tasks.helpers.kafka;

import com.transferwise.common.baseutils.ExceptionUtils;
import com.transferwise.tasks.config.TwTasksKafkaConfiguration;
import java.time.Duration;
import java.util.Collections;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.DescribeTopicsResult;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.common.errors.UnknownTopicOrPartitionException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.kafka.KafkaProperties;

@Slf4j
public class NoOpTopicPartitionsManager implements ITopicPartitionsManager {

  private static final int COMMANDS_TIMEOUT_S = 30;

  @Autowired
  private TwTasksKafkaConfiguration kafkaConfiguration;

  @Override
  public AdminClient createKafkaAdminClient(KafkaProperties kafkaProperties) {
    Map<String, Object> adminConfig = kafkaConfiguration.getKafkaProperties().buildAdminProperties();
    //we are passing empty client-id, this will generate auto increment id with format: "adminclient-" + ADMIN_CLIENT_ID_SEQUENCE.getAndIncrement()
    //see https://github.com/apache/kafka/blob/trunk/clients/src/main/java/org/apache/kafka/clients/admin/KafkaAdminClient.java#generateClientId
    adminConfig.put(AdminClientConfig.CLIENT_ID_CONFIG, "");
    return AdminClient.create(adminConfig);
  }

  @Override
  public void setPartitionsCount(String topic, int partitionsCount) {
    ExceptionUtils.doUnchecked(() -> {
      AdminClient adminClient = createKafkaAdminClient(kafkaConfiguration.getKafkaProperties());
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
