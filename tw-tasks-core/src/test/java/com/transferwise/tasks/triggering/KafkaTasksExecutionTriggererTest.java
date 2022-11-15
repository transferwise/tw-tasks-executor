package com.transferwise.tasks.triggering;

import static org.junit.jupiter.api.Assertions.assertEquals;

import com.transferwise.tasks.domain.BaseTask;
import com.transferwise.tasks.triggering.KafkaTasksExecutionTriggerer.ConsumerBucket;
import com.transferwise.tasks.triggering.KafkaTasksExecutionTriggerer.ConsumerTopicPartition;
import java.util.UUID;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.Test;

class KafkaTasksExecutionTriggererTest {

  @Test
  void somethingWorks() {
    ConsumerBucket consumerBucket = new ConsumerBucket();
    KafkaTasksExecutionTriggerer triggerer = new KafkaTasksExecutionTriggerer();
    TopicPartition topicPartition0 = new TopicPartition("FancyTopic", 0);

    for (int i = 0; i < 5; i++) {
      triggerer.registerPolledOffset(consumerBucket, topicPartition0, i);
    }

    // 1st message was completed
    triggerer.releaseCompletedOffset(consumerBucket, topicPartition0, 0);
    ConsumerTopicPartition consumerTopicPartition = consumerBucket.getConsumerTopicPartitions().get(topicPartition0);

    assertEquals(4, consumerTopicPartition.getOffsets().size());
    assertEquals(0, consumerBucket.getOffsetsCompletedCount());

    // 3rd message was completed
    triggerer.releaseCompletedOffset(consumerBucket, topicPartition0, 2);

    // can not commit 3rd offset as 2nd message is not processed yet
    assertEquals(4, consumerTopicPartition.getOffsets().size());
    assertEquals(1, consumerBucket.getOffsetsCompletedCount());

    // 2nd messages was completed
    triggerer.releaseCompletedOffset(consumerBucket, topicPartition0, 1);

    // can commit 2nd and 3rd message
    assertEquals(2, consumerTopicPartition.getOffsets().size());
    assertEquals(0, consumerBucket.getOffsetsCompletedCount());
  }

  @Test
  void name() {
    KafkaTasksExecutionTriggerer triggerer = new KafkaTasksExecutionTriggerer();


    BaseTask baseTask = new BaseTask().setId(UUID.randomUUID()).setVersion(1).setType("type").setPriority(2);

    triggerer.trigger(baseTask);
  }
}
