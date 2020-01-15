package com.transferwise.tasks.triggering

import com.transferwise.tasks.test.BaseSpec
import org.apache.kafka.common.TopicPartition

class KafkaTasksExecutionTriggererSpec extends BaseSpec {

    def "something works"() {
        given:
            KafkaTasksExecutionTriggerer.ConsumerBucket consumerBucket = new KafkaTasksExecutionTriggerer.ConsumerBucket()
            KafkaTasksExecutionTriggerer triggerer = new KafkaTasksExecutionTriggerer()
            TopicPartition topicPartition0 = new TopicPartition("FancyTopic", 0)
        when: '5 triggers arrive'
            5.times {
                triggerer.registerPolledOffset(consumerBucket, topicPartition0, it)
            }
        and: '1st message was completed'
            triggerer.releaseCompletedOffset(consumerBucket, topicPartition0, 0)
            KafkaTasksExecutionTriggerer.ConsumerTopicPartition consumerTopicPartition = consumerBucket.getConsumerTopicPartitions().get(topicPartition0);
        then:
            consumerTopicPartition.offsets.size() == 4
            consumerTopicPartition.offsetsCompleted.size() == 0
        when: '3rd message was completed'
            triggerer.releaseCompletedOffset(consumerBucket, topicPartition0, 2)
        then: 'can not commit 3rd offset as 2nd message is not processed yet'
            consumerTopicPartition.offsets.size() == 4
            consumerTopicPartition.offsetsCompleted.size() == 1
        when: '2nd messages was completed'
            triggerer.releaseCompletedOffset(consumerBucket, topicPartition0, 1)
        then: 'can commit 2nd and 3rd message'
            consumerTopicPartition.offsets.size() == 2
            consumerTopicPartition.offsetsCompleted.size() == 0
    }
}
