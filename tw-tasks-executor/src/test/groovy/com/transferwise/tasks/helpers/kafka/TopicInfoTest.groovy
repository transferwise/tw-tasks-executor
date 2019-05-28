package com.transferwise.tasks.helpers.kafka

import spock.lang.Specification

class TopicInfoTest extends Specification {
    def "constructor should build a correct topic info"() {
        given:
            List<Integer> partitionsIds = [1, 2, 3]
            String topic = "topic"

        when:
            TopicInfo topicInfo = new TopicInfo(topic, partitionsIds)

        then:
            topicInfo.name == topic
            topicInfo.partitionIds == "1,2,3"
            topicInfo.numPartitions == partitionsIds.size()
    }
}
