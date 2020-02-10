package com.transferwise.tasks.testappa.helpers.kafka.messagetotask

import com.fasterxml.jackson.databind.ObjectMapper
import com.transferwise.tasks.config.TwTasksKafkaConfiguration
import com.transferwise.tasks.domain.Task
import com.transferwise.tasks.domain.TaskStatus
import com.transferwise.tasks.helpers.kafka.messagetotask.CreateTaskForCorruptedMessageRecoveryStrategy
import com.transferwise.tasks.impl.tokafka.test.IToKafkaTestHelper
import com.transferwise.tasks.test.BaseIntSpec
import org.apache.kafka.clients.admin.AdminClient
import org.apache.kafka.clients.producer.ProducerRecord
import org.awaitility.Awaitility
import org.springframework.beans.factory.annotation.Autowired

import static com.transferwise.tasks.helpers.kafka.messagetotask.CreateTaskForCorruptedMessageRecoveryStrategy.DEFAULT_CORRUPTED_MESSAGE_TASK_TYPE

class ResilientKafkaMessageHandlerIntSpec extends BaseIntSpec {

    @Autowired
    IToKafkaTestHelper toKafkaTestHelper

    @Autowired
    ObjectMapper objectMapper

    @Autowired
    private TwTasksKafkaConfiguration kafkaConfiguration;

    def cleanup() {
        AdminClient adminClient = AdminClient.create(kafkaConfiguration.getKafkaProperties().buildAdminProperties())
        try {
            adminClient.deleteTopics(Arrays.asList(CorruptedMessageTestSetup.KAFKA_TOPIC_WITH_CORRUPTED_MESSAGES))
        }
        finally {
            adminClient.close();
        }
    }

    def "corrupted message results in error task of corrupted type"() {
        when: "send corrupted message"
        toKafkaTestHelper.sendDirectKafkaMessage(
                new ProducerRecord<>(
                        CorruptedMessageTestSetup.KAFKA_TOPIC_WITH_CORRUPTED_MESSAGES,
                        null,
                        System.currentTimeMillis(),
                        "key",
                        '{"corrupted json.'
                )
        )
        and: "send consistent message"
        toKafkaTestHelper.sendDirectKafkaMessage(
                new ProducerRecord<>(
                        CorruptedMessageTestSetup.KAFKA_TOPIC_WITH_CORRUPTED_MESSAGES,
                        null,
                        System.currentTimeMillis(),
                        "key",
                        '{}'
                )
        )
        and: "there is a message of corrupted type in error state (because no handler provided for it)"
        List<Task> corruptedTasks = []
        Awaitility.await().until({ ->
            corruptedTasks = testTasksService.getTasks(DEFAULT_CORRUPTED_MESSAGE_TASK_TYPE, null, TaskStatus.ERROR)
            corruptedTasks.size() == 1
        })

        then: "The corrupted message is properly wrapped and saved"
        def message = objectMapper.readValue(corruptedTasks[0].data, CreateTaskForCorruptedMessageRecoveryStrategy.CorruptedKafkaMessage)
        message.getTopic() == CorruptedMessageTestSetup.KAFKA_TOPIC_WITH_CORRUPTED_MESSAGES
        message.getCorruptedData() == '{"corrupted json.'

        and: "The further messages in the topic are processed"
        Awaitility.await().until({ ->
            testTasksService.getTasks(CorruptedMessageTestSetup.TASK_TYPE, null, TaskStatus.DONE).size() == 1
        })
    }
}


