package com.transferwise.tasks.testappa

import com.transferwise.tasks.helpers.kafka.messagetotask.CoreKafkaListener
import com.transferwise.tasks.impl.tokafka.test.IToKafkaTestHelper
import com.transferwise.tasks.test.BaseIntSpec
import io.micrometer.core.instrument.MeterRegistry
import org.springframework.beans.factory.annotation.Autowired
import spock.util.concurrent.PollingConditions

import java.util.concurrent.atomic.AtomicInteger

import static com.transferwise.tasks.testappa.config.TestConfiguration.KAFKA_TEST_TOPIC_A

class CoreKafkaListenerIntSpec extends BaseIntSpec {

    @Autowired
    private IToKafkaTestHelper toKafkaTestHelper

    @Autowired
    private MeterRegistry meterRegistry

    @Autowired
    private CoreKafkaListener kafkaListener

    def "should track the number of messages consumed per topic"() {
        given:
        Thread thread
        int N = 10
        Map<String, AtomicInteger> messagesReceivedCounts = [:]

        for (int i = 0; i < N; i++) {
            String message = "Message" + i
            messagesReceivedCounts[message] = new AtomicInteger(0)
            toKafkaTestHelper.sendDirectKafkaMessage(KAFKA_TEST_TOPIC_A, message)
        }
        when:
        kafkaListener.kafkaDataCenterPrefixes = []
        thread = new Thread({ kafkaListener.poll([KAFKA_TEST_TOPIC_A]) as Runnable })
        thread.start()
        then:
        new PollingConditions(timeout: 10, delay: 0.5).eventually {
            meterRegistry.find("twTasks.coreKafka.processedMessagesCount")
                    .tag("topic", KAFKA_TEST_TOPIC_A).counter().count() == N
        }
        cleanup:
        thread.interrupt()
    }
}
