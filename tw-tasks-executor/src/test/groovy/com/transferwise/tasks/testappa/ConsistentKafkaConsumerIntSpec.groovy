package com.transferwise.tasks.testappa

import com.transferwise.tasks.config.TwTasksKafkaConfiguration
import com.transferwise.tasks.helpers.kafka.ConsistentKafkaConsumer
import com.transferwise.tasks.impl.tokafka.test.IToKafkaTestHelper
import com.transferwise.tasks.test.BaseIntSpec
import groovy.util.logging.Slf4j
import org.springframework.beans.factory.annotation.Autowired

import java.util.concurrent.atomic.AtomicInteger

import static org.awaitility.Awaitility.await

@Slf4j
class ConsistentKafkaConsumerIntSpec extends BaseIntSpec {
    @Autowired
    private IToKafkaTestHelper toKafkaTestHelper
    @Autowired
    private TwTasksKafkaConfiguration kafkaConfiguration

    def "all messages will be received once on rebalancing"() {
        given:
            int N = 10
            Map<String, AtomicInteger> messagesReceivedCounts = [:]

            def testTopic = "ConsistentKafkaConsumerIntSpec"
            for (int i = 0; i < N; i++) {
                String message = "Message" + i
                messagesReceivedCounts[message] = new AtomicInteger(0);
                toKafkaTestHelper.sendDirectKafkaMessage(testTopic, message);
            }

        when:
            boolean shouldFinish = false

            ConsistentKafkaConsumer consumer = new ConsistentKafkaConsumer().setKafkaPropertiesSupplier({
                kafkaConfiguration.getKafkaProperties().buildConsumerProperties()
            })
                .setTopics([testTopic]).setShouldFinishPredicate({ shouldFinish })
                .setShouldPollPredicate({ !shouldFinish })
                .setRecordConsumer({ consumerRecord ->
                log.info "Received message '" + consumerRecord.value() + "': " +
                    messagesReceivedCounts.get(consumerRecord.value()).incrementAndGet() + ".";

            })

            Thread consumerThread = new Thread({ consumer.consume() })
            consumerThread.start()

            await().until {
                messagesReceivedCounts.values().sum { it.get() } >= N
            }

            shouldFinish = true
            consumerThread.join()
        then: 'we received all messages and only once'
            !messagesReceivedCounts.values().find({ it.get() != 1 })
        when: 'another kafka consumer starts up'
            String message = "Message" + N
            messagesReceivedCounts[message] = new AtomicInteger(0)
            toKafkaTestHelper.sendDirectKafkaMessage(testTopic, message)

            shouldFinish = false

            consumerThread = new Thread({ consumer.consume() })
            consumerThread.start()

            await().until {
                messagesReceivedCounts.values().sum { it.get() } > N
            }

            shouldFinish = true
            consumerThread.join()
        then: 'we received no old messages again, but new message was received correctly'
            !messagesReceivedCounts.values().find({ it.get() != 1 })
            for (int i = 0; i < N + 1; i++) {
                messagesReceivedCounts["Message" + i].get() == 1
            }
    }
}
