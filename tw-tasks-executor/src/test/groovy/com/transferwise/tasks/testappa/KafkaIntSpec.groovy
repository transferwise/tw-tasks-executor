package com.transferwise.tasks.testappa

import com.transferwise.common.baseutils.transactionsmanagement.ITransactionsHelper
import com.transferwise.tasks.config.TwTasksKafkaConfiguration
import com.transferwise.tasks.helpers.kafka.ConsistentKafkaConsumer
import com.transferwise.tasks.helpers.kafka.ITopicPartitionsManager
import com.transferwise.tasks.impl.tokafka.IToKafkaSenderService
import com.transferwise.tasks.test.BaseIntSpec
import groovy.util.logging.Slf4j
import org.springframework.beans.factory.annotation.Autowired
import spock.lang.Unroll

import java.time.Duration
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.AtomicInteger

import static org.awaitility.Awaitility.await

@Slf4j
class KafkaIntSpec extends BaseIntSpec {
    @Autowired
    TwTasksKafkaConfiguration kafkaConfiguration
    @Autowired
    private IToKafkaSenderService toKafkaSenderService
    @Autowired
    private ITransactionsHelper transactionsHelper
    @Autowired
    private ITopicPartitionsManager topicPartitionsManager

    def "sending a message to Kafka works"() {
        given:
        String topic = "ToKafkaTest"
        StringBuilder sb = new StringBuilder()
        // 10MB, We want to see what happens to big message.
        for (int i = 0; i < 999 * 1000; i++) {
            sb.append("Hello Worl")
        }
        String payload = sb.toString()
        when:
        transactionsHelper.withTransaction().asNew().call({
            toKafkaSenderService.sendMessage(
                    new IToKafkaSenderService.SendMessageRequest()
                            .setTopic(topic).setPayloadString(payload))
        })

        AtomicInteger messagesReceivedCount = new AtomicInteger()
        long start = System.currentTimeMillis()
        new ConsistentKafkaConsumer().setTopics([topic])
                .setDelayTimeout(Duration.ofSeconds(1))
                .setShouldPollPredicate({ true })
                .setShouldFinishPredicate({
                    messagesReceivedCount.get() == 1 || System.currentTimeMillis() - start > 30000
                })
                .setKafkaPropertiesSupplier({ kafkaConfiguration.getKafkaProperties().buildConsumerProperties() })
                .setRecordConsumer({ record ->
                    if (record.value() == payload) {
                        messagesReceivedCount.incrementAndGet()
                    }
                }).consume()
        then:
        messagesReceivedCount.get() == 1
    }

    @Unroll
    def "sending batch messages to Kafka works"() {
        given:
        String topic = "toKafkaBatchTestTopic"
        int N = 1000
        when:
        Map<String, AtomicInteger> messagesMap = new ConcurrentHashMap<>()
        for (int i = 0; i < N; i++) {
            messagesMap.put("Message " + iteration + ":" + i, new AtomicInteger())
        }

        transactionsHelper.withTransaction().asNew().call({
            def messages = new IToKafkaSenderService.SendMessagesRequest()
                    .setTopic(topic)
            for (int i = 0; i < N; i++) {
                messages.add(new IToKafkaSenderService.SendMessagesRequest.Message()
                        .setPayloadString("Message " + iteration + ":" + i).setKey(String.valueOf(i)))
            }
            toKafkaSenderService.sendMessages(messages)
        })

        long start = System.currentTimeMillis()
        new ConsistentKafkaConsumer().setTopics([topic])
                .setDelayTimeout(Duration.ofSeconds(1))
                .setShouldPollPredicate({ true })
                .setShouldFinishPredicate({
                    messagesMap.find({ k, v -> v.get() != 1 }) == null || System.currentTimeMillis() - start > 30000
                })
                .setKafkaPropertiesSupplier({ kafkaConfiguration.getKafkaProperties().buildConsumerProperties() })
                .setRecordConsumer({ record ->
                    if (record.offset() < iteration * 1000 || record.offset() > (iteration + 1) * 1000 - 1) {
                        throw new IllegalStateException("Unexpected offset detected for iteration " + iteration + ": " + iteration)
                    }
                    if (record.value() != 'Warmup') {
                        messagesMap.get(record.value()).incrementAndGet()
                    }
                }).consume()
        then:
        await().until {
            messagesMap.find({ k, v -> v.get() != 1 }) == null
        }
        where:
        // We test is commiting offsets is working correctly and is able to finish before closing consumer.
        iteration << [0, 1, 2, 3]
    }

    @Unroll
    def "sending batch messages to Kafka works with 5 partitions"() {
        given:
        String topic = "toKafkaBatchTestTopic5Partitions"
        int N = 1000
        when:
        Map<String, AtomicInteger> messagesMap = new ConcurrentHashMap<>()
        for (int i = 0; i < N; i++) {
            messagesMap.put("Message " + iteration + ":" + i, new AtomicInteger())
        }

        Map<Integer, AtomicInteger> partitionsMap = new ConcurrentHashMap<>()

        transactionsHelper.withTransaction().asNew().call({
            def messages = new IToKafkaSenderService.SendMessagesRequest()
                    .setTopic(topic)
            for (int i = 0; i < N; i++) {
                messages.add(new IToKafkaSenderService.SendMessagesRequest.Message()
                        .setPayloadString("Message " + iteration + ":" + i))
            }
            toKafkaSenderService.sendMessages(messages)
        })

        long start = System.currentTimeMillis()
        new ConsistentKafkaConsumer().setTopics([topic])
                .setDelayTimeout(Duration.ofSeconds(1))
                .setShouldPollPredicate({ true })
                .setShouldFinishPredicate({
                    messagesMap.find({ k, v -> v.get() != 1 }) == null || System.currentTimeMillis() - start > 30000
                })
                .setKafkaPropertiesSupplier({ kafkaConfiguration.getKafkaProperties().buildConsumerProperties() })
                .setRecordConsumer({ record ->
                    if (record.value() != 'Warmup') {
                        messagesMap.get(record.value()).incrementAndGet()
                    }
                    partitionsMap.computeIfAbsent(record.partition(), { k -> new AtomicInteger() }).incrementAndGet()
                }).consume()
        then:
        await().until {
            messagesMap.find({ k, v -> v.get() != 1 }) == null
        }
        5.times {
            assert partitionsMap.get(it).get() > 0
        }
        where:
        // We test is commiting offsets is working correctly and is able to finish before closing consumer.
        iteration << [0, 1, 2, 3]
    }

    @Unroll
    def "flaky messages accepter will not stop the processing"() {
        given:
        String topic = "toKafkaBatchTestTopic2"
        int N = 10
        when:
        Map<String, AtomicInteger> messagesMap = new ConcurrentHashMap<>()
        for (int i = 0; i < N; i++) {
            messagesMap.put("Message " + iteration + ":" + i, new AtomicInteger())
        }

        transactionsHelper.withTransaction().asNew().call({
            def messages = new IToKafkaSenderService.SendMessagesRequest()
                    .setTopic(topic)
            for (int i = 0; i < N; i++) {
                messages.add(new IToKafkaSenderService.SendMessagesRequest.Message()
                        .setPayloadString("Message " + iteration + ":" + i).setKey(String.valueOf(i)))
            }
            toKafkaSenderService.sendMessages(messages)
        })

        long start = System.currentTimeMillis()
        new ConsistentKafkaConsumer().setTopics([topic])
                .setDelayTimeout(Duration.ofMillis(1))
                .setShouldPollPredicate({ true })
                .setShouldFinishPredicate({
                    messagesMap.find({ k, v -> v.get() != 1 }) == null || System.currentTimeMillis() - start > 30000
                })
                .setKafkaPropertiesSupplier({ kafkaConfiguration.kafkaProperties.buildConsumerProperties() })
                .setRecordConsumer({ record ->
                    if (Math.random() < 0.5) {
                        throw new RuntimeException("Unlucky!")
                    }
                    if (record.value() != 'Warmup') {
                        messagesMap.get(record.value()).incrementAndGet()
                    }
                }).consume()
        then:
        await().until {
            messagesMap.find({ k, v -> v.get() != 1 }) == null
        }
        where:
        // We test is commiting offsets is working correctly and is able to finish before closing consumer.
        iteration << [0, 1, 2, 3]
    }
}
