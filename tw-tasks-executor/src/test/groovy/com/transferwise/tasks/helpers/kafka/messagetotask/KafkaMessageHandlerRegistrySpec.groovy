package com.transferwise.tasks.helpers.kafka.messagetotask

import com.transferwise.tasks.test.BaseSpec
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.springframework.context.ApplicationContext
import org.springframework.context.annotation.AnnotationConfigApplicationContext
import org.springframework.context.annotation.Bean

class KafkaMessageHandlerRegistrySpec extends BaseSpec {
    static final TOPIC_A = "TOPIC_A"

    IKafkaMessageHandlerRegistry kafkaMessageHandlerRegistry

    def setup() {
        ApplicationContext applicationContext = new AnnotationConfigApplicationContext(KafkaMessageHandlerConfiguration)
        kafkaMessageHandlerRegistry = applicationContext.getBean(KafkaMessageHandlerRegistry)
    }

    def "should successfully register autowire handlers"() {
        expect:
        kafkaMessageHandlerRegistry.isEmpty() == false
    }

    def "should return collection of handlers for topic"() {
        expect:
        kafkaMessageHandlerRegistry.getForTopic(TOPIC_A).size() == 2
    }

    def "should return empty collection for non existent topic"() {
        expect:
        kafkaMessageHandlerRegistry.getForTopic("UNKNOWN").size() == 0
    }

    def "should throw IllegalStateException for non existent topic"() {
        when:
        kafkaMessageHandlerRegistry.getForTopicOrFail("UNKNOWN")
        then:
        thrown(IllegalStateException)
    }

    static class KafkaMessageHandlerConfiguration {
        @Bean
        KafkaMessageHandlerRegistry twTasksKafkaMessageHandlerRegistry() {
            return new KafkaMessageHandlerRegistry()
        }

        @Bean
        IKafkaMessageHandler aHandlerForTopicA() {
            return new IKafkaMessageHandler() {

                @Override
                List<IKafkaMessageHandler.Topic> getTopics() {
                    Collections.singletonList(new IKafkaMessageHandler.Topic().setAddress(TOPIC_A))
                }

                @Override
                boolean handles(String topic) {
                    return topic == TOPIC_A
                }

                @Override
                void handle(ConsumerRecord<String, String> record) {

                }
            }
        }

        @Bean
        IKafkaMessageHandler aSecondHandlerForTopicA() {
            return new IKafkaMessageHandler() {

                @Override
                List<IKafkaMessageHandler.Topic> getTopics() {
                    Collections.singletonList(new IKafkaMessageHandler.Topic().setAddress(TOPIC_A))
                }

                @Override
                boolean handles(String topic) {
                    return topic == TOPIC_A
                }

                @Override
                void handle(ConsumerRecord<String, String> record) {

                }
            }
        }
    }
}
