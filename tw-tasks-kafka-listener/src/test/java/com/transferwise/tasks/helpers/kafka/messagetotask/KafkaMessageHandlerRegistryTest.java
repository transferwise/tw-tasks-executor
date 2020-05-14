package com.transferwise.tasks.helpers.kafka.messagetotask;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Collections;
import java.util.List;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;
import org.springframework.context.annotation.Bean;

class KafkaMessageHandlerRegistryTest {

  private static final String TOPIC_A = "TOPIC_A";

  private IKafkaMessageHandlerRegistry<?> kafkaMessageHandlerRegistry;

  @BeforeEach
  void setup() {
    ApplicationContext applicationContext = new AnnotationConfigApplicationContext(KafkaMessageHandlerConfiguration.class);
    kafkaMessageHandlerRegistry = applicationContext.getBean(KafkaMessageHandlerRegistry.class);
  }

  @Test
  void successfullyRegistersAutowireHandlers() {
    assertFalse(kafkaMessageHandlerRegistry.isEmpty());
  }

  @Test
  void returnsCollectionOfHandlersForTopic() {
    assertEquals(2, kafkaMessageHandlerRegistry.getForTopic(TOPIC_A).size());
  }

  @Test
  void returnsEmptyCollectionForNonExistingTopic() {
    assertTrue(kafkaMessageHandlerRegistry.getForTopic("UNKNOWN").isEmpty());
  }

  @Test
  void throwsIllegalStateExceptionForNonExistingTopic() {
    assertThrows(IllegalStateException.class, () -> kafkaMessageHandlerRegistry.getForTopicOrFail("UNKNOWN"));
  }

  static class KafkaMessageHandlerConfiguration {

    @Bean
    KafkaMessageHandlerRegistry<?> twTasksKafkaMessageHandlerRegistry() {
      return new KafkaMessageHandlerRegistry<>();
    }

    @Bean
    IKafkaMessageHandler<String> handlerForTopicA() {
      return new IKafkaMessageHandler<String>() {

        @Override
        public List<Topic> getTopics() {
          return Collections.singletonList(new IKafkaMessageHandler.Topic().setAddress(TOPIC_A));
        }

        @Override
        public boolean handles(String topic) {
          return TOPIC_A.equals(topic);
        }

        @Override
        public void handle(ConsumerRecord<String, String> record) {
        }
      };
    }

    @Bean
    IKafkaMessageHandler<String> secondHandlerForTopicA() {
      return new IKafkaMessageHandler<String>() {

        @Override
        public List<IKafkaMessageHandler.Topic> getTopics() {
          return Collections.singletonList(new IKafkaMessageHandler.Topic().setAddress(TOPIC_A));
        }

        @Override
        public boolean handles(String topic) {
          return TOPIC_A.equals(topic);
        }

        @Override
        public void handle(ConsumerRecord<String, String> record) {
        }
      };
    }
  }
}
