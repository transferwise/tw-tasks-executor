package com.transferwise.tasks.ext.kafkalistener;

import com.transferwise.tasks.helpers.kafka.messagetotask.IKafkaMessageHandler;
import java.util.Arrays;
import java.util.List;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class KafkaListenerExtTestConfiguration {

  public static final String TOPIC_A = "KafkaListenerTopicA";
  public static final String TOPIC_B = "KafkaListenerTopicB";

  @Bean
  public TestMessagesListeners testMessagesListeners() {
    return new TestMessagesListeners();
  }

  @Bean
  public IKafkaMessageHandler<String> messageHandlerA(TestMessagesListeners testMessagesListeners) {
    return new IKafkaMessageHandler<String>() {
      @Override
      public List<Topic> getTopics() {
        return Arrays.asList(new Topic().setAddress(TOPIC_A),
            new Topic().setAddress(TOPIC_B).setShard(1));
      }

      @Override
      public boolean handles(String topic) {
        return topic.equals(TOPIC_A) || topic.equals(TOPIC_B);
      }

      @Override
      public void handle(ConsumerRecord<String, String> record) {
        testMessagesListeners.messageReceived(record);
      }
    };
  }
}
