package com.transferwise.tasks.ext.kafkalistener.autoconfigure;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.transferwise.tasks.ITasksService;
import com.transferwise.tasks.helpers.kafka.TwTasksExtKafkaListenerProperties;
import com.transferwise.tasks.helpers.kafka.messagetotask.CoreKafkaListener;
import com.transferwise.tasks.helpers.kafka.messagetotask.KafkaMessageHandlerFactory;
import com.transferwise.tasks.helpers.kafka.messagetotask.KafkaMessageHandlerRegistry;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@EnableConfigurationProperties
@Configuration
public class TwTasksExtKafkaListenerAutoConfiguration {

  @Bean
  @SuppressWarnings("rawtypes")
  public CoreKafkaListener twTasksCoreKafkaListener() {
    return new CoreKafkaListener();
  }

  @Bean
  @SuppressWarnings("rawtypes")
  public KafkaMessageHandlerRegistry twTasksKafkaMessageHandlerRegistry() {
    return new KafkaMessageHandlerRegistry();
  }

  @Bean
  @ConfigurationProperties(prefix = "tw-tasks.ext.kafka-listener")
  public TwTasksExtKafkaListenerProperties kafkaListenerConfigurationProperties() {
    return new TwTasksExtKafkaListenerProperties();
  }

  @Bean
  public KafkaMessageHandlerFactory kafkaMessageHandlerFactory(ITasksService tasksService, ObjectMapper objectMapper) {
    return new KafkaMessageHandlerFactory(tasksService, objectMapper);
  }
}
