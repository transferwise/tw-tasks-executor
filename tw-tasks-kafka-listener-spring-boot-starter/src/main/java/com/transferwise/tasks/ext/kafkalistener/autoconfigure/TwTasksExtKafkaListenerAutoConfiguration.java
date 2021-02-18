package com.transferwise.tasks.ext.kafkalistener.autoconfigure;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.transferwise.tasks.ITasksService;
import com.transferwise.tasks.helpers.kafka.configuration.TwTasksKafkaConfiguration;
import com.transferwise.tasks.helpers.kafka.messagetotask.CoreKafkaListener;
import com.transferwise.tasks.helpers.kafka.messagetotask.IKafkaMessageHandlerRegistry;
import com.transferwise.tasks.helpers.kafka.messagetotask.KafkaMessageHandlerFactory;
import com.transferwise.tasks.helpers.kafka.messagetotask.KafkaMessageHandlerRegistry;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.autoconfigure.kafka.KafkaProperties;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.core.KafkaTemplate;

@EnableConfigurationProperties
@Configuration
public class TwTasksExtKafkaListenerAutoConfiguration {

  @Bean
  @ConditionalOnMissingBean
  @SuppressWarnings("rawtypes")
  public CoreKafkaListener twTasksCoreKafkaListener() {
    return new CoreKafkaListener();
  }

  @Bean
  @ConditionalOnMissingBean(IKafkaMessageHandlerRegistry.class)
  @SuppressWarnings("rawtypes")
  public KafkaMessageHandlerRegistry twTasksKafkaMessageHandlerRegistry() {
    return new KafkaMessageHandlerRegistry();
  }

  @Bean
  @ConditionalOnMissingBean
  public KafkaMessageHandlerFactory kafkaMessageHandlerFactory(ITasksService tasksService, ObjectMapper objectMapper) {
    return new KafkaMessageHandlerFactory(tasksService, objectMapper);
  }

  @Bean
  @ConditionalOnMissingBean
  public TwTasksKafkaConfiguration twTaskKafkaConfiguration(KafkaProperties kafkaProperties) {
    return new TwTasksKafkaConfiguration(kafkaProperties);
  }
}
