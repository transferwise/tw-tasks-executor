package com.transferwise.tasks.ext.kafkapublisher.autoconfigure;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.transferwise.common.baseutils.tracing.IXRequestIdHolder;
import com.transferwise.tasks.ITasksService;
import com.transferwise.tasks.impl.tokafka.ToKafkaProperties;
import com.transferwise.tasks.impl.tokafka.ToKafkaSenderService;
import com.transferwise.tasks.impl.tokafka.ToKafkaTaskHandlerConfiguration;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;

@Configuration
@Import(ToKafkaTaskHandlerConfiguration.class)
@EnableConfigurationProperties
public class TwTasksExtKafkaPublisherAutoConfiguration {

  @Bean
  public ToKafkaSenderService twTasksToKafkaSenderService(
      ObjectMapper objectMapper, ITasksService taskService, ToKafkaProperties properties,
      @Autowired(required = false) IXRequestIdHolder requestIdHolder) {
    return new ToKafkaSenderService(objectMapper, taskService, properties.getBatchSizeMb(), requestIdHolder);
  }

  @Bean
  @ConfigurationProperties(prefix = "tw-tasks.impl.to-kafka", ignoreUnknownFields = false)
  public ToKafkaProperties toKafkaProperties() {
    return new ToKafkaProperties();
  }
}
