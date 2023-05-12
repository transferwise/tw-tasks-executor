package com.transferwise.tasks.ext.kafkalistener.autoconfigure;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.transferwise.common.baseutils.meters.cache.IMeterCache;
import com.transferwise.tasks.ITasksService;
import com.transferwise.tasks.helpers.kafka.IKafkaListenerConsumerPropertiesProvider;
import com.transferwise.tasks.helpers.kafka.TwTasksKafkaListenerProperties;
import com.transferwise.tasks.helpers.kafka.messagetotask.CoreKafkaListener;
import com.transferwise.tasks.helpers.kafka.messagetotask.EnvironmentValidator;
import com.transferwise.tasks.helpers.kafka.messagetotask.IEnvironmentValidator;
import com.transferwise.tasks.helpers.kafka.messagetotask.IKafkaMessageHandlerRegistry;
import com.transferwise.tasks.helpers.kafka.messagetotask.JavaxValidationEnvironmentValidator;
import com.transferwise.tasks.helpers.kafka.messagetotask.KafkaMessageHandlerFactory;
import com.transferwise.tasks.helpers.kafka.messagetotask.KafkaMessageHandlerRegistry;
import com.transferwise.tasks.helpers.kafka.meters.IKafkaListenerMetricsTemplate;
import com.transferwise.tasks.helpers.kafka.meters.KafkaListenerMetricsTemplate;
import org.springframework.boot.autoconfigure.AutoConfigureAfter;
import org.springframework.boot.autoconfigure.condition.ConditionalOnBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.autoconfigure.kafka.KafkaProperties;
import org.springframework.boot.autoconfigure.validation.ValidationAutoConfiguration;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@EnableConfigurationProperties
@Configuration
@AutoConfigureAfter({ValidationAutoConfiguration.class})
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
  public KafkaMessageHandlerFactory twTasksKafkaMessageHandlerFactory(ITasksService tasksService, ObjectMapper objectMapper) {
    return new KafkaMessageHandlerFactory(tasksService, objectMapper);
  }

  @Bean
  @ConditionalOnMissingBean(IKafkaListenerMetricsTemplate.class)
  public KafkaListenerMetricsTemplate twTasksKafkaListenerMetricsTemplate(IMeterCache meterCache) {
    return new KafkaListenerMetricsTemplate(meterCache);
  }

  @Bean
  @ConfigurationProperties(prefix = "tw-tasks.impl.kafka.listener", ignoreUnknownFields = false)
  @ConditionalOnMissingBean
  public TwTasksKafkaListenerProperties twTasksKafkaListenerProperties() {
    return new TwTasksKafkaListenerProperties();
  }

  @Bean
  @ConditionalOnClass(KafkaProperties.class)
  @ConditionalOnMissingBean(IKafkaListenerConsumerPropertiesProvider.class)
  public SpringKafkaConsumerPropertiesProvider twTasksKafkaListenerSpringKafkaConsumerPropertiesProvider() {
    return new SpringKafkaConsumerPropertiesProvider();
  }

  @Bean
  @ConditionalOnMissingBean()
  public IKafkaListenerConsumerPropertiesProvider twTasksKafkaListenerSpringKafkaConsumerPropertiesProviderMissing() {
    throw new IllegalStateException(
        "You need to provide a bean implementing 'IKafkaListenerConsumerPropertiesProvider' or add `spring-kafka` to the classpath.");
  }

  @Bean
  @ConditionalOnMissingBean(IEnvironmentValidator.class)
  @ConditionalOnBean(type = "jakarta.validation.Validator")
  public EnvironmentValidator twTasksKafkaListenerEnvironmentValidator() {
    return new EnvironmentValidator();
  }

  @Bean
  @ConditionalOnMissingBean(IEnvironmentValidator.class)
  @ConditionalOnBean(type = "javax.validation.Validator")
  public JavaxValidationEnvironmentValidator twTasksKafkaListenerJavaxValidationEnvironmentValidator() {
    return new JavaxValidationEnvironmentValidator();
  }

}
