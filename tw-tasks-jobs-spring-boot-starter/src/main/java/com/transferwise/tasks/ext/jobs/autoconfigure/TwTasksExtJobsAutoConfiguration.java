package com.transferwise.tasks.ext.jobs.autoconfigure;

import com.transferwise.tasks.impl.jobs.JobTaskHandler;
import com.transferwise.tasks.impl.jobs.JobsProperties;
import com.transferwise.tasks.impl.jobs.JobsService;
import com.transferwise.tasks.impl.jobs.interfaces.IJobsService;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
@EnableConfigurationProperties
public class TwTasksExtJobsAutoConfiguration {

  @Bean
  @ConditionalOnMissingBean(JobTaskHandler.class)
  public JobTaskHandler twTasksJobsJobTaskHandler() {
    return new JobTaskHandler();
  }

  @Bean
  @ConditionalOnMissingBean(IJobsService.class)
  public JobsService twTasksJobsJobService() {
    return new JobsService();
  }

  @Bean
  @ConfigurationProperties("tw-tasks.jobs")
  public JobsProperties twTasksJobsProperties() {
    return new JobsProperties();
  }

  @Bean
  @ConditionalOnMissingBean(CronJobAnnotationProcessor.class)
  public CronJobAnnotationProcessor cronJobAnnotationProcessor(){
    return new CronJobAnnotationProcessor();
  }
}