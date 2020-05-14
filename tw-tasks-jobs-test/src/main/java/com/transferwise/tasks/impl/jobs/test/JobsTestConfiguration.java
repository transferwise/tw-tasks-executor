package com.transferwise.tasks.impl.jobs.test;

import com.transferwise.tasks.test.ITestTasksService;
import com.transferwise.tasks.test.TestTasksService;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Primary;

@Configuration
public class JobsTestConfiguration {

  @Bean
  @ConditionalOnMissingBean(ITestTasksService.class)
  public TestTasksService twTasksTestTasksService() {
    return new TestTasksService();
  }

  @Bean
  @ConditionalOnMissingBean(ITestJobsService.class)
  @Primary
  public TestJobsService twTasksJobsTestJobsService() {
    return new TestJobsService();
  }
}
