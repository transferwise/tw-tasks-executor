package com.transferwise.tasks.ext.jobs;

import com.transferwise.tasks.ext.jobs.JobsIntTest.CronJobA;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class JobsTestConfiguration {

  @Bean
  public CronJobA cronJobA() {
    return new CronJobA();
  }

}