package com.transferwise.tasks.testapp.config;

import com.transferwise.tasks.ext.jobs.JobsTestConfiguration;
import com.transferwise.tasks.ext.kafkalistener.KafkaListenerExtTestConfiguration;
import com.transferwise.tasks.test.DefaultMetricsTestHelper;
import com.transferwise.tasks.testapp.ResultRegisteringSyncTaskProcessor;
import com.transferwise.tasks.testapp.TestTaskHandler;
import com.transferwise.tasks.testapp.helpers.kafka.messagetotask.CorruptedMessageTestSetup;
import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Import;

@SpringBootApplication
@Slf4j
@Import({KafkaListenerExtTestConfiguration.class, JobsTestConfiguration.class, CorruptedMessageTestSetup.class})
public class TestApplication {

  @Bean
  public DefaultMetricsTestHelper defaultMetricsTestHelper() {
    return new DefaultMetricsTestHelper();
  }

  @Bean
  public ResultRegisteringSyncTaskProcessor resultRegisteringSyncTaskProcessor() {
    return new ResultRegisteringSyncTaskProcessor();
  }

  @Bean
  public TestTaskHandler testTaskHandler() {
    return new TestTaskHandler();
  }
}