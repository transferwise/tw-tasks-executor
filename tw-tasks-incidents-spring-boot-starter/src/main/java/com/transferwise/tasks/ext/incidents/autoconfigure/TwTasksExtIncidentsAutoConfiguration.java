package com.transferwise.tasks.ext.incidents.autoconfigure;

import com.transferwise.tasks.health.TasksIncidentGenerator;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class TwTasksExtIncidentsAutoConfiguration {

  @Bean
  @ConditionalOnMissingBean
  public TasksIncidentGenerator tasksIncidentGenerator() {
    return new TasksIncidentGenerator();
  }
}
