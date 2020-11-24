package com.transferwise.tasks.ext.incidents.autoconfigure;

import com.transferwise.common.incidents.IncidentGenerator;
import com.transferwise.tasks.health.TasksIncidentGenerator;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class TwTasksExtIncidentsAutoConfiguration {

  @Bean
  @ConditionalOnMissingBean(IncidentGenerator.class)
  public TasksIncidentGenerator tasksIncidentGenerator() {
    return new TasksIncidentGenerator();
  }
}
