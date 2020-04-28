package com.transferwise.tasks.ext.incidents.config;

import com.transferwise.tasks.health.TasksIncidentGenerator;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class TwTasksExtIncidentsAutoConfiguration {

  @Bean
  public TasksIncidentGenerator tasksIncidentGenerator() {
    return new TasksIncidentGenerator();
  }
}
