package com.transferwise.tasks.ext.management.autoconfigure;

import com.transferwise.tasks.management.TasksManagementPortController;
import com.transferwise.tasks.management.TasksManagementService;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class TwTasksExtManagementAutoConfiguration {

  @Bean
  public TasksManagementService twTasksTasksManagementService() {
    return new TasksManagementService();
  }

  @Bean
  @ConditionalOnMissingBean(TasksManagementPortController.class)
  public TasksManagementPortController twTasksTasksManagementPortController() {
    return new TasksManagementPortController();
  }
}
