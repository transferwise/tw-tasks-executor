package com.transferwise.tasks.ext.management.autoconfigure;

import com.transferwise.tasks.TasksProperties;
import com.transferwise.tasks.core.autoconfigure.TwTasksCoreAutoConfiguration.TwTasksDataSourceProvider;
import com.transferwise.tasks.management.TasksManagementPortController;
import com.transferwise.tasks.management.TasksManagementService;
import com.transferwise.tasks.management.dao.ManagementTaskDao;
import com.transferwise.tasks.management.dao.MySqlManagementTaskDao;
import com.transferwise.tasks.management.dao.PostgresManagementTaskDao;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class TwTasksExtManagementAutoConfiguration {

  @Bean
  @ConditionalOnProperty(value = "tw-tasks.core.db-type", havingValue = "POSTGRES")
  public ManagementTaskDao postgresManagementTaskDao(TwTasksDataSourceProvider twTasksDataSourceProvider, TasksProperties tasksProperties) {
    return new PostgresManagementTaskDao(twTasksDataSourceProvider.getDataSource(), tasksProperties);
  }

  @Bean
  @ConditionalOnProperty(value = "tw-tasks.core.db-type", havingValue = "MYSQL")
  public ManagementTaskDao mysqlManagementTaskDao(TwTasksDataSourceProvider twTasksDataSourceProvider, TasksProperties tasksProperties) {
    return new MySqlManagementTaskDao(twTasksDataSourceProvider.getDataSource(), tasksProperties);
  }

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
