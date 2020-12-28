package com.transferwise.tasks.ext.management.autoconfigure;

import com.transferwise.tasks.TasksProperties;
import com.transferwise.tasks.core.autoconfigure.TwTasksDataSourceProvider;
import com.transferwise.tasks.dao.ITaskDaoDataSerializer;
import com.transferwise.tasks.management.ITasksManagementService;
import com.transferwise.tasks.management.TasksManagementPortController;
import com.transferwise.tasks.management.TasksManagementService;
import com.transferwise.tasks.management.dao.IManagementTaskDao;
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
  @ConditionalOnMissingBean(IManagementTaskDao.class)
  public PostgresManagementTaskDao postgresManagementTaskDao(TwTasksDataSourceProvider twTasksDataSourceProvider, TasksProperties tasksProperties,
      ITaskDaoDataSerializer taskDataSerializer) {
    return new PostgresManagementTaskDao(twTasksDataSourceProvider.getDataSource(), tasksProperties, taskDataSerializer);
  }

  @Bean
  @ConditionalOnProperty(value = "tw-tasks.core.db-type", havingValue = "MYSQL")
  @ConditionalOnMissingBean(IManagementTaskDao.class)
  public MySqlManagementTaskDao mysqlManagementTaskDao(TwTasksDataSourceProvider twTasksDataSourceProvider, TasksProperties tasksProperties,
      ITaskDaoDataSerializer taskDataSerializer) {
    return new MySqlManagementTaskDao(twTasksDataSourceProvider.getDataSource(), tasksProperties, taskDataSerializer);
  }

  @Bean
  @ConditionalOnMissingBean(ITasksManagementService.class)
  public TasksManagementService twTasksTasksManagementService() {
    return new TasksManagementService();
  }

  @Bean
  @ConditionalOnMissingBean(TasksManagementPortController.class)
  public TasksManagementPortController twTasksTasksManagementPortController() {
    return new TasksManagementPortController();
  }
}
