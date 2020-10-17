package com.transferwise.tasks.ext.management.autoconfigure;

import com.transferwise.tasks.core.autoconfigure.TwTasksCoreAutoConfiguration.TwTasksDataSourceProvider;
import com.transferwise.tasks.dao.DbConvention;
import com.transferwise.tasks.management.TasksManagementPortController;
import com.transferwise.tasks.management.TasksManagementService;
import com.transferwise.tasks.management.dao.ManagementTaskDao;
import com.transferwise.tasks.management.dao.SqlManagementTaskDao;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class TwTasksExtManagementAutoConfiguration {

  @Bean
  public ManagementTaskDao managementTaskDao(TwTasksDataSourceProvider twTasksDataSourceProvider, DbConvention dbConvention) {
    return new SqlManagementTaskDao(twTasksDataSourceProvider.getDataSource(), dbConvention);
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
