package com.transferwise.tasks.test;

import com.transferwise.tasks.TasksProperties;
import com.transferwise.tasks.core.autoconfigure.ITwTasksDataSourceProvider;
import com.transferwise.tasks.dao.ITaskDaoDataSerializer;
import com.transferwise.tasks.test.dao.ITestTaskDao;
import com.transferwise.tasks.test.dao.MySqlTestTaskDao;
import com.transferwise.tasks.test.dao.PostgresTestTaskDao;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class TwTasksCoreTestAutoConfiguration {

  @Bean
  @ConditionalOnMissingBean(ITestTaskDao.class)
  @ConditionalOnProperty(value = "tw-tasks.core.db-type", havingValue = "POSTGRES")
  public PostgresTestTaskDao twTasksPostgresTestTaskDao(ITwTasksDataSourceProvider twTasksDataSourceProvider, TasksProperties properties,
      ITaskDaoDataSerializer taskDaoDataSerializer) {
    return new PostgresTestTaskDao(twTasksDataSourceProvider.getDataSource(), properties, taskDaoDataSerializer);
  }

  @Bean
  @ConditionalOnMissingBean(ITestTaskDao.class)
  @ConditionalOnProperty(value = "tw-tasks.core.db-type", havingValue = "MYSQL")
  public MySqlTestTaskDao twTasksMysqlTestTaskDao(ITwTasksDataSourceProvider twTasksDataSourceProvider, TasksProperties properties,
      ITaskDaoDataSerializer taskDaoDataSerializer) {
    return new MySqlTestTaskDao(twTasksDataSourceProvider.getDataSource(), properties, taskDaoDataSerializer);
  }

  @Bean
  public TestTasksService twTasksTestTasksService() {
    return new TestTasksService();
  }
}
