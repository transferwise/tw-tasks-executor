package com.transferwise.tasks.config;

import com.transferwise.common.baseutils.transactionsmanagement.ITransactionsHelper;
import com.transferwise.tasks.TasksProperties;
import com.transferwise.tasks.dao.ITaskDao;
import com.transferwise.tasks.dao.MongoTaskDao;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Bean;
import org.springframework.data.mongodb.MongoDatabaseFactory;
import org.springframework.data.mongodb.MongoTransactionManager;
import org.springframework.data.mongodb.core.MongoTemplate;

@ConditionalOnProperty(value = "tw-tasks.core.db-type", havingValue = "MONGO")
public class MongoConfiguration {

  @Bean
  @ConditionalOnMissingBean(MongoTransactionManager.class)
  MongoTransactionManager transactionManager(MongoDatabaseFactory databaseFactory) {
    return new MongoTransactionManager(databaseFactory);
  }

  @Bean
  @ConditionalOnMissingBean(ITaskDao.class)
  public ITaskDao twTasksMongoTaskDao(MongoTemplate mongoTemplate, TasksProperties tasksProperties) {
    return new MongoTaskDao(mongoTemplate, tasksProperties);
  }

  @Bean
  @ConditionalOnMissingBean(ITransactionsHelper.class)
  public ITransactionsHelper twMongoTransactionsHelper(MongoTransactionManager mongoTransactionManager) {
    return new MongoTransactionsHelper(mongoTransactionManager);
  }
}
