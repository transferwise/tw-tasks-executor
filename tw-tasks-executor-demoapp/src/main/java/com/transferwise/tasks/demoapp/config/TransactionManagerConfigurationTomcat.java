package com.transferwise.tasks.demoapp.config;

import com.transferwise.common.gaffer.ServiceRegistry;
import com.transferwise.common.gaffer.ServiceRegistryHolder;
import com.transferwise.common.gaffer.jdbc.DataSourceImpl;
import javax.sql.DataSource;
import javax.transaction.TransactionManager;
import javax.transaction.UserTransaction;
import org.springframework.beans.BeansException;
import org.springframework.beans.factory.config.BeanPostProcessor;
import org.springframework.boot.autoconfigure.jdbc.DataSourceProperties;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.jdbc.DatabaseDriver;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Primary;
import org.springframework.transaction.annotation.EnableTransactionManagement;
import org.springframework.transaction.jta.JtaTransactionManager;

@Configuration
@EnableTransactionManagement
public class TransactionManagerConfigurationTomcat {

  @Bean
  public UserTransaction gafferUserTransaction() {
    ServiceRegistry serviceRegistry = ServiceRegistryHolder.getServiceRegistry();
    return serviceRegistry.getUserTransaction();
  }

  @Bean
  public TransactionManager gafferTransactionManager() {
    ServiceRegistry serviceRegistry = ServiceRegistryHolder.getServiceRegistry();
    return serviceRegistry.getTransactionManager();
  }

  @Bean
  public JtaTransactionManager transactionManager() {
    ServiceRegistry serviceRegistry = ServiceRegistryHolder.getServiceRegistry();
    JtaTransactionManager jtaTransactionManager = new JtaTransactionManager(gafferUserTransaction(), gafferTransactionManager());
    jtaTransactionManager.setTransactionSynchronizationRegistry(serviceRegistry.getTransactionSynchronizationRegistry());
    return jtaTransactionManager;
  }

  @Bean
  @ConfigurationProperties(prefix = "spring.datasource.tomcat", ignoreUnknownFields = false)
  public DataSource tomcatDataSource(DataSourceProperties properties) {
    org.apache.tomcat.jdbc.pool.DataSource dataSource = createDataSource(properties);
    DatabaseDriver databaseDriver = DatabaseDriver.fromJdbcUrl(properties.determineUrl());
    String validationQuery = databaseDriver.getValidationQuery();
    if (validationQuery != null) {
      dataSource.setTestOnBorrow(true);
      dataSource.setValidationQuery(validationQuery);
    }
    return dataSource;
  }

  @Bean
  @Primary
  public BeanPostProcessor jtaDataSourceBeanPostProcessor() {
    return new BeanPostProcessor() {
      @Override
      public Object postProcessAfterInitialization(Object bean, String beanName) throws BeansException {
        if (beanName.equals("tomcatDataSource")) {
          DataSourceImpl dataSourceImpl = new DataSourceImpl();
          dataSourceImpl.setUniqueName("bssDb");
          dataSourceImpl.setDataSource((DataSource) bean);
          dataSourceImpl.setRegisterAsMBean(false);
          dataSourceImpl.init();

          return dataSourceImpl;
        }
        return bean;
      }
    };
  }

  @SuppressWarnings("unchecked")
  protected <T> T createDataSource(DataSourceProperties properties) {
    return (T) properties.initializeDataSourceBuilder().type((Class<? extends DataSource>) org.apache.tomcat.jdbc.pool.DataSource.class).build();
  }
}
