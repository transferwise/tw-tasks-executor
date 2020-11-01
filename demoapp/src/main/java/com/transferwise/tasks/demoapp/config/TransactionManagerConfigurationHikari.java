package com.transferwise.tasks.demoapp.config;

import com.transferwise.common.gaffer.ServiceRegistry;
import com.transferwise.common.gaffer.ServiceRegistryHolder;
import com.transferwise.common.gaffer.jdbc.DataSourceImpl;
import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import javax.sql.DataSource;
import javax.transaction.TransactionManager;
import javax.transaction.UserTransaction;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Primary;
import org.springframework.core.env.Environment;
import org.springframework.jdbc.datasource.IsolationLevelDataSourceAdapter;
import org.springframework.transaction.annotation.EnableTransactionManagement;
import org.springframework.transaction.jta.JtaTransactionManager;

//@Configuration
@EnableTransactionManagement
public class TransactionManagerConfigurationHikari {

  @Autowired
  private Environment env;

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
    jtaTransactionManager.setAllowCustomIsolationLevels(true);
    jtaTransactionManager.setTransactionSynchronizationRegistry(serviceRegistry.getTransactionSynchronizationRegistry());
    return jtaTransactionManager;
  }

  @Bean
  @Primary
  public DataSource jtaDataSource() {
    DataSourceImpl dataSourceImpl = new DataSourceImpl();
    dataSourceImpl.setUniqueName("twTasksDemoappDb");
    dataSourceImpl.setDataSource(dataSource());
    dataSourceImpl.setValidationTimeoutSeconds(10);
    dataSourceImpl.setRegisterAsMBean(false);

    IsolationLevelDataSourceAdapter da = new IsolationLevelDataSourceAdapter();
    da.setTargetDataSource(dataSourceImpl);
    return da;
  }

  private DataSource dataSource() {
    HikariConfig config = new HikariConfig();
    config.setJdbcUrl(env.getProperty("spring.datasource.url"));
    config.setUsername(env.getProperty("spring.datasource.username"));
    config.setPassword(env.getProperty("spring.datasource.password"));
    config.setConnectionTimeout(10000);
    config.setTransactionIsolation("TRANSACTION_READ_COMMITTED");
    config.setRegisterMbeans(true);
    config.setMaxLifetime(1800000);
    config.setMaximumPoolSize(30);
    config.setMinimumIdle(30);
    config.setIdleTimeout(60000);
    config.setValidationTimeout(10000);

    if (config.getJdbcUrl().contains("mysql") || config.getJdbcUrl().contains("mariadb")) {
      config.addDataSourceProperty("cachePrepStmts", "true");
      config.addDataSourceProperty("prepStmtCacheSize", "250");
      config.addDataSourceProperty("prepStmtCacheSqlLimit", "4096"); // Long live the Hibernate!
      config.addDataSourceProperty("useServerPrepStmts", "true");

      config.addDataSourceProperty("cacheCallableStmts", "true");
      config.addDataSourceProperty("callableStmtCacheSize", "250");

      config.addDataSourceProperty("cacheResultSetMetadata", "true");
      config.addDataSourceProperty("metadataCacheSize", "250");

      config.addDataSourceProperty("useLocalSessionState", "true");
      config.addDataSourceProperty("useLocalTransactionState", "true"); // Dangerous to have true.
      config.addDataSourceProperty("cacheServerConfiguration", "true");
      config.addDataSourceProperty("elideSetAutoCommits", "true");
      config.addDataSourceProperty("alwaysSendSetIsolation", "false");

      config.addDataSourceProperty("rewriteBatchedStatements", "true");
      config.addDataSourceProperty("maintainTimeStats", "false");
      config.addDataSourceProperty("useJvmCharsetConverters", "true");
      config.addDataSourceProperty("useSSL", "false");
      config.addDataSourceProperty("characterEncoding", "UTF-8");
      config.addDataSourceProperty("useUnicode", "true");

      config.addDataSourceProperty("dontTrackOpenResources", "true");
      config.addDataSourceProperty("holdResultsOpenOverStatementClose", "true");
      config.addDataSourceProperty("enableQueryTimeouts", "false");

      config.addDataSourceProperty("connectTimeout", "10000");
      config.addDataSourceProperty("socketTimeout", "600000");
    }

    return new HikariDataSource(config);
  }
}
