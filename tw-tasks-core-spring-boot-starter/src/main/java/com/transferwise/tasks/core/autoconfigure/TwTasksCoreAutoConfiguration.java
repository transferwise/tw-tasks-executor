package com.transferwise.tasks.core.autoconfigure;

import com.transferwise.common.baseutils.concurrency.DefaultExecutorServicesProvider;
import com.transferwise.common.baseutils.concurrency.IExecutorServicesProvider;
import com.transferwise.common.baseutils.transactionsmanagement.TransactionsConfiguration;
import com.transferwise.common.gracefulshutdown.GracefulShutdowner;
import com.transferwise.tasks.EnvironmentValidator;
import com.transferwise.tasks.IEnvironmentValidator;
import com.transferwise.tasks.IPriorityManager;
import com.transferwise.tasks.ITaskDataSerializer;
import com.transferwise.tasks.ITasksService;
import com.transferwise.tasks.JavaxEnvironmentValidator;
import com.transferwise.tasks.PriorityManager;
import com.transferwise.tasks.TaskDataSerializer;
import com.transferwise.tasks.TasksProperties;
import com.transferwise.tasks.TasksService;
import com.transferwise.tasks.TwTasks;
import com.transferwise.tasks.buckets.BucketsManager;
import com.transferwise.tasks.buckets.IBucketsManager;
import com.transferwise.tasks.cleaning.ITasksCleaner;
import com.transferwise.tasks.cleaning.TasksCleaner;
import com.transferwise.tasks.dao.ITaskDao;
import com.transferwise.tasks.dao.ITaskDaoDataSerializer;
import com.transferwise.tasks.dao.MySqlTaskDao;
import com.transferwise.tasks.dao.PostgresTaskDao;
import com.transferwise.tasks.dao.TaskDaoDataSerializer;
import com.transferwise.tasks.entrypoints.EntryPointsService;
import com.transferwise.tasks.entrypoints.IEntryPointsService;
import com.transferwise.tasks.entrypoints.IMdcService;
import com.transferwise.tasks.entrypoints.MdcService;
import com.transferwise.tasks.handler.TaskHandlerRegistry;
import com.transferwise.tasks.handler.interfaces.ITaskHandlerRegistry;
import com.transferwise.tasks.health.ClusterWideTasksStateMonitor;
import com.transferwise.tasks.health.ITasksStateMonitor;
import com.transferwise.tasks.helpers.CoreMetricsTemplate;
import com.transferwise.tasks.helpers.ErrorLoggingThrottler;
import com.transferwise.tasks.helpers.ICoreMetricsTemplate;
import com.transferwise.tasks.helpers.IErrorLoggingThrottler;
import com.transferwise.tasks.helpers.executors.ExecutorsHelper;
import com.transferwise.tasks.helpers.executors.IExecutorsHelper;
import com.transferwise.tasks.helpers.kafka.ITopicPartitionsManager;
import com.transferwise.tasks.helpers.kafka.NoOpTopicPartitionsManager;
import com.transferwise.tasks.helpers.kafka.partitionkey.IPartitionKeyStrategy;
import com.transferwise.tasks.helpers.kafka.partitionkey.RandomPartitionKeyStrategy;
import com.transferwise.tasks.processing.GlobalProcessingState;
import com.transferwise.tasks.processing.ITasksProcessingService;
import com.transferwise.tasks.processing.TasksProcessingService;
import com.transferwise.tasks.stucktasks.ITasksResumer;
import com.transferwise.tasks.stucktasks.TasksResumer;
import com.transferwise.tasks.triggering.ITasksExecutionTriggerer;
import com.transferwise.tasks.triggering.KafkaTasksExecutionTriggerer;
import javax.sql.DataSource;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.config.BeanDefinition;
import org.springframework.beans.factory.config.ConfigurableListableBeanFactory;
import org.springframework.boot.autoconfigure.AutoConfigureAfter;
import org.springframework.boot.autoconfigure.condition.ConditionalOnBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.autoconfigure.validation.ValidationAutoConfiguration;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;

@Configuration
@Slf4j
@Import(TransactionsConfiguration.class)
@EnableConfigurationProperties
@AutoConfigureAfter({ValidationAutoConfiguration.class})
public class TwTasksCoreAutoConfiguration {

  // Following is not used by the code, but makes sure, that someone has not turned graceful shutdown completely off.
  @Autowired
  private GracefulShutdowner gracefulShutdowner;

  @Bean
  @ConfigurationProperties(prefix = "tw-tasks.core", ignoreUnknownFields = false)
  public TasksProperties twTasksProperties() {
    return new TasksProperties();
  }

  @Bean
  @ConditionalOnMissingBean(ITwTasksDataSourceProvider.class)
  public TwTasksDataSourceProvider twTasksDataSourceProvider(
      @Autowired(required = false) @TwTasks DataSource dataSource, ConfigurableListableBeanFactory beanFactory) {
    if (dataSource == null) {
      String[] beanNames = beanFactory.getBeanNamesForType(DataSource.class);
      if (beanNames.length == 0) {
        throw new IllegalStateException("No DataSource bean(s) found.");
      } else if (beanNames.length == 1) {
        dataSource = beanFactory.getBean(beanNames[0], DataSource.class);
      } else {
        for (String beanName : beanNames) {
          BeanDefinition bd = beanFactory.getBeanDefinition(beanName);
          if (bd.isPrimary()) {
            dataSource = beanFactory.getBean(beanName, DataSource.class);
            break;
          }
        }
        if (dataSource == null) {
          throw new IllegalStateException(
              "" + beanNames.length + " data source(s) found, but none is marked as Primary nor qualified with @TwTasks: "
                  + String.join(", ", beanNames));
        }
      }
    }
    return new TwTasksDataSourceProvider(dataSource);
  }

  @Bean
  @ConditionalOnMissingBean(ITaskDao.class)
  @ConditionalOnProperty(value = "tw-tasks.core.db-type", havingValue = "POSTGRES")
  public PostgresTaskDao twTasksPostgresTaskDao(ITwTasksDataSourceProvider twTasksDataSourceProvider) {
    return new PostgresTaskDao(twTasksDataSourceProvider.getDataSource());
  }

  @Bean
  @ConditionalOnMissingBean(ITaskDao.class)
  @ConditionalOnProperty(value = "tw-tasks.core.db-type", havingValue = "MYSQL")
  public MySqlTaskDao twTasksMysqlTaskDao(ITwTasksDataSourceProvider twTasksDataSourceProvider) {
    return new MySqlTaskDao(twTasksDataSourceProvider.getDataSource());
  }

  @Bean
  @ConditionalOnMissingBean(ITasksProcessingService.class)
  public TasksProcessingService twTasksTasksProcessingService() {
    return new TasksProcessingService();
  }

  @Bean
  @ConditionalOnMissingBean(ITasksExecutionTriggerer.class)
  public KafkaTasksExecutionTriggerer twTasksTasksExecutionTriggerer() {
    return new KafkaTasksExecutionTriggerer();
  }

  @Bean
  @ConditionalOnMissingBean(ITasksResumer.class)
  public TasksResumer twTasksTasksResumer() {
    return new TasksResumer();
  }

  @Bean
  @ConditionalOnMissingBean(ITasksService.class)
  public TasksService twTasksTasksService() {
    return new TasksService();
  }

  @Bean
  @ConditionalOnMissingBean(ITaskHandlerRegistry.class)
  public TaskHandlerRegistry twTasksTaskHandlerRegistry() {
    return new TaskHandlerRegistry();
  }

  @Bean
  @ConditionalOnMissingBean(IExecutorsHelper.class)
  public ExecutorsHelper twTasksExecutorsHelper() {
    return new ExecutorsHelper();
  }

  @Bean
  @ConditionalOnMissingBean(ITopicPartitionsManager.class)
  public NoOpTopicPartitionsManager twTasksTopicPartitionsManager() {
    return new NoOpTopicPartitionsManager();
  }

  @Bean
  public GlobalProcessingState twTasksGlobalProcessingState() {
    return new GlobalProcessingState();
  }

  @Bean
  @ConditionalOnMissingBean(IPriorityManager.class)
  public PriorityManager twTasksPriorityManager() {
    return new PriorityManager();
  }

  @Bean
  @ConditionalOnMissingBean(IBucketsManager.class)
  public BucketsManager twTasksBucketsManager() {
    return new BucketsManager();
  }

  @Bean
  @ConditionalOnMissingBean(ITasksCleaner.class)
  public TasksCleaner twTasksCleaner() {
    return new TasksCleaner();
  }

  @Bean
  @ConditionalOnMissingBean(IExecutorServicesProvider.class)
  public DefaultExecutorServicesProvider twTasksExecutorServicesProvider() {
    return new DefaultExecutorServicesProvider();
  }

  @Bean
  @ConditionalOnMissingBean(IErrorLoggingThrottler.class)
  public ErrorLoggingThrottler twTasksErrorLoggingThrottler() {
    return new ErrorLoggingThrottler();
  }

  @Bean
  @ConditionalOnMissingBean(ICoreMetricsTemplate.class)
  public ICoreMetricsTemplate twTasksCoreMetricsTemplate() {
    return new CoreMetricsTemplate();
  }

  @Bean
  @ConditionalOnMissingBean(ITasksStateMonitor.class)
  public ClusterWideTasksStateMonitor twTasksEngineMonitor() {
    return new ClusterWideTasksStateMonitor();
  }

  @Bean
  @ConditionalOnMissingBean(IMdcService.class)
  public MdcService twTasksMdcService() {
    return new MdcService();
  }

  @Bean
  @ConditionalOnMissingBean(IEntryPointsService.class)
  public EntryPointsService twTasksEntryPointsService() {
    return new EntryPointsService();
  }

  @Bean
  @ConditionalOnMissingBean(ITaskDaoDataSerializer.class)
  public TaskDaoDataSerializer twTasksTaskDaoDataSerializer() {
    return new TaskDaoDataSerializer();
  }

  @Bean
  @ConditionalOnMissingBean(ITaskDataSerializer.class)
  public TaskDataSerializer twTasksTaskDataSerializer() {
    return new TaskDataSerializer();
  }

  @Bean
  @ConditionalOnMissingBean(IEnvironmentValidator.class)
  @ConditionalOnBean(type = "jakarta.validation.Validator")
  public EnvironmentValidator twTasksEnvironmentValidator() {
    return new EnvironmentValidator();
  }

  @Bean
  @ConditionalOnMissingBean(IEnvironmentValidator.class)
  @ConditionalOnBean(type = "javax.validation.Validator")
  public JavaxEnvironmentValidator twTasksJavaxEnvironmentValidator() {
    return new JavaxEnvironmentValidator();
  }

  @Bean
  @ConditionalOnMissingBean(IPartitionKeyStrategy.class)
  public IPartitionKeyStrategy twTasksPartitionKeyStrategy() {
    return new RandomPartitionKeyStrategy();
  }
}
