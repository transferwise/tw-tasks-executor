package com.transferwise.tasks;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.transferwise.common.baseutils.tracing.IXRequestIdHolder;
import com.transferwise.common.baseutils.transactionsmanagement.TransactionsConfiguration;
import com.transferwise.common.gracefulshutdown.GracefulShutdowner;
import com.transferwise.tasks.buckets.BucketsManager;
import com.transferwise.tasks.cleaning.TasksCleaner;
import com.transferwise.tasks.config.IExecutorServicesProvider;
import com.transferwise.tasks.config.TwTasksKafkaConfiguration;
import com.transferwise.tasks.dao.ITaskDao;
import com.transferwise.tasks.dao.MySqlTaskDao;
import com.transferwise.tasks.dao.PostgresTaskDao;
import com.transferwise.tasks.handler.TaskHandlerRegistry;
import com.transferwise.tasks.handler.interfaces.ITaskHandlerRegistry;
import com.transferwise.tasks.health.ClusterWideTasksStateMonitor;
import com.transferwise.tasks.health.TasksIncidentGenerator;
import com.transferwise.tasks.helpers.ErrorLoggingThrottler;
import com.transferwise.tasks.helpers.IMeterHelper;
import com.transferwise.tasks.helpers.MicrometerMeterHelper;
import com.transferwise.tasks.helpers.NoOpMeterHelper;
import com.transferwise.tasks.helpers.executors.DefaultExecutorServiceProvider;
import com.transferwise.tasks.helpers.executors.ExecutorsHelper;
import com.transferwise.tasks.helpers.kafka.AdminClientTopicPartitionsManager;
import com.transferwise.tasks.helpers.kafka.ITopicPartitionsManager;
import com.transferwise.tasks.helpers.kafka.NoOpTopicPartitionsManager;
import com.transferwise.tasks.helpers.kafka.messagetotask.CoreKafkaListener;
import com.transferwise.tasks.helpers.kafka.messagetotask.KafkaMessageHandlerFactory;
import com.transferwise.tasks.helpers.kafka.messagetotask.KafkaMessageHandlerRegistry;
import com.transferwise.tasks.impl.tokafka.ToKafkaProperties;
import com.transferwise.tasks.impl.tokafka.ToKafkaSenderService;
import com.transferwise.tasks.impl.tokafka.ToKafkaTaskHandlerConfiguration;
import com.transferwise.tasks.management.TasksManagementPortController;
import com.transferwise.tasks.management.TasksManagementService;
import com.transferwise.tasks.processing.GlobalProcessingState;
import com.transferwise.tasks.processing.ITasksProcessingService;
import com.transferwise.tasks.processing.TasksProcessingService;
import com.transferwise.tasks.stucktasks.ITasksResumer;
import com.transferwise.tasks.stucktasks.TasksResumer;
import com.transferwise.tasks.triggering.ITasksExecutionTriggerer;
import com.transferwise.tasks.triggering.KafkaTasksExecutionTriggerer;
import io.micrometer.core.instrument.MeterRegistry;
import lombok.extern.slf4j.Slf4j;
import org.apache.curator.framework.CuratorFramework;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.config.BeanDefinition;
import org.springframework.beans.factory.config.ConfigurableListableBeanFactory;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.autoconfigure.kafka.KafkaProperties;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;
import org.springframework.format.datetime.standard.DateTimeFormatterRegistrar;
import org.springframework.format.support.DefaultFormattingConversionService;
import org.springframework.format.support.FormattingConversionService;
import org.springframework.kafka.core.KafkaTemplate;

import javax.sql.DataSource;

@Configuration
@EnableConfigurationProperties({TasksProperties.class, ToKafkaProperties.class})
@Import({ToKafkaTaskHandlerConfiguration.class, TransactionsConfiguration.class})
@Slf4j
public class TwTasksAutoConfiguration {
    // Following is not used by the code, but makes sure, that someone has not turned graceful shutdown completely off.
    @Autowired
    private GracefulShutdowner gracefulShutdowner;

    @Bean
    public static TasksProperties.Validator twTasksTasksPropertiesValidator() {
        return new TasksProperties.Validator();
    }

    @Bean
    @ConditionalOnMissingBean(TwTasksDataSourceProvider.class)
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
                        "" + beanNames.length + " data source(s) found, but none is marked as Primary nor qualified with @TwTasks: " +
                            String.join(", ", beanNames));
                }
            }
        }
        return new TwTasksDataSourceProvider(dataSource);
    }

    @Bean
    @ConditionalOnMissingBean(ITaskDao.class)
    @ConditionalOnProperty(value = "tw-tasks.core.db-type", havingValue = "POSTGRES")
    public ITaskDao twTasksPostgresTaskDao(TwTasksDataSourceProvider twTasksDataSourceProvider) {
        return new PostgresTaskDao(twTasksDataSourceProvider.getDataSource());
    }

    @Bean
    @ConditionalOnMissingBean(ITaskDao.class)
    @ConditionalOnProperty(value = "tw-tasks.core.db-type", havingValue = "MYSQL")
    public ITaskDao twTasksMysqlTaskDao(TwTasksDataSourceProvider twTasksDataSourceProvider) {
        return new MySqlTaskDao(twTasksDataSourceProvider.getDataSource());
    }

    @Bean
    public ToKafkaSenderService twTasksToKafkaSenderService(
        ObjectMapper objectMapper, ITasksService taskService, ToKafkaProperties properties,
        @Autowired(required = false) IXRequestIdHolder xRequestIdHolder) {
        return new ToKafkaSenderService(objectMapper, taskService, properties.getBatchSizeMb(), xRequestIdHolder);
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
    public TasksResumer twTasksTasksResumer(
        ITasksExecutionTriggerer tasksExecutionTriggerer,
        ITaskHandlerRegistry taskHandlerRegistry,
        TasksProperties tasksProperties,
        ITaskDao taskDao,
        CuratorFramework curatorFramework,
        IExecutorServicesProvider executorServicesProvider,
        IMeterHelper meterHelper) {
        return new TasksResumer(
            tasksExecutionTriggerer,
            taskHandlerRegistry,
            tasksProperties,
            taskDao,
            curatorFramework,
            executorServicesProvider,
            meterHelper
        );
    }

    @Bean
    @ConditionalOnMissingBean(ITasksService.class)
    public TasksService twTasksTasksService() {
        return new TasksService();
    }

    @Bean
    public TaskHandlerRegistry twTasksTaskHandlerRegistry() {
        return new TaskHandlerRegistry();
    }

    @Bean
    public ExecutorsHelper twTasksExecutorsHelper() {
        return new ExecutorsHelper();
    }

    @Bean
    public ITopicPartitionsManager twTasksTopicPartitionsManager(TasksProperties tasksProperties) {
        if (tasksProperties.isConfigureKafkaTopics()) {
            return new AdminClientTopicPartitionsManager();
        }
        return new NoOpTopicPartitionsManager();
    }

    @Bean
    public TasksIncidentGenerator twTasksStuckTasksIncidentGenerator() {
        return new TasksIncidentGenerator();
    }

    @Bean
    public GlobalProcessingState twTasksGlobalProcessingState() {
        return new GlobalProcessingState();
    }

    @Bean
    public PriorityManager twTasksPriorityManager() {
        return new PriorityManager();
    }

    @Bean
    public BucketsManager twTasksBucketsManager() {
        return new BucketsManager();
    }

    @Bean
    public FormattingConversionService twTasksConversionService() {
        DefaultFormattingConversionService conversionService = new DefaultFormattingConversionService();
        new DateTimeFormatterRegistrar().registerFormatters(conversionService);
        return conversionService;
    }

    @Bean
    public TasksCleaner twTasksCleaner() {
        return new TasksCleaner();
    }

    @Bean
    public CoreKafkaListener twTasksCoreKafkaListener() {
        return new CoreKafkaListener();
    }

    @Bean
    public KafkaMessageHandlerRegistry twTasksKafkaMessageHandlerRegistry() {
        return new KafkaMessageHandlerRegistry();
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

    @Bean
    @ConditionalOnMissingBean(IExecutorServicesProvider.class)
    public IExecutorServicesProvider twTasksExecutorServicesProvider() {
        return new DefaultExecutorServiceProvider();
    }

    @Bean
    public ErrorLoggingThrottler twTasksErrorLoggingThrottler() {
        return new ErrorLoggingThrottler();
    }

    @Bean
    public IMeterHelper twTasksMeterHelper(@Autowired(required = false) MeterRegistry meterRegistry) {
        if (meterRegistry == null) {
            log.warn("Micrometer registry was not found. Falling back to NoOpMeterHelper.");
            return new NoOpMeterHelper();
        }
        return new MicrometerMeterHelper(meterRegistry);
    }

    @Bean
    public ClusterWideTasksStateMonitor twTasksEngineMonitor() {
        return new ClusterWideTasksStateMonitor();
    }

    @Bean
    @ConditionalOnMissingBean
    public TwTasksKafkaConfiguration twTaskKafkaConfiguration(KafkaProperties kafkaProperties, KafkaTemplate<String, String> kafkaTemplate) {
        return new TwTasksKafkaConfiguration(kafkaProperties, kafkaTemplate);
    }

    @Bean
    public KafkaMessageHandlerFactory kafkaMessageHandlerFactory(ITasksService tasksService, ObjectMapper objectMapper) {
        return new KafkaMessageHandlerFactory(tasksService, objectMapper);
    }

    public static class TwTasksDataSourceProvider {
        private final DataSource dataSource;

        public TwTasksDataSourceProvider(DataSource dataSource) {
            this.dataSource = dataSource;
        }

        public DataSource getDataSource() {
            return dataSource;
        }
    }
}
