package com.transferwise.tasks.testapp.config;

import com.transferwise.common.context.TwContextClockHolder;
import com.transferwise.tasks.TasksProperties;
import com.transferwise.tasks.buckets.BucketProperties;
import com.transferwise.tasks.buckets.IBucketsManager;
import com.transferwise.tasks.config.TwTasksKafkaConfiguration;
import com.transferwise.tasks.domain.ITask;
import com.transferwise.tasks.ext.kafkalistener.KafkaListenerExtTestConfiguration;
import com.transferwise.tasks.helpers.kafka.messagetotask.IKafkaMessageHandler;
import com.transferwise.tasks.impl.jobs.interfaces.IJob;
import com.transferwise.tasks.impl.jobs.test.JobsTestConfiguration;
import com.transferwise.tasks.impl.tokafka.test.IToKafkaTestHelper;
import com.transferwise.tasks.impl.tokafka.test.ToKafkaTestHelper;
import com.transferwise.tasks.processing.ITaskProcessingInterceptor;
import com.transferwise.tasks.test.TestTasksService;
import com.transferwise.tasks.test.dao.MySqlTestTaskDao;
import com.transferwise.tasks.test.dao.PostgresTestTaskDao;
import com.transferwise.tasks.test.dao.TestTaskDao;
import com.transferwise.tasks.utils.LogUtils;
import java.time.ZonedDateTime;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import javax.annotation.PostConstruct;
import javax.sql.DataSource;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;

@Configuration
@Slf4j
@Import({JobsTestConfiguration.class, KafkaListenerExtTestConfiguration.class})
public class TestConfiguration {

  public static final String KAFKA_TEST_TOPIC_A = "myTopicA";
  public static final String TEST_JOB_UNIQUE_NAME = "MyFancyJobA";

  @Autowired
  private IBucketsManager bucketsManager;

  @Autowired
  private TwTasksKafkaConfiguration kafkaConfiguration;

  @PostConstruct
  public void init() {
    bucketsManager.registerBucketProperties("manualStart", new BucketProperties()
        .setAutoStartProcessing(false));

    AdminClient adminClient = AdminClient.create(kafkaConfiguration.getKafkaProperties().buildAdminProperties());

    List<NewTopic> newTopics = Arrays.asList(new NewTopic("twTasks.test-mysql.executeTask.manualStart", 1, (short) 1),
        new NewTopic("twTasks.test-mysql.executeTask.default", 1, (short) 1),
        new NewTopic("ToKafkaTest", 100, (short) 1),
        new NewTopic("toKafkaBatchTestTopic", 1, (short) 1),
        new NewTopic("toKafkaBatchTestTopic5Partitions", 5, (short) 1));

    adminClient.createTopics(newTopics);
    adminClient.close();
  }

  @Bean
  @ConditionalOnProperty(value = "tw-tasks.core.db-type", havingValue = "POSTGRES")
  public TestTaskDao postgresTestTaskDao(DataSource dataSource, TasksProperties tasksProperties) {
    return new PostgresTestTaskDao(dataSource, tasksProperties);
  }

  @Bean
  @ConditionalOnProperty(value = "tw-tasks.core.db-type", havingValue = "MYSQL")
  public TestTaskDao mysqlTestTaskDao(DataSource dataSource, TasksProperties tasksProperties) {
    return new MySqlTestTaskDao(dataSource, tasksProperties);
  }

  @Bean
  public TestTasksService testTasksService() {
    return new TestTasksService();
  }

  @Bean
  public ITaskProcessingInterceptor taskProcessingInterceptor() {
    return (task, processor) -> {
      if (log.isDebugEnabled()) {
        log.debug("Task {} got intercepted.", LogUtils.asParameter(task.getVersionId()));
      }
      processor.run();
    };
  }

  @Bean
  public IToKafkaTestHelper toKafkaTestHelper() {
    return new ToKafkaTestHelper();
  }

  @Bean
  IKafkaMessageHandler newHandlerForTopicA() {
    return new IKafkaMessageHandler() {
      @Override
      public List<Topic> getTopics() {
        return Collections.singletonList(new IKafkaMessageHandler.Topic().setAddress(KAFKA_TEST_TOPIC_A));
      }

      @Override
      public boolean handles(String topic) {
        return topic.equals(KAFKA_TEST_TOPIC_A);
      }

      @Override
      public void handle(ConsumerRecord record) {
      }
    };
  }

  @Bean
  public IJob myFancyJobA() {

    return new IJob() {
      @Override
      public ZonedDateTime getNextRunTime() {
        return ZonedDateTime.now(TwContextClockHolder.getClock()).plusYears(20);
      }

      @Override
      public ProcessResult process(ITask task) {
        return null;
      }

      @Override
      public String getUniqueName() {
        return TEST_JOB_UNIQUE_NAME;
      }
    };
  }
}
