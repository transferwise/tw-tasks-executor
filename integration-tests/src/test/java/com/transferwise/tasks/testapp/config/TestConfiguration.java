package com.transferwise.tasks.testapp.config;

import com.transferwise.tasks.buckets.BucketProperties;
import com.transferwise.tasks.buckets.IBucketsManager;
import com.transferwise.tasks.config.TwTasksKafkaConfiguration;
import com.transferwise.tasks.helpers.kafka.messagetotask.IKafkaMessageHandler;
import com.transferwise.tasks.impl.tokafka.test.IToKafkaTestHelper;
import com.transferwise.tasks.impl.tokafka.test.ToKafkaTestHelper;
import com.transferwise.tasks.processing.ITaskProcessingInterceptor;
import com.transferwise.tasks.test.TestTasksService;
import com.transferwise.tasks.utils.LogUtils;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import javax.annotation.PostConstruct;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
@Slf4j
public class TestConfiguration {

  public static final String KAFKA_TEST_TOPIC_A = "myTopicA";

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
  @ConditionalOnProperty(value = "testcontainers.enabled", matchIfMissing = true)
  public TestContainersManager testContainersManager() {
    return new TestContainersManager();
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
}
