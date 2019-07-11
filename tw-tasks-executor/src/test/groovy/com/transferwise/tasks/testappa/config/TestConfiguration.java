package com.transferwise.tasks.testappa.config;

import com.transferwise.tasks.helpers.kafka.messagetotask.IKafkaMessageHandler;
import com.transferwise.tasks.impl.tokafka.test.IToKafkaTestHelper;
import com.transferwise.tasks.impl.tokafka.test.ToKafkaTestHelper;
import com.transferwise.tasks.processing.ITaskProcessingInterceptor;
import com.transferwise.tasks.test.TestTasksService;
import com.transferwise.tasks.utils.LogUtils;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.util.Collections;
import java.util.List;

@Configuration
@Slf4j
public class TestConfiguration {
    public static final String KAFKA_TEST_TOPIC_A = "myTopicA";
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
    IKafkaMessageHandler aHandlerForTopicA() {
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

