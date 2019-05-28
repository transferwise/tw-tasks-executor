package com.transferwise.tasks.impl.tokafka;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.transferwise.common.baseutils.ExceptionUtils;
import com.transferwise.tasks.handler.ExponentialTaskRetryPolicy;
import com.transferwise.tasks.handler.SimpleTaskConcurrencyPolicy;
import com.transferwise.tasks.handler.SimpleTaskProcessingPolicy;
import com.transferwise.tasks.handler.TaskHandlerAdapter;
import com.transferwise.tasks.handler.interfaces.IAsyncTaskProcessor;
import com.transferwise.tasks.handler.interfaces.ITaskHandler;
import com.transferwise.tasks.helpers.IMeterHelper;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Tags;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.core.KafkaTemplate;

import java.time.Duration;
import java.util.concurrent.atomic.AtomicInteger;

@Slf4j
@Configuration
public class ToKafkaTaskHandlerConfiguration {
    @Autowired(required = false)
    private MeterRegistry meterRegistry;

    @Bean
    public ITaskHandler toKafkaTaskHandler(KafkaTemplate<String, String> kafkaTemplate,
                                           ObjectMapper objectMapper, ToKafkaProperties toKafkaProperties) {
        return new TaskHandlerAdapter(
            (task) -> ToKafkaTaskType.VALUE.equals(task.getType()),
            (IAsyncTaskProcessor) (taskForProcessing, doneCallback, errorCallback) -> ExceptionUtils.doUnchecked(() -> {
                ToKafkaMessages toKafkaMessages = objectMapper.readValue(taskForProcessing.getData(), ToKafkaMessages.class);

                AtomicInteger doneCnt = new AtomicInteger(toKafkaMessages.getMessages().size());

                String topic = toKafkaMessages.getTopic();
                for (ToKafkaMessages.Message message : toKafkaMessages.getMessages()) {
                    kafkaTemplate.send(topic, message.getKey(), message.getMessage()).addCallback(
                        result -> {
                            log.debug("Sent and acked Kafka message to topic '{}'.", topic);
                            registerSentMessage(topic);
                            if (doneCnt.decrementAndGet() == 0) {
                                doneCallback.run();
                            }
                        },
                        exception -> {
                            log.error("Sending message to Kafka topic '" + topic + "'.", exception);
                            errorCallback.accept(exception);
                        });
                }
            })
        ).setConcurrencyPolicy(new SimpleTaskConcurrencyPolicy(toKafkaProperties.getMaxConcurrency())).setProcessingPolicy(
            new SimpleTaskProcessingPolicy().setMaxProcessingDuration(Duration.ofMillis(toKafkaProperties.getMaxProcessingDurationMs()))).setRetryPolicy(
            new ExponentialTaskRetryPolicy().setDelay(Duration.ofMillis(toKafkaProperties.getRetryDelayMs())).setMultiplier(toKafkaProperties.getRetryExponent())
                .setMaxCount(toKafkaProperties.getRetryMaxCount()).setMaxDelay(Duration.ofMillis(toKafkaProperties.getRetryMaxDelayMs())));
    }

    private void registerSentMessage(String topic) {
        if (meterRegistry != null) {
            meterRegistry.counter(IMeterHelper.METRIC_PREFIX + "toKafka.sentMessagesCount", Tags.of("topic", topic));
        }
    }
}
