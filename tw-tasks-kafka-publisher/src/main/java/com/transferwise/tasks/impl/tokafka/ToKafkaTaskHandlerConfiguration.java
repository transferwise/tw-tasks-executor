package com.transferwise.tasks.impl.tokafka;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.Multimap;
import com.transferwise.common.baseutils.ExceptionUtils;
import com.transferwise.tasks.config.TwTasksKafkaConfiguration;
import com.transferwise.tasks.handler.ExponentialTaskRetryPolicy;
import com.transferwise.tasks.handler.SimpleTaskConcurrencyPolicy;
import com.transferwise.tasks.handler.SimpleTaskProcessingPolicy;
import com.transferwise.tasks.handler.TaskHandlerAdapter;
import com.transferwise.tasks.handler.interfaces.IAsyncTaskProcessor;
import com.transferwise.tasks.handler.interfaces.ITaskHandler;
import com.transferwise.tasks.helpers.IMeterHelper;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Tags;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Slf4j
@Configuration
public class ToKafkaTaskHandlerConfiguration {

  @Autowired(required = false)
  private MeterRegistry meterRegistry;

  @Bean
  public ITaskHandler toKafkaTaskHandler(TwTasksKafkaConfiguration kafkaConfiguration,
      ObjectMapper objectMapper, ToKafkaProperties toKafkaProperties) {
    return new TaskHandlerAdapter(
        (task) -> ToKafkaTaskType.VALUE.equals(task.getType()),
        (IAsyncTaskProcessor) (taskForProcessing, doneCallback, errorCallback) -> ExceptionUtils.doUnchecked(() -> {
          ToKafkaMessages toKafkaMessages = objectMapper.readValue(taskForProcessing.getData(), ToKafkaMessages.class);

          AtomicInteger doneCnt = new AtomicInteger(toKafkaMessages.getMessages().size());

          String topic = toKafkaMessages.getTopic();
          for (ToKafkaMessages.Message message : toKafkaMessages.getMessages()) {
            ProducerRecord<String, String> producerRecord = new ProducerRecord<>(
                topic,
                null,
                message.getKey(),
                message.getMessage(),
                toKafkaHeaders(message.getHeaders()));
            kafkaConfiguration.getKafkaTemplate()
                .send(producerRecord)
                .addCallback(
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
    ).setConcurrencyPolicy(new SimpleTaskConcurrencyPolicy(toKafkaProperties.getMaxConcurrency()))
        .setProcessingPolicy(
            new SimpleTaskProcessingPolicy()
                .setMaxProcessingDuration(Duration.ofMillis(toKafkaProperties.getMaxProcessingDurationMs()))
                .setDeleteTaskOnFinish(true)
        )
        .setRetryPolicy(
            new ExponentialTaskRetryPolicy()
                .setDelay(Duration.ofMillis(toKafkaProperties.getRetryDelayMs()))
                .setMultiplier(toKafkaProperties.getRetryExponent())
                .setMaxCount(toKafkaProperties.getRetryMaxCount())
                .setMaxDelay(Duration.ofMillis(toKafkaProperties.getRetryMaxDelayMs()))
        );
  }

  private List<Header> toKafkaHeaders(Multimap<String, byte[]> headers) {
    if (headers == null) {
      return null;
    }
    return headers.entries()
        .stream()
        .map(header -> new RecordHeader(header.getKey(), header.getValue()))
        .collect(Collectors.toList());
  }

  private void registerSentMessage(String topic) {
    if (meterRegistry != null) {
      meterRegistry.counter(IMeterHelper.METRIC_PREFIX + "toKafka.sentMessagesCount", Tags.of("topic", topic)).increment();
    }
  }
}
