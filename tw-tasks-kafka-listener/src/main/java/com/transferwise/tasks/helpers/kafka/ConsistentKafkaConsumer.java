package com.transferwise.tasks.helpers.kafka;

import com.transferwise.common.context.UnitOfWorkManager;
import com.transferwise.tasks.entrypoints.EntryPoint;
import com.transferwise.tasks.entrypoints.EntryPointsGroups;
import com.transferwise.tasks.helpers.IErrorLoggingThrottler;
import com.transferwise.tasks.helpers.kafka.meters.IKafkaListenerMetricsTemplate;
import com.transferwise.tasks.utils.WaitUtils;
import java.time.Duration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import lombok.Setter;
import lombok.experimental.Accessors;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.mutable.MutableObject;
import org.apache.kafka.clients.consumer.CommitFailedException;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.RetriableException;

@Accessors(chain = true)
@Slf4j
public class ConsistentKafkaConsumer<T> {

  @Setter
  private List<String> topics;
  @Setter
  private Supplier<Map<String, Object>> kafkaPropertiesSupplier;
  @Setter
  private Supplier<Boolean> shouldFinishPredicate;
  @Setter
  private Supplier<Boolean> shouldPollPredicate;
  @Setter
  private java.util.function.Consumer<ConsumerRecord<String, T>> recordConsumer;
  @Setter
  private Duration delayTimeout = Duration.ofSeconds(5);
  @Setter
  private IErrorLoggingThrottler errorLoggingThrottler;
  @Setter
  private UnitOfWorkManager unitOfWorkManager;
  @Setter
  private IKafkaListenerMetricsTemplate kafkaListenerMetricsTemplate;

  public void consume() {
    MutableObject<Consumer<String, T>> consumerHolder = new MutableObject<>();
    try {
      while (!shouldFinishPredicate.get()) {
        try {
          //TODO: Add Newrelic trace.
          if (consumerHolder.getValue() == null) {
            Map<String, Object> kafkaConsumerProps = new HashMap<>(kafkaPropertiesSupplier.get());
            kafkaConsumerProps.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
            consumerHolder.setValue(new KafkaConsumer<>(kafkaConsumerProps));
            consumerHolder.getValue().subscribe(topics);
          }

          boolean shouldPoll = shouldPollPredicate.get();
          if (!shouldPoll) {
            continue;
          }

          log.debug("Polling from Kafka.");
          ConsumerRecords<String, T> consumerRecords = consumerHolder.getValue().poll(delayTimeout);
          handlePolledKafkaMessages(consumerHolder, consumerRecords);
        } catch (Throwable t) {
          log.error(t.getMessage(), t);
          WaitUtils.sleepQuietly(delayTimeout);
        }
      }
    } finally {
      close(consumerHolder);
    }
  }

  @EntryPoint
  protected void handlePolledKafkaMessages(MutableObject<Consumer<String, T>> consumerHolder, ConsumerRecords<String, T> consumerRecords) {
    unitOfWorkManager.createEntryPoint(EntryPointsGroups.TW_TASKS_ENGINE, "handlePolledKafkaMessages").toContext().execute(() -> {
      if (log.isDebugEnabled()) {
        log.debug("Polled " + consumerRecords.count() + " messages from Kafka.");
      }
      if (consumerRecords.isEmpty()) {
        if (log.isDebugEnabled()) {
          log.debug("No records found for: " + StringUtils.join(consumerHolder.getValue().subscription(), ", "));
        }
        return;
      }

      Map<TopicPartition, Long> committableOffsets = new HashMap<>();

      for (ConsumerRecord<String, T> consumerRecord : consumerRecords) {
        if (log.isDebugEnabled()) {
          log.debug("Received Kafka message from topic '{}', partition {}, offset {}.", consumerRecord.topic(), consumerRecord.partition(),
              consumerRecord
                  .offset());
        }

        TopicPartition topicPartition = new TopicPartition(consumerRecord.topic(), consumerRecord.partition());

        while (!shouldFinishPredicate.get()) {
          try {
            recordConsumer.accept(consumerRecord);
            committableOffsets.put(topicPartition, consumerRecord.offset() + 1);
            break;
          } catch (Throwable t) {
            log.error("Accepting Kafka message from topic '" + consumerRecord.topic() + "', partition " + consumerRecord
                .partition() + ", offset " + consumerRecord
                .offset() + " failed.", t);
            WaitUtils.sleepQuietly(delayTimeout);
          }
        }
      }

      if (!committableOffsets.isEmpty()) {
        Map<TopicPartition, OffsetAndMetadata> commitOffsetMap = committableOffsets.entrySet().stream().collect(Collectors.toMap(Map.Entry::getKey,
            entry -> new OffsetAndMetadata(entry.getValue())));

        if (shouldFinishPredicate.get()) {
          try {
            consumerHolder.getValue().commitSync(commitOffsetMap);
          } catch (Throwable t) {
            registerCommitException(t);
          }
        } else {
          consumerHolder.getValue().commitAsync(commitOffsetMap, (map, e) -> {
            if (e == null) {
              log.debug("Committing offsets completed.");
            } else {
              registerCommitException(e);
            }
          });
        }
      }
    });
  }

  protected void registerCommitException(Throwable t) {
    if (t instanceof CommitFailedException || t instanceof RetriableException) { // Topic got rebalanced on shutdown.
      if (kafkaListenerMetricsTemplate != null) {
        kafkaListenerMetricsTemplate.registerFailedCommit();
      }
      log.debug("Committing Kafka offset failed.", t);
      return;
    }
    if (errorLoggingThrottler == null || errorLoggingThrottler.canLogError()) {
      log.error("Committing Kafka offset failed.", t);
    }
  }

  private void close(MutableObject<Consumer<String, T>> consumerHolder) {
    if (consumerHolder == null || consumerHolder.getValue() == null) {
      return;
    }

    try {
      log.info("Unsubscribing from topic '" + StringUtils.join(consumerHolder.getValue().subscription(), ",'") + "'.");
      consumerHolder.getValue().unsubscribe();
    } catch (Throwable t) {
      log.error(t.getMessage(), t);
    }
    try {
      consumerHolder.getValue().close();
    } catch (Throwable t) {
      log.error(t.getMessage(), t);
    }
    consumerHolder.setValue(null);
  }
}
