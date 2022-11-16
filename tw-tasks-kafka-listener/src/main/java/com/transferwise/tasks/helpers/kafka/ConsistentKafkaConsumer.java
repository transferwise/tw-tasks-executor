package com.transferwise.tasks.helpers.kafka;

import com.transferwise.common.baseutils.ExceptionUtils;
import com.transferwise.common.context.TwContextClockHolder;
import com.transferwise.common.context.UnitOfWorkManager;
import com.transferwise.tasks.entrypoints.EntryPoint;
import com.transferwise.tasks.entrypoints.EntryPointsGroups;
import com.transferwise.tasks.helpers.IErrorLoggingThrottler;
import com.transferwise.tasks.helpers.kafka.meters.IKafkaListenerMetricsTemplate;
import com.transferwise.tasks.triggering.SeekToDurationOnRebalanceListener;
import com.transferwise.tasks.utils.WaitUtils;
import java.time.Duration;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ThreadLocalRandom;
import java.util.function.Supplier;
import lombok.Setter;
import lombok.experimental.Accessors;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.apache.kafka.clients.consumer.CommitFailedException;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
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
  @Setter
  private String autoResetOffsetTo = "-PT1H";
  @Setter
  private long shard = 0;
  @Setter
  private Duration asyncCommitInterval = Duration.ofSeconds(5);
  @Setter
  private Duration asyncCommitIntervalJitter = Duration.ofMillis(100);

  private Map<TopicPartition, Long> nextAsyncCommitTimesMs = new ConcurrentHashMap<>();
  protected Consumer<String, T> consumer;
  private Map<TopicPartition, Long> committableOffsets = new ConcurrentHashMap<>();
  private Map<TopicPartition, Long> lastSuccessfullyCommittedOffsets = new ConcurrentHashMap<>();
  private AutoCloseable consumerMetricsHandle;

  protected void createKafkaConsumer() {
    Map<String, Object> kafkaConsumerProps = new HashMap<>(kafkaPropertiesSupplier.get());
    kafkaConsumerProps.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);

    if (StringUtils.equalsIgnoreCase(autoResetOffsetTo, "earliest") || StringUtils.equalsIgnoreCase(autoResetOffsetTo, "latest")) {
      kafkaConsumerProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, autoResetOffsetTo);
    }

    consumer = new KafkaConsumer<>(kafkaConsumerProps);
  }

  protected void registerKafkaConsumerMetrics() {
    if (kafkaListenerMetricsTemplate != null) {
      consumerMetricsHandle = kafkaListenerMetricsTemplate.registerKafkaConsumer(consumer, shard);
    }
  }

  protected void subscribe() {
    if (!StringUtils.equalsIgnoreCase(autoResetOffsetTo, "earliest") && !StringUtils.equalsIgnoreCase(autoResetOffsetTo, "latest")) {
      var autoResetOffsetToDuration = ExceptionUtils.doUnchecked(() -> Duration.parse(autoResetOffsetTo));
      consumer.subscribe(topics,
          new CommittingConsumerRebalanceListener(new SeekToDurationOnRebalanceListener(consumer, autoResetOffsetToDuration)));
    } else {
      consumer.subscribe(topics, new CommittingConsumerRebalanceListener(null));
    }
  }

  public void consume() {
    createKafkaConsumer();
    try {
      registerKafkaConsumerMetrics();

      subscribe();

      while (!shouldFinishPredicate.get()) {
        try {
          boolean shouldPoll = shouldPollPredicate == null || shouldPollPredicate.get();
          if (!shouldPoll) {
            // We expect `shouldPollPredicate` itself to avoid CPU burn from this continuous loop.
            // It is mostly used for tests.
            continue;
          }

          var consumerRecords = consumer.poll(delayTimeout);

          handlePolledKafkaMessages(consumerRecords);

          commitAsyncWithLowFrequency();
        } catch (Throwable t) {
          log.error(t.getMessage(), t);
          WaitUtils.sleepQuietly(delayTimeout);
        }
      }
    } finally {
      unsubscribe();
      close();
    }
  }

  @EntryPoint
  protected void handlePolledKafkaMessages(ConsumerRecords<String, T> consumerRecords) {
    unitOfWorkManager.createEntryPoint(EntryPointsGroups.TW_TASKS_ENGINE, "handlePolledKafkaMessages").toContext().execute(() -> {
      if (log.isDebugEnabled()) {
        log.debug("Polled " + consumerRecords.count() + " messages from Kafka.");
      }
      if (consumerRecords.isEmpty()) {
        if (log.isDebugEnabled()) {
          log.debug("No records found for: " + StringUtils.join(consumer.subscription(), ", "));
        }
        return;
      }

      for (var consumerRecord : consumerRecords) {
        if (log.isDebugEnabled()) {
          log.debug("Received Kafka message from topic '{}', partition {}, offset {}.", consumerRecord.topic(), consumerRecord.partition(),
              consumerRecord.offset());
        }

        while (!shouldFinishPredicate.get()) {
          try {
            recordConsumer.accept(consumerRecord);

            var topicPartition = new TopicPartition(consumerRecord.topic(), consumerRecord.partition());
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
    });
  }

  protected void registerCommitException(Throwable t) {
    if (t instanceof CommitFailedException || t instanceof RetriableException) { // Topic got rebalanced on shutdown.
      log.debug("Committing Kafka offset failed.", t);
      return;
    }
    if (errorLoggingThrottler == null || errorLoggingThrottler.canLogError()) {
      log.error("Committing Kafka offset failed.", t);
    }
  }

  protected void unsubscribe() {
    if (consumer == null) {
      return;
    }
    try {
      log.info("Unsubscribing from topics '" + StringUtils.join(consumer.subscription(), ",'") + "'.");
      consumer.unsubscribe();
    } catch (Throwable t) {
      log.error("Unsubscribing Kafka consumer failed.", t);
    }
  }

  protected void close() {
    if (consumerMetricsHandle != null) {
      try {
        consumerMetricsHandle.close();
      } catch (Throwable t) {
        log.error("Closing Kafka client metrics failed.", t);
      }
      consumerMetricsHandle = null;
    }

    try {
      consumer.close();
    } catch (Throwable t) {
      log.error("Closing Kafka consumer failed.", t);
    }

    consumer = null;

    lastSuccessfullyCommittedOffsets.clear();
    committableOffsets.clear();
    nextAsyncCommitTimesMs.clear();
  }

  protected void commitSyncOnPartitionRevoke(Collection<TopicPartition> partitions) {
    Map<TopicPartition, OffsetAndMetadata> offsets = null;
    for (var partition : partitions) {
      var offset = committableOffsets.remove(partition);
      if (offset != null) {
        var lastSuccessfullyCommittedOffset = lastSuccessfullyCommittedOffsets.remove(partition);
        if (lastSuccessfullyCommittedOffset == null || !lastSuccessfullyCommittedOffset.equals((offset))) {
          if (offsets == null) {
            offsets = new HashMap<>();
          }
          offsets.put(partition, new OffsetAndMetadata(offset));
        }
      }
    }

    if (offsets == null) {
      return;
    }

    var success = false;
    try {
      log.debug("Executing sync commit.");
      consumer.commitSync(offsets);
      success = true;
    } catch (Throwable t) {
      registerCommitException(t);
    } finally {
      if (kafkaListenerMetricsTemplate != null) {
        for (var entry : offsets.entrySet()) {
          var partition = entry.getKey();
          kafkaListenerMetricsTemplate.registerCommit(shard, partition.topic(), partition.partition(), true, success);
        }
      }
    }
  }

  protected void commitAsyncWithLowFrequency() {
    if (asyncCommitInterval == null) {
      return;
    }

    for (var entry : committableOffsets.entrySet()) {
      var partition = entry.getKey();
      var commitableOffset = committableOffsets.get(partition);

      var nextAsyncCommitTimeMs = nextAsyncCommitTimesMs.get(partition);

      if (nextAsyncCommitTimeMs != null && nextAsyncCommitTimeMs >= TwContextClockHolder.getClock().millis()) {
        continue;
      }

      var lastSuccessfullyCommittedOffset = lastSuccessfullyCommittedOffsets.get(partition);
      if (lastSuccessfullyCommittedOffset != null && lastSuccessfullyCommittedOffset.equals(commitableOffset)) {
        continue;
      }

      try {
        var offsetsToCommit = Collections.singletonMap(partition, new OffsetAndMetadata(commitableOffset));

        try {
          log.debug("Executing async commit.");

          consumer.commitAsync(offsetsToCommit, (offsets, e) -> {
            if (e == null) {
              log.debug("Successfully async-committed offset {} for partition '{}'.", commitableOffset, partition);

              lastSuccessfullyCommittedOffsets.put(partition, commitableOffset);
            } else {
              registerCommitException(e);
            }

            if (kafkaListenerMetricsTemplate != null) {
              kafkaListenerMetricsTemplate.registerCommit(shard, partition.topic(), partition.partition(), false, e == null);
            }
          });
        } catch (Throwable t) {
          registerCommitException(t);
        }
      } finally {
        nextAsyncCommitTimesMs.put(partition, TwContextClockHolder.getClock().millis() + asyncCommitInterval.toMillis()
            + ThreadLocalRandom.current().nextLong(asyncCommitIntervalJitter.toMillis()));
      }
    }
  }

  protected class CommittingConsumerRebalanceListener implements ConsumerRebalanceListener {

    private ConsumerRebalanceListener delegate;

    protected CommittingConsumerRebalanceListener(ConsumerRebalanceListener delegate) {
      this.delegate = delegate;
    }

    @Override
    public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
      log.info("Partitions revoked for shard {}: '{}'.", shard, partitions);

      commitSyncOnPartitionRevoke(partitions);

      if (delegate != null) {
        delegate.onPartitionsRevoked(partitions);
      }
    }

    @Override
    public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
      log.info("Partitions assigned for shard {}: '{}'.", shard, partitions);

      for (var partition : partitions) {
        lastSuccessfullyCommittedOffsets.remove(partition);
        committableOffsets.remove(partition);
      }

      if (delegate != null) {
        delegate.onPartitionsAssigned(partitions);
      }
    }
  }
}
