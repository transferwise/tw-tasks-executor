package com.transferwise.tasks.helpers.kafka.meters;

import static com.transferwise.tasks.helpers.ICoreMetricsTemplate.METRIC_PREFIX;

import com.transferwise.common.baseutils.meters.cache.IMeterCache;
import com.transferwise.common.baseutils.meters.cache.TagsSet;
import io.micrometer.core.instrument.ImmutableTag;
import io.micrometer.core.instrument.binder.kafka.KafkaClientMetrics;
import java.util.List;
import org.apache.kafka.clients.consumer.Consumer;
import org.springframework.beans.factory.annotation.Autowired;

public class KafkaListenerMetricsTemplate implements IKafkaListenerMetricsTemplate {

  private static final String METRIC_CORE_KAFKA_PROCESSED_MESSAGES_COUNT = METRIC_PREFIX + "coreKafka.processedMessagesCount";
  private static final String METRIC_CONSISTENT_KAFKA_CONSUMER_FAILED_COMMITS_COUNT = METRIC_PREFIX + "consistentKafkaConsumer.failedCommitsCount";

  private static final String TAG_TOPIC = "topic";
  private static final String TAG_SHARD = "shard";
  private static long kafkaConsumerId = 0;

  @Autowired
  private IMeterCache meterCache;

  @Override
  public void registerKafkaCoreMessageProcessing(int shard, String topic) {
    meterCache.counter(METRIC_CORE_KAFKA_PROCESSED_MESSAGES_COUNT, TagsSet.of(TAG_SHARD, String.valueOf(shard), TAG_TOPIC, topic))
        .increment();
  }

  @Override
  public void registerFailedCommit() {
    meterCache.counter(METRIC_CONSISTENT_KAFKA_CONSUMER_FAILED_COMMITS_COUNT, TagsSet.empty())
        .increment();
  }

  @Override
  @SuppressWarnings("rawtypes")
  public synchronized void registerKafkaConsumer(Consumer consumer, long shard) {
    // Spring application are setting the tag `spring.id`, so we need to set it as well.

    // Add specific client id for consumer.

    new KafkaClientMetrics(consumer, List.of(new ImmutableTag("spring.id", "tw-tasks-kafka-listener-" + shard + "-" + (kafkaConsumerId++))))
        .bindTo(meterCache.getMeterRegistry());
  }
}
