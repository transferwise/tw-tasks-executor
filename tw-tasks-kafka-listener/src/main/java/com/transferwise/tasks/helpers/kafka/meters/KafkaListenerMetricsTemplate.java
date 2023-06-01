package com.transferwise.tasks.helpers.kafka.meters;

import static com.transferwise.tasks.helpers.ICoreMetricsTemplate.METRIC_PREFIX;

import com.transferwise.common.baseutils.meters.cache.IMeterCache;
import com.transferwise.common.baseutils.meters.cache.TagsSet;
import io.micrometer.core.instrument.ImmutableTag;
import io.micrometer.core.instrument.binder.kafka.KafkaClientMetrics;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.kafka.clients.consumer.Consumer;

public class KafkaListenerMetricsTemplate implements IKafkaListenerMetricsTemplate {

  private static final String METRIC_CORE_KAFKA_PROCESSED_MESSAGES_COUNT = METRIC_PREFIX + "coreKafka.processedMessagesCount";
  private static final String METRIC_CONSISTENT_KAFKA_CONSUMER_COMMITS_COUNT = METRIC_PREFIX + "consistentKafkaConsumer.commitsCount";

  private static final String TAG_TOPIC = "topic";
  private static final String TAG_PARTITION = "partition";
  private static final String TAG_SUCCESS = "success";
  private static final String TAG_SYNC = "sync";
  private static final String TAG_SHARD = "shard";
  private static final AtomicInteger kafkaConsumerId = new AtomicInteger();

  private final IMeterCache meterCache;

  public KafkaListenerMetricsTemplate(IMeterCache meterCache) {
    this.meterCache = meterCache;
  }

  @Override
  public void registerKafkaCoreMessageProcessing(int shard, String topic) {
    meterCache.counter(METRIC_CORE_KAFKA_PROCESSED_MESSAGES_COUNT, TagsSet.of(TAG_SHARD, String.valueOf(shard), TAG_TOPIC, topic))
        .increment();
  }

  @Override
  public void registerCommit(long shard, String topic, int partition, boolean sync, boolean success) {
    meterCache.counter(METRIC_CONSISTENT_KAFKA_CONSUMER_COMMITS_COUNT, TagsSet.of(TAG_SHARD, String.valueOf(shard), TAG_TOPIC, topic,
            TAG_PARTITION, String.valueOf(partition), TAG_SUCCESS, String.valueOf(success), TAG_SYNC, String.valueOf(sync)))
        .increment();
  }

  @Override
  @SuppressWarnings("rawtypes")
  public AutoCloseable registerKafkaConsumer(Consumer consumer, long shard) {
    // Spring application are setting the tag `spring.id`, so we need to set it as well.

    // Add specific client id for consumer.

    var kafkaClientMetrics = new KafkaClientMetrics(consumer,
        List.of(new ImmutableTag("spring.id", "tw-tasks-kafka-listener-" + shard + "-" + (kafkaConsumerId.getAndIncrement()))));
    kafkaClientMetrics.bindTo(meterCache.getMeterRegistry());

    return kafkaClientMetrics;
  }
}
