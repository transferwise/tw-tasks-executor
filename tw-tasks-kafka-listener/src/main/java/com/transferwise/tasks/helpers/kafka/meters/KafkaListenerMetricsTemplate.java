package com.transferwise.tasks.helpers.kafka.meters;

import static com.transferwise.tasks.helpers.ICoreMetricsTemplate.METRIC_PREFIX;

import com.transferwise.common.baseutils.meters.cache.IMeterCache;
import com.transferwise.common.baseutils.meters.cache.TagsSet;
import org.springframework.beans.factory.annotation.Autowired;

public class KafkaListenerMetricsTemplate implements IKafkaListenerMetricsTemplate {

  private static final String METRIC_CORE_KAFKA_PROCESSED_MESSAGES_COUNT = METRIC_PREFIX + "coreKafka.processedMessagesCount";
  private static final String METRIC_CONSISTENT_KAFKA_CONSUMER_FAILED_COMMITS_COUNT = METRIC_PREFIX + "consistentKafkaConsumer.failedCommitsCount";

  private static final String TAG_TOPIC = "topic";
  private static final String TAG_SHARD = "shard";

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
}
