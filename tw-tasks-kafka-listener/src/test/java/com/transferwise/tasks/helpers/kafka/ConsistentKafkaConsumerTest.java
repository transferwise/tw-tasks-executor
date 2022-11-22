package com.transferwise.tasks.helpers.kafka;

import static org.apache.kafka.common.requests.DeleteAclsResponse.log;
import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.transferwise.common.baseutils.clock.TestClock;
import com.transferwise.common.baseutils.meters.cache.MeterCache;
import com.transferwise.common.context.DefaultUnitOfWorkManager;
import com.transferwise.common.context.TwContextClockHolder;
import com.transferwise.tasks.helpers.kafka.meters.KafkaListenerMetricsTemplate;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import java.time.Duration;
import java.time.Instant;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import lombok.SneakyThrows;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.MockConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.Test;

public class ConsistentKafkaConsumerTest {

  private MockConsumer<String, String> mockConsumer;

  @Test
  @SneakyThrows
  void testCommitLogic() {
    var testClock = new TestClock(Instant.now());
    TwContextClockHolder.setClock(testClock);

    final var meterRegistry = new SimpleMeterRegistry();
    final var meterCache = new MeterCache(meterRegistry);
    final var unitOfWorkManager = new DefaultUnitOfWorkManager(meterCache);
    final var shouldFinish = new AtomicBoolean();
    final var kafkaListenerMetricsTemplate = new KafkaListenerMetricsTemplate(meterCache);

    final var testTopic = "TestTopic";
    final var partition0 = new TopicPartition(testTopic, 0);
    final var processedRecordsCount = new AtomicInteger();

    // MockConsumer does not seem to support rebalance listeners.
    // So we can not test sync commit metrics here.

    mockConsumer = new MockConsumer<>(OffsetResetStrategy.EARLIEST) {
      // We are creating some delay, to reduce the spam of logs form the test.
      @Override
      public synchronized ConsumerRecords<String, String> poll(final Duration timeout) {
        long startTimeMs = System.currentTimeMillis();
        while (true) {
          var records = super.poll(timeout);
          if (!records.isEmpty()) {
            return records;
          }
          if (System.currentTimeMillis() - startTimeMs > timeout.toMillis()) {
            return records;
          }
        }
      }
    };

    mockConsumer.updatePartitions(testTopic, List.of(new PartitionInfo(testTopic, 1, null, null, null)));

    mockConsumer.schedulePollTask(() -> mockConsumer.rebalance(Collections.singletonList(new TopicPartition(testTopic, 0))));
    mockConsumer.schedulePollTask(() -> mockConsumer.addRecord(new ConsumerRecord<>(testTopic, 0, 0, "testKey", "testValue0")));

    HashMap<TopicPartition, Long> startOffsets = new HashMap<>();
    TopicPartition tp = new TopicPartition(testTopic, 0);
    startOffsets.put(tp, 0L);
    mockConsumer.updateBeginningOffsets(startOffsets);

    var consistentConsumer = new MockedConsistentKafkaConsumer()
        .setTopics(Collections.singletonList(testTopic))
        .setUnitOfWorkManager(unitOfWorkManager)
        .setShouldFinishPredicate(() -> shouldFinish.get())
        .setAutoResetOffsetTo("earliest")
        .setAsyncCommitInterval(Duration.ofSeconds(1))
        .setDelayTimeout(Duration.ofMillis(100))
        .setKafkaListenerMetricsTemplate(kafkaListenerMetricsTemplate)
        .setRecordConsumer(consumerRecord -> {
          log.info("Received message '{}'.", consumerRecord.value());
          processedRecordsCount.incrementAndGet();
        });

    Thread consumerThread = new Thread(consistentConsumer::consume);
    consumerThread.start();

    await().until(() -> {
          var offsetAndMetaData = mockConsumer.committed(Set.of(partition0)).get(partition0);
          return offsetAndMetaData != null && offsetAndMetaData.offset() == 1;
        }
    );

    mockConsumer.schedulePollTask(() -> {
      testClock.tick(Duration.ofSeconds(5));
      mockConsumer.addRecord(new ConsumerRecord<>(testTopic, 0, 1, "testKey", "testValue1"));
    });

    await().until(() -> {
          var offsetAndMetaData = mockConsumer.committed(Set.of(partition0)).get(partition0);
          return offsetAndMetaData != null && offsetAndMetaData.offset() == 2;
        }
    );

    mockConsumer.commitSync(Map.of(partition0, new OffsetAndMetadata(1)));
    assertEquals(1, mockConsumer.committed(Set.of(partition0)).get(partition0).offset());

    testClock.tick(Duration.ofSeconds(5));
    consistentConsumer.commitAsyncWithLowFrequency();

    // If lastCommittedOffset did not change, we will not commit
    assertEquals(1, mockConsumer.committed(Set.of(partition0)).get(partition0).offset());

    mockConsumer.schedulePollTask(() -> mockConsumer.addRecord(new ConsumerRecord<>(testTopic, 0, 2, "testKey", "testValue2")));

    await().until(() -> {
          var offsetAndMetaData = mockConsumer.committed(Set.of(partition0)).get(partition0);
          return offsetAndMetaData != null && offsetAndMetaData.offset() == 3;
        }
    );

    mockConsumer.schedulePollTask(() -> mockConsumer.addRecord(new ConsumerRecord<>(testTopic, 0, 3, "testKey", "testValue3")));
    await().until(() -> processedRecordsCount.get() == 4);

    shouldFinish.set(true);
    consumerThread.join();

    var asyncCommitsCounter = meterRegistry.find("twTasks.consistentKafkaConsumer.commitsCount")
        .tags("shard", "0", "topic", testTopic, "partition", "0", "sync", "false", "success", "true").counter();

    var asyncCommitsCount = asyncCommitsCounter == null ? 0 : asyncCommitsCounter.count();
    assertTrue(asyncCommitsCount >= 3);
  }

  private class MockedConsistentKafkaConsumer extends ConsistentKafkaConsumer<String> {

    protected void createKafkaConsumer() {
      consumer = mockConsumer;
    }
  }

}
