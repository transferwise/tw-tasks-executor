package com.transferwise.tasks.triggering;

import com.transferwise.common.context.TwContextClockHolder;
import java.time.Duration;
import java.time.Instant;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndTimestamp;
import org.apache.kafka.common.TopicPartition;

/**
 * For any partition that doesn't have offset committed for a given consumer - seeks to the earliest offset that is greater than {@code now() +
 * autoResetOffsetToDuration}. Seeks to the beginning in case the timestamp of the very first record in partition has value greater than {@code now()
 * + autoResetOffsetToDuration}.
 *
 * <p>Note that {@code autoResetOffsetToDuration} can be set to positive or negative. Both of them always gets converted to a positive value.
 */
@Slf4j
@RequiredArgsConstructor
public class SeekToDurationOnRebalanceListener implements ConsumerRebalanceListener {

  private final Consumer<?, ?> consumer;
  private final Duration autoResetOffsetToDuration;

  @Override
  public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
  }

  @Override
  public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
    Map<TopicPartition, Long> timestampsToSearch = new HashMap<>();
    long timestampToSearchMs = ZonedDateTime.now(TwContextClockHolder.getClock()).minus(autoResetOffsetToDuration.abs()).toInstant().toEpochMilli();

    for (TopicPartition partition : partitions) {
      try {
        // Default kafka API timeout is 60s. Should be fine.
        if (consumer.committed(Collections.singleton(partition)).get(partition) == null) {
          timestampsToSearch.put(partition, timestampToSearchMs);
        }
      } catch (Throwable t) {
        timestampsToSearch.put(partition, timestampToSearchMs);
      }
    }

    if (!timestampsToSearch.isEmpty()) {
      List<TopicPartition> seekToBeginningPartitions = new ArrayList<>();

      Map<TopicPartition, OffsetAndTimestamp> offsets = consumer.offsetsForTimes(timestampsToSearch);
      offsets.forEach((k, v) -> {
        if (v != null) {
          log.warn("No offset was committed for '" + k + "', seeking to offset " + v.offset() + ", @" + Instant.ofEpochMilli(v.timestamp()));
          consumer.seek(k, v.offset());
        } else {
          log.warn("No offset was committed for '" + k + "', seeking to beginning");
          seekToBeginningPartitions.add(k);
        }
      });

      if (!seekToBeginningPartitions.isEmpty()) {
        consumer.seekToBeginning(seekToBeginningPartitions);
      }
    }
  }
}
