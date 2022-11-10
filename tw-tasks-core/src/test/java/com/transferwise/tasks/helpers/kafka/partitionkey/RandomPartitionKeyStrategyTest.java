package com.transferwise.tasks.helpers.kafka.partitionkey;

import static org.assertj.core.api.Assertions.assertThat;

import com.transferwise.tasks.domain.BaseTask;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class RandomPartitionKeyStrategyTest {

  private RandomPartitionKeyStrategy subject;

  @BeforeEach
  void setUp() {
    subject = new RandomPartitionKeyStrategy();
  }

  @Test
  void shouldAlwaysReturnADifferentKey() {
    final var id = UUID.randomUUID();
    final var baseTask = new BaseTask()
        .setId(id)
        .setType("type")
        .setPriority(1)
        .setPartitionKey("key")
        .setVersion(1L);

    final var keys = IntStream.range(0, 10)
        .mapToObj(i -> subject.getPartitionKey(baseTask))
        .collect(Collectors.toList());

    keys.forEach(k -> assertThat(keys).containsOnlyOnce(k));
  }
}