package com.transferwise.tasks.helpers.kafka.partitionkey;

import com.transferwise.tasks.domain.BaseTask;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.junit.jupiter.api.Assertions;
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
        .setVersion(1L);

    final var firstTryResults = IntStream.range(0, 10)
        .mapToObj(i -> subject.createPartitionKey(baseTask))
        .collect(Collectors.toSet());

    final var secondTryResults = IntStream.range(0, 10)
        .mapToObj(i -> subject.createPartitionKey(baseTask))
        .collect(Collectors.toSet());

    Assertions.assertTrue(firstTryResults.size() == 10 || secondTryResults.size() == 10);
  }
}