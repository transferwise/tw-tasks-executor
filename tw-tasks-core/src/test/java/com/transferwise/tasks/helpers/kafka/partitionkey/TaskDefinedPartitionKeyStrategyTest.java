package com.transferwise.tasks.helpers.kafka.partitionkey;

import static org.assertj.core.api.Assertions.assertThat;

import com.transferwise.tasks.domain.BaseTask;
import java.util.UUID;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class TaskDefinedPartitionKeyStrategyTest {

  private TaskDefinedPartitionKeyStrategy subject;

  @BeforeEach
  void setUp() {
    subject = new TaskDefinedPartitionKeyStrategy();
  }

  @Test
  void shouldReturnTheKeyDefinedInTheTask() {
    final var baseTask = new BaseTask()
        .setId(UUID.randomUUID())
        .setType("type")
        .setPriority(1)
        .setKey(UUID.randomUUID().toString())
        .setVersion(1L);

    final var partitionKey = subject.getPartitionKey(baseTask);

    assertThat(partitionKey).isEqualTo(baseTask.getKey());
  }
}