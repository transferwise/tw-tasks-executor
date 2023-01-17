package com.transferwise.tasks.handler;

import static org.assertj.core.api.Assertions.assertThat;

import com.transferwise.tasks.domain.BaseTask;
import com.transferwise.tasks.helpers.kafka.partitionkey.IPartitionKeyStrategy;
import org.junit.jupiter.api.Test;

class SimpleTaskProcessingPolicyTest {

  @Test
  void shouldUseDefinedPartitionKeyStrategyWhenDifferentStrategyIsProvided() {
    final var overridenPartitionKeyStrategy = new IPartitionKeyStrategy() {
      @Override
      public String createPartitionKey(BaseTask task) {
        return "Key";
      }
    };

    final var simpleTaskProcessingPolicy = new SimpleTaskProcessingPolicy()
        .setPartitionKeyStrategy(overridenPartitionKeyStrategy);

    assertThat(simpleTaskProcessingPolicy.getPartitionKeyStrategy()).isEqualTo(overridenPartitionKeyStrategy);
  }
}