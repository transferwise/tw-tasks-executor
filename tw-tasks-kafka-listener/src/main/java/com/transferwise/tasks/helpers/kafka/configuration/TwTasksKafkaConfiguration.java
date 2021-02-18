package com.transferwise.tasks.helpers.kafka.configuration;

import lombok.Getter;
import lombok.RequiredArgsConstructor;
import org.springframework.boot.autoconfigure.kafka.KafkaProperties;

@Getter
@RequiredArgsConstructor
public class TwTasksKafkaConfiguration {

  private final KafkaProperties kafkaProperties;
}
