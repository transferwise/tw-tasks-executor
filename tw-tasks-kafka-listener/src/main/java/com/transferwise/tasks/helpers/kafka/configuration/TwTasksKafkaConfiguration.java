package com.transferwise.tasks.helpers.kafka.configuration;

import lombok.Getter;
import lombok.RequiredArgsConstructor;
import org.springframework.boot.autoconfigure.kafka.KafkaProperties;
import org.springframework.kafka.core.KafkaTemplate;

@Getter
@RequiredArgsConstructor
public class TwTasksKafkaConfiguration {

  private final KafkaProperties kafkaProperties;
  private final KafkaTemplate<String, String> kafkaTemplate;
}
