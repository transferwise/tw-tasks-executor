package com.transferwise.tasks.impl.tokafka;

import lombok.Data;
import lombok.experimental.Accessors;
import org.springframework.boot.context.properties.ConfigurationProperties;

@Data
@Accessors(chain = true)
/*
  Defaults give 20 retries over 52 hours.
 */
@ConfigurationProperties(prefix = "tw-tasks.impl.to-kafka", ignoreUnknownFields = false)
@SuppressWarnings("checkstyle:magicnumber")
public class ToKafkaProperties {
    private int maxConcurrency = 5;
    private int maxProcessingDurationMs = 1000 * 60 * 30;
    private int retryDelayMs = 5 * 1000;
    private int retryExponent = 2;
    private int retryMaxCount = 20;
    private int retryMaxDelayMs = 1000 * 60;
    private int batchSizeMb = 16 / 2; // MySQL client's default value / 2
}
