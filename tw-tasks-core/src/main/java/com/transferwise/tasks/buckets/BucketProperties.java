package com.transferwise.tasks.buckets;

import com.transferwise.common.baseutils.ExceptionUtils;
import java.time.Duration;
import lombok.Data;
import lombok.experimental.Accessors;
import org.apache.commons.lang3.StringUtils;

/**
 * Shard specific override configuration.
 *
 * <p>Can be registered via {@link IBucketsManager}
 *
 * <p>Check {@link com.transferwise.tasks.TasksProperties} for description of these properties.
 */
@Data
@Accessors(chain = true)
public class BucketProperties {

  private Integer maxTriggersInMemory;
  private Integer triggeringTopicPartitionsCount;
  private Boolean triggerSameTaskInAllNodes;
  private String autoResetOffsetTo;
  private Integer triggersFetchSize;
  private Boolean triggerInSameProcess;
  private Integer taskGrabbingMaxConcurrency;
  private Boolean autoStartProcessing;
  private Duration autoResetOffsetToDuration;

  public BucketProperties setAutoResetOffsetTo(String autoResetOffsetTo) {
    if (!StringUtils.equalsIgnoreCase(autoResetOffsetTo, "earliest") && !StringUtils.equalsIgnoreCase(autoResetOffsetTo, "latest")) {
      ExceptionUtils.doUnchecked(() -> {
        autoResetOffsetToDuration = Duration.parse(autoResetOffsetTo);
      });
    }
    this.autoResetOffsetTo = autoResetOffsetTo;
    return this;
  }
}
