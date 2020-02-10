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
  /**
   * The more buckets or cluster nodes you have, the lower you probably want to have this.
   *
   * <p>Having more threads will lower the individual node task processing latency in situations where processing slots are mostly full (heavy load).
   * But if you have more nodes or lower load, this starts mattering less and less.
   */
  private int taskGrabbingConcurrency = 10;

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
