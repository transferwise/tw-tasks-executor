package com.transferwise.tasks.handler;

import com.transferwise.common.context.Criticality;
import com.transferwise.common.context.TwContextClockHolder;
import com.transferwise.tasks.buckets.IBucketsManager;
import com.transferwise.tasks.domain.IBaseTask;
import com.transferwise.tasks.handler.interfaces.ITaskProcessingPolicy;
import java.time.Duration;
import java.time.Instant;
import lombok.Getter;
import lombok.NonNull;
import lombok.Setter;
import lombok.experimental.Accessors;

public class SimpleTaskProcessingPolicy implements ITaskProcessingPolicy {

  @Getter
  @Setter
  @Accessors(chain = true)
  private Duration maxProcessingDuration = Duration.ofHours(1);

  /**
   * The bucket to use when processing tasks configured with this policy.
   *
   * <p>When using a value other than {@link com.transferwise.tasks.buckets.IBucketsManager#DEFAULT_ID}, ensure that it is also supplied to
   * {@link com.transferwise.tasks.TasksProperties#additionalProcessingBuckets}, otherwise tasks using this policy will fail to be processed.
   */
  @SuppressWarnings("JavadocReference")
  @Getter
  @Setter
  @Accessors(chain = true)
  private String processingBucket = IBucketsManager.DEFAULT_ID;

  @Getter
  @Setter
  @Accessors(chain = true)
  private StuckTaskResolutionStrategy stuckTaskResolutionStrategy = StuckTaskResolutionStrategy.MARK_AS_ERROR;

  @Override
  public String getProcessingBucket(IBaseTask task) {
    return processingBucket;
  }

  @Override
  public StuckTaskResolutionStrategy getStuckTaskResolutionStrategy(IBaseTask task, StuckDetector stuckDetector) {
    return stuckTaskResolutionStrategy;
  }

  @Override
  public @NonNull Instant getProcessingDeadline(IBaseTask task) {
    return TwContextClockHolder.getClock().instant().plus(maxProcessingDuration);
  }

  @Getter
  @Setter
  @Accessors(chain = true)
  private Criticality criticality;

  @Override
  public Criticality getProcessingCriticality(IBaseTask task) {
    return criticality;
  }

  @Getter
  @Setter
  @Accessors(chain = true)
  private String owner;

  @Override
  public String getOwner(IBaseTask task) {
    return owner;
  }
}
