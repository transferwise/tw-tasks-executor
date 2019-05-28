package com.transferwise.tasks.handler;

import com.transferwise.common.baseutils.clock.ClockHolder;
import com.transferwise.tasks.buckets.IBucketsManager;
import com.transferwise.tasks.domain.IBaseTask;
import com.transferwise.tasks.handler.interfaces.ITaskProcessingPolicy;
import lombok.Getter;
import lombok.Setter;
import lombok.experimental.Accessors;

import java.time.Duration;
import java.time.ZonedDateTime;

public class SimpleTaskProcessingPolicy implements ITaskProcessingPolicy {
    @Getter
    @Setter
    @Accessors(chain = true)
    private Duration maxProcessingDuration = Duration.ofHours(1);

    /**
     * The bucket to use when processing tasks configured with this policy.
     * <p>
     * When using a value other than {@link com.transferwise.tasks.buckets.IBucketsManager#DEFAULT_ID}, ensure that it
     * is also supplied to {@link com.transferwise.tasks.TasksProperties#additionalProcessingBuckets}, otherwise tasks
     * using this policy will fail to be processed.
     */
    @Getter
    @Setter
    @Accessors(chain = true)
    private String processingBucket = IBucketsManager.DEFAULT_ID;

    @Getter
    @Setter
    @Accessors(chain = true)
    private StuckTaskResolutionStrategy stuckTaskResolutionStrategy = StuckTaskResolutionStrategy.RETRY;

    @Override
    public ZonedDateTime getMaxProcessingEndTime(IBaseTask task) {
        return ZonedDateTime.now(ClockHolder.getClock()).plus(maxProcessingDuration);
    }

    @Override
    public String getProcessingBucket(IBaseTask task) {
        return processingBucket;
    }

    @Override
    public StuckTaskResolutionStrategy getStuckTaskResolutionStrategy(IBaseTask task) {
        return stuckTaskResolutionStrategy;
    }
}
