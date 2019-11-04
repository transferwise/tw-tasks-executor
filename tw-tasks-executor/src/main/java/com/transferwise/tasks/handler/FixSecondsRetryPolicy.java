package com.transferwise.tasks.handler;

import com.transferwise.common.baseutils.clock.ClockHolder;
import com.transferwise.tasks.domain.ITask;
import com.transferwise.tasks.handler.interfaces.ITaskRetryPolicy;

import java.time.ZonedDateTime;
import java.time.temporal.ChronoUnit;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

/**
 * Allows fine-tuning retry policies by explicitly defining the delays between retries.
 */
public class FixSecondsRetryPolicy implements ITaskRetryPolicy {

    private final List<Integer> seconds;

    /**
     * @param seconds list of delays between retries in seconds.
     */
    public FixSecondsRetryPolicy(int... seconds) {
        this.seconds = Arrays.stream(seconds)
            .peek(x -> {
                if (x <= 0) {
                    throw new IllegalArgumentException("Delay in seconds for retrying must be positive!");
                }
            })
            .boxed()
            .collect(Collectors.toList());
    }

    @Override
    public ZonedDateTime getRetryTime(ITask taskForProcessing, Throwable t) {
        final long triesCount = taskForProcessing.getProcessingTriesCount();

        if (triesCount >= seconds.size()) {
            return null;
        }

        long addedTimeSeconds = 0;
        for (int i = 0; i <= triesCount; i++) {
            addedTimeSeconds += seconds.get(i);
        }

        return now().plus(addedTimeSeconds, ChronoUnit.SECONDS);
    }

    public ZonedDateTime now() {
        return ZonedDateTime.now(ClockHolder.getClock());
    }
}
