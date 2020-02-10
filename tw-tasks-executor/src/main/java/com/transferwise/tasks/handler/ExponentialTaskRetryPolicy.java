package com.transferwise.tasks.handler;

import com.transferwise.common.baseutils.clock.ClockHolder;
import com.transferwise.tasks.domain.ITask;
import com.transferwise.tasks.handler.interfaces.ITaskRetryPolicy;
import java.time.Duration;
import java.time.ZonedDateTime;
import java.time.temporal.ChronoUnit;
import lombok.Getter;
import lombok.Setter;
import lombok.experimental.Accessors;

@Getter
@Setter
@Accessors(chain = true)
public class ExponentialTaskRetryPolicy implements ITaskRetryPolicy {

  private Duration delay = Duration.ofMillis(0);
  private double multiplier = 2;
  /**
   * Maximum delay to add on each retry (not the total delay since task creation).
   */
  private Duration maxDelay;
  private int maxCount = 1;
  private int triesDelta = 0;

  @Override
  public ZonedDateTime getRetryTime(ITask taskForProcessing, Throwable t) {
    long triesCount = taskForProcessing.getProcessingTriesCount() + triesDelta;
    if (triesCount >= maxCount) {
      return null;
    }
    long addedTimeMs = delay.toMillis() * (long) Math.pow(multiplier, (triesCount - 1));
    if (maxDelay != null && addedTimeMs > maxDelay.toMillis()) {
      addedTimeMs = maxDelay.toMillis();
    }
    return ZonedDateTime.now(ClockHolder.getClock()).plus(addedTimeMs, ChronoUnit.MILLIS);
  }
}
