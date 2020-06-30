package com.transferwise.tasks.handler;

import com.google.common.math.LongMath;
import com.transferwise.common.context.TwContextClockHolder;
import com.transferwise.tasks.domain.ITask;
import com.transferwise.tasks.handler.interfaces.ITaskRetryPolicy;
import java.time.Duration;
import java.time.ZonedDateTime;
import java.time.temporal.ChronoUnit;
import lombok.Getter;
import lombok.NonNull;
import lombok.Setter;
import lombok.experimental.Accessors;

@Getter
@Setter
@Accessors(chain = true)
public class ExponentialTaskRetryPolicy implements ITaskRetryPolicy {

  @NonNull
  private Duration delay = Duration.ofMillis(0);
  private long multiplier = 2;
  /**
   * Maximum delay to add on each retry (not the total delay since task creation).
   */
  @NonNull
  private Duration maxDelay = Duration.ofDays(1);
  private long maxCount = 1;
  private long triesDelta = 0;

  @Override
  public ZonedDateTime getRetryTime(ITask taskForProcessing, Throwable t) {
    long triesCount = taskForProcessing.getProcessingTriesCount() + triesDelta;
    if (triesCount >= maxCount) {
      return null;
    }
    long addedTimeMs;

    try {
      if (triesCount > Integer.MAX_VALUE) {
        addedTimeMs = maxDelay.toMillis();
      } else {
        addedTimeMs = LongMath.checkedMultiply(delay.toMillis(), LongMath.checkedPow(multiplier, (int) triesCount - 1));
        if (addedTimeMs > maxDelay.toMillis()) {
          addedTimeMs = maxDelay.toMillis();
        }
      }
    } catch (ArithmeticException ignored) {
      addedTimeMs = maxDelay.toMillis();
    }

    return ZonedDateTime.now(TwContextClockHolder.getClock()).plus(addedTimeMs, ChronoUnit.MILLIS);
  }
}
