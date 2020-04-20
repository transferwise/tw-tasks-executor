package com.transferwise.tasks.handler;

import com.transferwise.tasks.BaseTest;
import com.transferwise.tasks.domain.ITask;
import com.transferwise.tasks.domain.Task;
import java.time.Clock;
import java.time.Duration;
import java.time.ZonedDateTime;
import java.util.stream.Stream;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

class ExponentialTaskRetryPolicyTest extends BaseTest {

  private final ExponentialTaskRetryPolicy exponentialTaskRetryPolicy = new ExponentialTaskRetryPolicy();

  @ParameterizedTest
  @MethodSource("maxDelaySource")
  void largeRetryCountsDoNotCauseNegativeRetryDelays(Duration maxDelay) {
    exponentialTaskRetryPolicy.setDelay(Duration.ofSeconds(20));
    exponentialTaskRetryPolicy.setMaxCount(Integer.MAX_VALUE);
    exponentialTaskRetryPolicy.setMaxDelay(maxDelay);

    ITask task = new Task().setProcessingTriesCount(180);

    ZonedDateTime retryTime = exponentialTaskRetryPolicy.getRetryTime(task, null);

    Assertions.assertTrue(retryTime.isAfter(ZonedDateTime.now(Clock.systemUTC())));
  }

  private static Stream<Duration> maxDelaySource() {
    return Stream.of(null, Duration.ofHours(1));
  }
}