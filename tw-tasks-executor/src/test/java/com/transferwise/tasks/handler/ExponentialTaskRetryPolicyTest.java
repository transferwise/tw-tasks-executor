package com.transferwise.tasks.handler;

import static org.assertj.core.api.Assertions.assertThat;

import com.transferwise.common.baseutils.clock.TestClock;
import com.transferwise.tasks.BaseTest;
import com.transferwise.tasks.domain.Task;
import java.time.Duration;
import java.time.Instant;
import org.junit.jupiter.api.Test;

public class ExponentialTaskRetryPolicyTest extends BaseTest {

  @Test
  public void arithmeticOverflowIsHandled() {
    ExponentialTaskRetryPolicy policy = new ExponentialTaskRetryPolicy()
        .setMaxCount(9999).setDelay(Duration.ofSeconds(5)).setMultiplier(2);
    Task task = new Task().setProcessingTriesCount(200);
    TestClock testClock = TestClock.createAndRegister();
    Instant retryTime = policy.getRetryTime(task, null).toInstant();
    assertThat(retryTime).isEqualTo(testClock.instant().plus(Duration.ofDays(1)));
  }

  @Test
  public void worksInGeneral() {
    ExponentialTaskRetryPolicy policy = new ExponentialTaskRetryPolicy()
        .setMaxCount(9999).setDelay(Duration.ofSeconds(5)).setMultiplier(2);
    Task task = new Task().setProcessingTriesCount(200);
    TestClock testClock = TestClock.createAndRegister();

    task.setProcessingTriesCount(1);
    Instant retryTime = policy.getRetryTime(task, null).toInstant();
    assertThat(retryTime).isEqualTo(testClock.instant().plus(Duration.ofSeconds(5)));

    task.setProcessingTriesCount(2);
    retryTime = policy.getRetryTime(task, null).toInstant();
    assertThat(retryTime).isEqualTo(testClock.instant().plus(Duration.ofSeconds(10)));
  }

  @Test
  public void triesDeltaWorks() {
    ExponentialTaskRetryPolicy policy = new ExponentialTaskRetryPolicy()
        .setMaxCount(9999).setDelay(Duration.ofSeconds(5)).setMultiplier(2).setTriesDelta(2);
    Task task = new Task().setProcessingTriesCount(200);
    TestClock testClock = TestClock.createAndRegister();

    task.setProcessingTriesCount(1);
    Instant retryTime = policy.getRetryTime(task, null).toInstant();
    assertThat(retryTime).isEqualTo(testClock.instant().plus(Duration.ofSeconds(20)));
  }
}
