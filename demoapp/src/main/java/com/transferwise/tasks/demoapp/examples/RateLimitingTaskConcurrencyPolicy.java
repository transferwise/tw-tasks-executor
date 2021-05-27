package com.transferwise.tasks.demoapp.examples;

import com.google.common.util.concurrent.RateLimiter;
import com.transferwise.tasks.domain.IBaseTask;
import com.transferwise.tasks.handler.SimpleTaskConcurrencyPolicy;
import com.transferwise.tasks.handler.interfaces.ITaskConcurrencyPolicy;
import java.time.Instant;
import lombok.NonNull;

public class RateLimitingTaskConcurrencyPolicy implements ITaskConcurrencyPolicy {

  // Up to 5 tasks in every second
  private RateLimiter rateLimiter = RateLimiter.create(5);

  // But not more than 3 in parallel
  private ITaskConcurrencyPolicy concurrencyLimitingPolicy = new SimpleTaskConcurrencyPolicy(3);

  @Override
  public @NonNull BookSpaceResponse bookSpace(IBaseTask task) {
    BookSpaceResponse concurrencyLimitingResponse = concurrencyLimitingPolicy.bookSpace(task);
    if (!concurrencyLimitingResponse.isHasRoom()) {
      return concurrencyLimitingResponse;
    }

    if (rateLimiter.tryAcquire()) {
      return concurrencyLimitingResponse;
    }
    concurrencyLimitingPolicy.freeSpace(task);

    // Most optimal would be to set `tryAgainTime` when a new permit becomes available.
    // Unfortunately Guava's implementation does not expose it's inner method for it, so we just try again after 100 millis.
    // 100 millis worst case latency here and then is not a problem for us.
    // There probably is a better rate limiting library.
    return new BookSpaceResponse(false).setTryAgainTime(Instant.now().plusMillis(100));
  }

  @Override
  public void freeSpace(IBaseTask task) {
    concurrencyLimitingPolicy.freeSpace(task);
  }
}
