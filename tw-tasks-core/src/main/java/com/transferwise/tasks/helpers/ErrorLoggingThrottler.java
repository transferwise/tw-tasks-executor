package com.transferwise.tasks.helpers;

import com.google.common.util.concurrent.RateLimiter;
import org.springframework.beans.factory.InitializingBean;

public class ErrorLoggingThrottler implements IErrorLoggingThrottler, InitializingBean {

  private RateLimiter rateLimiter;

  @Override
  public void afterPropertiesSet() {
    rateLimiter = RateLimiter.create(0.2);
  }

  @Override
  public boolean canLogError() {
    return rateLimiter.tryAcquire(1);
  }
}
