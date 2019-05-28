package com.transferwise.tasks.helpers;

import com.google.common.util.concurrent.RateLimiter;

import javax.annotation.PostConstruct;

public class ErrorLoggingThrottler implements IErrorLoggingThrottler {
    private RateLimiter rateLimiter;

    @PostConstruct
    @SuppressWarnings("checkstyle:magicnumber")
    public void init() {
        rateLimiter = RateLimiter.create(0.2);
    }

    @Override
    public boolean canLogError() {
        return rateLimiter.tryAcquire(1);
    }
}
