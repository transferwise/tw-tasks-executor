package com.transferwise.tasks.helpers;

/**
 * Some errors can be very spammy on infrastructure failure.
 * However they can also occur on misconfigurations and application owner would want to see, that something is wrong.
 * The compromise would be to log some errors down, but have them throttled.
 */
public interface IErrorLoggingThrottler {
    boolean canLogError();
}
