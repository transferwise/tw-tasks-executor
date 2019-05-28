package com.transferwise.tasks.utils;

import lombok.experimental.UtilityClass;

import java.time.Duration;

@UtilityClass
public class WaitUtils {
    public static void sleepQuietly(Duration timeout) {
        try {
            Thread.sleep(timeout.toMillis());
        } catch (InterruptedException ignored) {
        }
    }
}
