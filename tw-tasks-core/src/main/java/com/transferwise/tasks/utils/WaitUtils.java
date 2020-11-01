package com.transferwise.tasks.utils;

import java.time.Duration;

public final class WaitUtils {

  private WaitUtils() {
    throw new AssertionError();
  }

  public static void sleepQuietly(Duration timeout) {
    try {
      Thread.sleep(timeout.toMillis());
    } catch (InterruptedException ignored) {
      //ignored
    }
  }
}
