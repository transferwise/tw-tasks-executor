package com.transferwise.tasks.utils;

import java.time.Duration;

public abstract class WaitUtils {

  public static void sleepQuietly(Duration timeout) {
    try {
      Thread.sleep(timeout.toMillis());
    } catch (InterruptedException ignored) {
      //ignored
    }
  }
}
