package com.transferwise.tasks.utils;

import java.time.Duration;
import lombok.experimental.UtilityClass;

@UtilityClass
public class WaitUtils {

  public static void sleepQuietly(Duration timeout) {
    try {
      Thread.sleep(timeout.toMillis());
    } catch (InterruptedException ignored) {
      //ignored
    }
  }
}
