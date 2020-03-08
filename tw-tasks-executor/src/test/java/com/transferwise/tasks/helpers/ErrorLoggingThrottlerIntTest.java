package com.transferwise.tasks.helpers;

import com.transferwise.tasks.BaseIntTest;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;

import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.api.Assertions.assertEquals;

class ErrorLoggingThrottlerIntTest extends BaseIntTest {

  @Autowired
  private ErrorLoggingThrottler errorLoggingThrottler;

  @Test
  void throttlingWorks() {
    await().until(() -> errorLoggingThrottler.canLogError());

    int cnt = 0;
    for (int i = 0; i < 10; i++) {
      cnt = errorLoggingThrottler.canLogError() ? 1 : 0;
    }

    assertEquals(0, cnt);
  }
}
