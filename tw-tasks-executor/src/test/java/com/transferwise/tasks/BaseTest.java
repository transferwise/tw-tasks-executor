package com.transferwise.tasks;

import com.transferwise.common.baseutils.clock.TestClock;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
public class BaseTest {

  @AfterEach
  void cleanupBaseTest() {
    TestClock.reset();
  }
}
