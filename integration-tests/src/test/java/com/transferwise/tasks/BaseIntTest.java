package com.transferwise.tasks;

import com.transferwise.common.baseutils.meters.cache.IMeterCache;
import com.transferwise.common.baseutils.transactionsmanagement.ITransactionsHelper;
import com.transferwise.common.context.TwContextClockHolder;
import com.transferwise.tasks.test.ITestTasksService;
import com.transferwise.tasks.testapp.IResultRegisteringSyncTaskProcessor;
import com.transferwise.tasks.testapp.TestTaskHandler;
import com.transferwise.tasks.testapp.config.TestApplication;
import com.transferwise.tasks.testapp.config.TestConfiguration;
import io.micrometer.core.instrument.MeterRegistry;
import java.time.Duration;
import lombok.extern.slf4j.Slf4j;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.TestInfo;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootContextLoader;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.client.TestRestTemplate;
import org.springframework.context.ApplicationContext;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.ContextConfiguration;

@ActiveProfiles(profiles = {"test", "mysql"}, resolver = SystemPropertyActiveProfilesResolver.class)
@SpringBootTest(classes = {TestConfiguration.class, TestApplication.class}, webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@ContextConfiguration(loader = SpringBootContextLoader.class)
@Slf4j
public abstract class BaseIntTest {

  static {
    Awaitility.setDefaultPollInterval(Duration.ofMillis(5));
    Awaitility.setDefaultTimeout(Duration.ofSeconds(20));
  }

  @Autowired
  protected ITestTasksService testTasksService;
  @Autowired
  protected ITransactionsHelper transactionsHelper;
  @Autowired
  protected TestRestTemplate testRestTemplate;
  @Autowired
  protected IResultRegisteringSyncTaskProcessor resultRegisteringSyncTaskProcessor;
  @Autowired
  protected TestTaskHandler testTaskHandlerAdapter;
  @Autowired
  protected MeterRegistry meterRegistry;
  @Autowired
  protected IMeterCache meterCache;

  @Autowired
  void setApplicationContext(ApplicationContext applicationContext) {
    TestApplicationContextHolder.setApplicationContext(applicationContext);
  }

  private long startTimeMs = System.currentTimeMillis();

  @BeforeEach
  void setupBaseTest(TestInfo testInfo) {
    startTimeMs = System.currentTimeMillis();
    testInfo.getTestMethod().ifPresent(name -> log.info("Setting up for '{}'", name));
  }

  @AfterEach
  void cleanupBaseTest(TestInfo testInfo) {
    TwContextClockHolder.reset();
    transactionsHelper.withTransaction().asNew().call(() -> {
      testTasksService.reset();
      return null;
    });
    resultRegisteringSyncTaskProcessor.reset();
    testTaskHandlerAdapter.reset();

    // Cheap when it's already running.
    testTasksService.resumeProcessing();

    meterRegistry.clear();
    meterCache.clear();

    testInfo.getTestMethod().ifPresent(name ->
        log.info("Cleaning up for '{}' It took {} ms", name, System.currentTimeMillis() - startTimeMs)
    );
  }

  protected void cleanWithoutException(Runnable runnable) {
    try {
      runnable.run();
    } catch (Throwable t) {
      log.error(t.getMessage(), t);
    }
  }
}

