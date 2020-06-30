package com.transferwise.tasks;

import com.transferwise.common.baseutils.transactionsmanagement.ITransactionsHelper;
import com.transferwise.common.context.TwContextClockHolder;
import com.transferwise.tasks.test.ITestTasksService;
import com.transferwise.tasks.testapp.IResultRegisteringSyncTaskProcessor;
import com.transferwise.tasks.testapp.TestTaskHandler;
import com.transferwise.tasks.testapp.config.TestApplication;
import com.transferwise.tasks.testapp.config.TestConfiguration;
import com.transferwise.tasks.testapp.config.TestContainersInitializer;
import io.micrometer.core.instrument.MeterRegistry;
import java.util.concurrent.TimeUnit;
import lombok.extern.slf4j.Slf4j;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.TestInfo;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.context.SpringBootContextLoader;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.client.TestRestTemplate;
import org.springframework.context.ApplicationContext;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.web.servlet.MockMvc;

@ActiveProfiles(profiles = {"test", "mysql"}, resolver = SystemPropertyActiveProfilesResolver.class)
@SpringBootTest(classes = {TestConfiguration.class, TestApplication.class}, webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@ContextConfiguration(loader = SpringBootContextLoader.class, initializers = {TestContainersInitializer.class})
@Slf4j
@AutoConfigureMockMvc
public abstract class BaseIntTest {

  static {
    Awaitility.setDefaultPollInterval(5, TimeUnit.MILLISECONDS);
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
  void setApplicationContext(ApplicationContext applicationContext) {
    TestApplicationContextHolder.setApplicationContext(applicationContext);
  }

  @Autowired
  protected MockMvc mockMvc;    // use for testing secured endpoints

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

    testInfo.getTestMethod().ifPresent(name ->
        log.info("Cleaning up for '{}' It took {} ms", name, System.currentTimeMillis() - startTimeMs)
    );
  }
}

