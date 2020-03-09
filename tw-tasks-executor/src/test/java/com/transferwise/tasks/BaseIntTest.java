package com.transferwise.tasks;

import com.transferwise.common.baseutils.clock.ClockHolder;
import com.transferwise.common.baseutils.clock.TestClock;
import com.transferwise.common.baseutils.transactionsmanagement.ITransactionsHelper;
import com.transferwise.tasks.test.ITestTasksService;
import com.transferwise.tasks.testapp.IResultRegisteringSyncTaskProcessor;
import com.transferwise.tasks.testapp.TestTaskHandler;
import com.transferwise.tasks.testapp.config.TestApplication;
import com.transferwise.tasks.testapp.config.TestConfiguration;
import com.transferwise.tasks.testapp.config.TestContainersInitializer;
import java.util.concurrent.TimeUnit;
import lombok.extern.slf4j.Slf4j;
import org.awaitility.Awaitility;
import org.junit.Rule;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.rules.TestName;
import org.mockito.junit.MockitoJUnit;
import org.mockito.junit.MockitoRule;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootContextLoader;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.client.TestRestTemplate;
import org.springframework.context.ApplicationContext;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.ContextConfiguration;

@ActiveProfiles(profiles = {"test", "mysql"}, resolver = SystemPropertyActiveProfilesResolver.class)
@SpringBootTest(classes = {TestConfiguration.class, TestApplication.class}, webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@ContextConfiguration(loader = SpringBootContextLoader.class, initializers = {TestContainersInitializer.class})
@Slf4j
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

  @Rule
  public TestName testName = new TestName();

  @Rule
  public MockitoRule mockitoRule = MockitoJUnit.rule();

  @Autowired
  void setApplicationContext(ApplicationContext applicationContext) {
    TestApplicationContextHolder.setApplicationContext(applicationContext);
  }

  private long startTimeMs = ClockHolder.getClock().millis();

  @BeforeEach
  void setupBaseTest() {
    startTimeMs = System.currentTimeMillis();
    log.info("Setting up for '{}'", testName.getMethodName());
  }

  @AfterEach
  void cleanupBaseTest() {
    TestClock.reset();
    transactionsHelper.withTransaction().asNew().call(() -> {
      testTasksService.reset();
      return null;
    });
    resultRegisteringSyncTaskProcessor.reset();
    testTaskHandlerAdapter.reset();

    log.info("Cleaning up for '{}' It took {} ms", testName.getMethodName(), System.currentTimeMillis() - startTimeMs);
  }
}

