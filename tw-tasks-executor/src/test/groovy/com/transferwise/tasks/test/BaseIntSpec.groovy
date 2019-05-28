package com.transferwise.tasks.test

import com.transferwise.common.baseutils.clock.TestClock
import com.transferwise.common.baseutils.transactionsmanagement.ITransactionsHelper
import com.transferwise.tasks.testappa.IResultRegisteringSyncTaskProcessor
import com.transferwise.tasks.testappa.TestTaskHandler
import com.transferwise.tasks.testappa.config.TestApplication
import com.transferwise.tasks.testappa.config.TestConfiguration
import com.transferwise.tasks.testappa.config.TestContainersInitializer
import org.awaitility.Awaitility
import org.junit.Rule
import org.mockito.junit.MockitoJUnit
import org.mockito.junit.MockitoRule
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.boot.test.context.SpringBootContextLoader
import org.springframework.boot.test.context.SpringBootTest
import org.springframework.boot.test.web.client.TestRestTemplate
import org.springframework.context.ApplicationContext
import org.springframework.test.context.ActiveProfiles
import org.springframework.test.context.ContextConfiguration
import spock.lang.Specification

import java.util.concurrent.TimeUnit

@ActiveProfiles(profiles = ["test", "mysql"], resolver = SystemPropertyActiveProfilesResolver)
@SpringBootTest(classes = [TestConfiguration, TestApplication], webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@ContextConfiguration(loader = SpringBootContextLoader, initializers = [TestContainersInitializer])
abstract class BaseIntSpec extends Specification {
    @Autowired
    protected ITestTasksService testTasksService
    @Autowired
    protected ITransactionsHelper transactionsHelper
    @Autowired
    protected TestRestTemplate testRestTemplate
    @Autowired
    protected IResultRegisteringSyncTaskProcessor resultRegisteringSyncTaskProcessor
    @Autowired
    protected TestTaskHandler testTaskHandlerAdapter

    @Autowired
    void setApplicationContext(ApplicationContext applicationContext) {
        TestApplicationContextHolder.setApplicationContext(applicationContext)
    }

    @Rule
    public MockitoRule mockitoRule = MockitoJUnit.rule()

    protected TestClock testClock

    static {
        Awaitility.setDefaultPollInterval(5, TimeUnit.MILLISECONDS)
    }

    def setup() {
        testClock = TestClock.createAndRegister()
        TestClock.reset()
        transactionsHelper.withTransaction().asNew().call {
            testTasksService.reset()
        }
    }

    def cleanup() {
        resultRegisteringSyncTaskProcessor.reset()
        testTaskHandlerAdapter.reset()

    }

    protected TestClock getTestClock() {
        return testClock
    }
}
