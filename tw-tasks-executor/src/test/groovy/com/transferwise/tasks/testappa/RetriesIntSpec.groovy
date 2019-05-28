package com.transferwise.tasks.testappa

import com.transferwise.common.baseutils.clock.ClockHolder
import com.transferwise.common.baseutils.clock.TestClock
import com.transferwise.common.baseutils.transactionsmanagement.ITransactionsHelper
import com.transferwise.tasks.ITasksService
import com.transferwise.tasks.domain.IBaseTask
import com.transferwise.tasks.domain.ITask
import com.transferwise.tasks.domain.Task
import com.transferwise.tasks.handler.ExponentialTaskRetryPolicy
import com.transferwise.tasks.handler.interfaces.IAsyncTaskProcessor
import com.transferwise.tasks.handler.interfaces.ISyncTaskProcessor
import com.transferwise.tasks.handler.interfaces.ITaskRetryPolicy
import com.transferwise.tasks.test.BaseIntSpec
import com.transferwise.tasks.test.ITestTasksService
import groovy.util.logging.Slf4j
import org.junit.Ignore
import org.springframework.beans.factory.annotation.Autowired
import spock.lang.Unroll

import java.time.Duration
import java.time.ZoneOffset
import java.time.ZonedDateTime
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.atomic.AtomicLong
import java.util.function.Consumer

import static com.transferwise.tasks.handler.interfaces.ISyncTaskProcessor.ProcessResult.ResultCode.COMMIT_AND_RETRY
import static org.awaitility.Awaitility.await

@Slf4j
class RetriesIntSpec extends BaseIntSpec {
    @Autowired
    private TestTaskHandler testTaskHandlerAdapter
    @Autowired
    private ITasksService tasksService
    @Autowired
    private ITestTasksService testTasksService
    @Autowired
    private ITransactionsHelper transactionsHelper

    def "after 5 retries tasks go to ERROR state"() {
        given:
            AtomicInteger processingCount = new AtomicInteger()
            int N = 5

            testTaskHandlerAdapter.setProcessor(new ISyncTaskProcessor() {
                @Override
                ISyncTaskProcessor.ProcessResult process(ITask task) {
                    log.info("Task retry nr: " + task.getProcessingTriesCount())
                    processingCount.incrementAndGet()
                    throw new RuntimeException("You can not pass, Frodo!")
                }
            })
            testTaskHandlerAdapter.setRetryPolicy(new ExponentialTaskRetryPolicy()
                .setDelay(Duration.ofMillis(0)).setMaxCount(N).setMultiplier(1))
        when:
            transactionsHelper.withTransaction().asNew().call({
                tasksService.addTask(new ITasksService.AddTaskRequest()
                    .setDataString("Hello World!")
                    .setType("test"))
            })

            await().until {
                processingCount.get() == N
            }
        then:
            processingCount.get() == N
    }

    @Unroll
    def "Repeating cron task getProcessingTriesCount resetting (#resetTriesCountOnSuccess) works"(boolean resetTriesCountOnSuccess, int triesCount) {
        given:
            AtomicBoolean processed = new AtomicBoolean(false)
            AtomicLong triesCountInRetryPolicy
            testTaskHandlerAdapter.setProcessor(new ISyncTaskProcessor() {
                @Override
                ISyncTaskProcessor.ProcessResult process(ITask task) {
                    processed.set(true)
                    log.info("Task processed!")
                    return new ISyncTaskProcessor.ProcessResult().setResultCode(COMMIT_AND_RETRY)
                }
            })
            testTaskHandlerAdapter.setRetryPolicy(new ITaskRetryPolicy() {
                @Override
                ZonedDateTime getRetryTime(ITask task, Throwable t) {
                    triesCountInRetryPolicy = new AtomicLong(task.processingTriesCount)
                    return ZonedDateTime.ofInstant(ClockHolder.clock.instant().plusSeconds(1000), ZoneOffset.UTC)
                }

                @Override
                boolean resetTriesCountOnSuccess(IBaseTask task) {
                    return resetTriesCountOnSuccess
                }
            })
        when:
            UUID taskId = transactionsHelper.withTransaction().asNew().call({
                tasksService.addTask(new ITasksService.AddTaskRequest().setType("test")).taskId
            })
            await().until {
                if (!processed.get()) {
                    return false
                }
                return testTasksService.getWaitingTasks("test", null).stream()
                    .filter({ t -> t.id == taskId }).findAny().orElse(null) != null
            }

            Task task = testTasksService.getWaitingTasks("test", null).stream()
                .filter({ t -> t.id == taskId }).findAny().get()
        then: "tries count gets reset on success"
            triesCountInRetryPolicy.get() == triesCount
            task.processingTriesCount == triesCount
        where:
            resetTriesCountOnSuccess | triesCount
            true                     | 0
            false                    | 1
    }

    @Ignore
    def "after 5 retries asynchronous tasks go to ERROR state"() {
        given:
            AtomicInteger processingCount = new AtomicInteger()
            int N = 5

            testTaskHandlerAdapter.setProcessor(new IAsyncTaskProcessor() {
                @Override
                void process(ITask task, Runnable successCallback, Consumer<Throwable> errorCallback) {
                    log.info("Task retry nr: " + task.getProcessingTriesCount())
                    processingCount.incrementAndGet()
                    if (task.getProcessingTriesCount() > 2) {
                        throw new RuntimeException("You can not pass, Frodo!")
                    } else {
                        new Thread({
                            errorCallback.accept(null)
                        }).start()
                    }
                }
            })
            testTaskHandlerAdapter.setRetryPolicy(new ExponentialTaskRetryPolicy()
                .setDelay(Duration.ofMillis(0)).setMaxCount(N).setMultiplier(1))
        when:
            transactionsHelper.withTransaction().asNew().call({
                tasksService.addTask(new ITasksService.AddTaskRequest()
                    .setDataString("Hello World!")
                    .setType("test"))
            })

            await().until {
                processingCount.get() == N
            }
        then:
            processingCount.get() == N
    }

    def "exponential retries work, each retry should take longer"() {
        given:
            TestClock clock = TestClock.createAndRegister()

            AtomicInteger processingCount = new AtomicInteger()
            int N = 5

            testTaskHandlerAdapter.setProcessor(new ISyncTaskProcessor() {
                @Override
                ISyncTaskProcessor.ProcessResult process(ITask task) {
                    log.info("Task try #" + task.getProcessingTriesCount())
                    processingCount.incrementAndGet()
                    throw new RuntimeException("You can not pass, Legolas!")
                }
            })
            testTaskHandlerAdapter.setRetryPolicy(new ExponentialTaskRetryPolicy()
                .setDelay(Duration.ofMillis(1000)).setMaxCount(N).setMultiplier(2))
        when:
            transactionsHelper.withTransaction().asNew().call({
                tasksService.addTask(new ITasksService.AddTaskRequest()
                    .setDataString("Hello World!")
                    .setType("test"))
            })

            await().until { processingCount.get() == 1 }

            for (int i = 2; i <= N; i++) {
                long delay = 1000 * (long) Math.pow((double) 2, (i - 2))

                await().until {
                    processingCount.get() == i - 1 && !testTasksService.getWaitingTasks("test", null).isEmpty()
                }

                clock.tick(Duration.ofMillis(delay + 2))

                log.info("Time is now " + clock.instant())

                await().until {
                    processingCount.get() == i
                }
            }

        then:
            processingCount.get() == N
    }
}
