package com.transferwise.tasks.testappa

import com.transferwise.common.baseutils.transactionsmanagement.ITransactionsHelper
import com.transferwise.tasks.ITasksService
import com.transferwise.tasks.dao.ITaskDao
import com.transferwise.tasks.domain.ITask
import com.transferwise.tasks.domain.TaskStatus
import com.transferwise.tasks.handler.SimpleTaskConcurrencyPolicy
import com.transferwise.tasks.handler.SimpleTaskProcessingPolicy
import com.transferwise.tasks.handler.interfaces.ISyncTaskProcessor
import com.transferwise.tasks.handler.interfaces.ITaskProcessingPolicy
import com.transferwise.tasks.test.BaseIntSpec
import com.transferwise.tasks.test.ITestTasksService
import com.transferwise.tasks.triggering.ITasksExecutionTriggerer
import com.transferwise.tasks.triggering.KafkaTasksExecutionTriggerer
import groovy.util.logging.Slf4j
import io.micrometer.core.instrument.Counter
import io.micrometer.core.instrument.MeterRegistry
import io.micrometer.core.instrument.Timer
import org.springframework.aop.framework.Advised
import org.springframework.beans.factory.annotation.Autowired

import java.time.Duration
import java.util.concurrent.ExecutorService
import java.util.concurrent.Executors
import java.util.concurrent.TimeUnit
import java.util.stream.Collectors

import static org.awaitility.Awaitility.await

@Slf4j
class TaskProcessingIntSpec extends BaseIntSpec {
    @Autowired
    protected ITasksService tasksService
    @Autowired
    protected TestTaskHandler testTaskHandlerAdapter
    @Autowired
    protected ITaskDao taskDao
    @Autowired
    protected IResultRegisteringSyncTaskProcessor resultRegisteringSyncTaskProcessor
    @Autowired
    protected ITestTasksService testTasksService
    @Autowired
    protected ITransactionsHelper transactionsHelper
    @Autowired
    protected ITasksExecutionTriggerer tasksExecutionTriggerer
    @Autowired
    protected MeterRegistry meterRegistry

    private KafkaTasksExecutionTriggerer kafkaTasksExecutionTriggerer

    def setup() {
        kafkaTasksExecutionTriggerer = ((Advised) tasksExecutionTriggerer).getTargetSource().getTarget()
    }

    def "all unique tasks will get processed"() {
        given:
        int initialProcessingsCount = counterSum("twTasks.tasks.processingsCount")
        int initialProcessedCount = counterSum("twTasks.tasks.processedCount")
        int initialDuplicatesCount = counterSum("twTasks.tasks.duplicatesCount")
        int initialSummaryCount = timerSum("twTasks.tasks.processingTime")

        int DUPLICATES_MULTIPLIER = 2
        int UNIQUE_TASKS_COUNT = 500
        int SUBMITTING_THREADS_COUNT = 10
        int TASK_PROCESSING_CONCURRENCY = 100

        testTaskHandlerAdapter.setProcessor(resultRegisteringSyncTaskProcessor)
        testTaskHandlerAdapter.setConcurrencyPolicy(new SimpleTaskConcurrencyPolicy(TASK_PROCESSING_CONCURRENCY))
        when:
        ExecutorService executorService = Executors.newFixedThreadPool(SUBMITTING_THREADS_COUNT)

        for (int j = 0; j < DUPLICATES_MULTIPLIER; j++) {
            for (int i = 0; i < UNIQUE_TASKS_COUNT; i++) {
                final int key = i
                executorService.submit {
                    try {
                        tasksService.addTask(new ITasksService.AddTaskRequest()
                                .setDataString("Hello World! " + key)
                                .setType("test")
                                .setKey(String.valueOf(key)))
                    }
                    catch (Throwable t) {
                        log.error(t.getMessage(), t)
                    }
                }
            }
        }
        log.info("Waiting for all tasks to be registered.")
        executorService.shutdown()
        executorService.awaitTermination(15, TimeUnit.SECONDS)

        log.info("All tasks have been registered.")
        long start = System.currentTimeMillis()

        await().until { resultRegisteringSyncTaskProcessor.getTaskResults().size() == UNIQUE_TASKS_COUNT }

        long end = System.currentTimeMillis()

        log.info "Tasks execution took ${end - start} ms."
        and:
        KafkaTasksExecutionTriggerer.ConsumerBucket consumerBucket = kafkaTasksExecutionTriggerer.getConsumerBucket("default")
        then: 'all temporary states will cool down'
        consumerBucket.getOffsetsCompletedCount() == 0
        consumerBucket.getOffsetsCount() == 0
        consumerBucket.getUnprocessedFetchedRecordsCount() == 0
        when:
        await().until { consumerBucket.getOffsetsToBeCommitedCount() == 0 }
        then:
        resultRegisteringSyncTaskProcessor.getTaskResults().size() == UNIQUE_TASKS_COUNT
        and:
        // instrumentation assertions
        counterSum("twTasks.tasks.processingsCount") == UNIQUE_TASKS_COUNT + initialProcessingsCount
        counterSum("twTasks.tasks.processedCount") == UNIQUE_TASKS_COUNT + initialProcessedCount
        counterSum("twTasks.tasks.duplicatesCount") == UNIQUE_TASKS_COUNT + initialDuplicatesCount
        timerSum("twTasks.tasks.processingTime") == UNIQUE_TASKS_COUNT + initialSummaryCount
        and:
        tasksService.getTasksProcessingState(null) == ITasksService.TasksProcessingState.STARTED
    }

    def "a task running for too long will be handled"() {
        given:
        ResultRegisteringSyncTaskProcessor resultRegisteringSyncTaskProcessor = new ResultRegisteringSyncTaskProcessor() {
            @Override
            ISyncTaskProcessor.ProcessResult process(ITask task) {
                log.info("Starting a long running task {}", task.id)
                sleep(100_000)
                log.info("Finished. Now marking as processed {}", task.id)
                return super.process(task)
            }
        }
        testTaskHandlerAdapter.setProcessor(resultRegisteringSyncTaskProcessor)
        testTaskHandlerAdapter.setConcurrencyPolicy(new SimpleTaskConcurrencyPolicy(100))
        testTaskHandlerAdapter.setProcessingPolicy(new SimpleTaskProcessingPolicy()
                .setStuckTaskResolutionStrategy(ITaskProcessingPolicy.StuckTaskResolutionStrategy.MARK_AS_ERROR)
                .setMaxProcessingDuration(Duration.ofMillis(100))
        )
        when:
        ITasksService.AddTaskResponse task
        Thread.start { // Just to trigger the task
            task = tasksService.addTask(new ITasksService.AddTaskRequest()
                    .setDataString("Hello World! 1")
                    .setType("test")
                    .setKey(String.valueOf("1"))
            )
        }.join()

        long start = System.currentTimeMillis()

        await().atMost(30, TimeUnit.SECONDS).until {
            def proceessedTasksCount = resultRegisteringSyncTaskProcessor.getTaskResults().size()
            if (proceessedTasksCount > 1) // Will explode if we restarted stuck task above.
                throw new RuntimeException("We have more running tasks than we expect")
            List<ITaskDao.DaoTask1> error = null
            transactionsHelper.withTransaction().asNew().call {
                error = taskDao.getTasksInErrorStatus(10)
            }
            def taskWasMarkedAsError = error.size() != 0 && error.first().id == task.taskId
            if (taskWasMarkedAsError) {
                log.info("StuckTaskResolution worked correctly, task {} was marked as ERROR", task.taskId)
            }
            if (proceessedTasksCount == 1) {
                throw new RuntimeException("Let's fail, as it means that task completed earlier, than was marked" +
                        " as error, which probably means that stuck task processor didn't work")
            }
            taskWasMarkedAsError
        }

        long end = System.currentTimeMillis()

        log.info "Tasks execution took ${end - start} ms."
        then:
        1 == 1
        and:
        meterRegistry.find("twTasks.tasks.markedAsErrorCount").counter().count() == 1
    }

    def "a task with huge message can be handled"() {
        given:
        StringBuilder sb = new StringBuilder()
        // 11MB
        for (int i = 0; i < 1000 * 1000; i++) {
            sb.append("Hello World!")
        }
        String st = sb.toString()
        testTaskHandlerAdapter.setProcessor(new ISyncTaskProcessor() {
            @Override
            ISyncTaskProcessor.ProcessResult process(ITask task) {
                assert st == task.getData()
            }
        })
        when:
        log.info("Submitting huge message task.")
        transactionsHelper.withTransaction().asNew().call({
            tasksService.addTask(new ITasksService.AddTaskRequest().setType("test").setDataString(st))
        })
        await().until {
            try {
                transactionsHelper.withTransaction().asNew().call({
                    return testTasksService.getFinishedTasks("test", null).size() == 1
                })
            }
            catch (Throwable t) {
                log.error(t.getMessage(), t)
            }
        }
        then:
        1 == 1
    }

    def "a task with unknown bucket is moved to error state"() {
        given:
        testTaskHandlerAdapter.setProcessor(resultRegisteringSyncTaskProcessor)
        testTaskHandlerAdapter.setProcessingPolicy(new SimpleTaskProcessingPolicy()
                .setProcessingBucket("unknown-bucket"))
        when:
        transactionsHelper.withTransaction().asNew().call({
            tasksService.addTask(new ITasksService.AddTaskRequest().setType("test").setDataString("Hello World!"))
        })
        await().until {
            try {
                transactionsHelper.withTransaction().asNew().call({
                    return testTasksService.getTasks("test", null, TaskStatus.ERROR).size() == 1
                })
            }
            catch (Throwable t) {
                log.error(t.getMessage(), t)
            }
        }
        then:
        1 == 1
    }

    private double counterSum(String name) {
        return meterRegistry.find(name).counters().stream().collect(Collectors.summingDouble({ Counter c -> c.count() }))
    }

    private long timerSum(String name) {
        meterRegistry.find(name).timers().stream().collect(Collectors.summingLong({ Timer t -> t.count() }))
    }

}
