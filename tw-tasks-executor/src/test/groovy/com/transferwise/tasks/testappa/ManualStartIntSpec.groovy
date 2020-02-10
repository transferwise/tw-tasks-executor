package com.transferwise.tasks.testappa

import com.transferwise.common.baseutils.clock.TestClock
import com.transferwise.tasks.ITasksService
import com.transferwise.tasks.buckets.IBucketsManager
import com.transferwise.tasks.dao.ITaskDao
import com.transferwise.tasks.domain.ITask
import com.transferwise.tasks.domain.Task
import com.transferwise.tasks.domain.TaskStatus
import com.transferwise.tasks.handler.SimpleTaskProcessingPolicy
import com.transferwise.tasks.handler.interfaces.ISyncTaskProcessor
import com.transferwise.tasks.test.BaseIntSpec
import groovy.util.logging.Slf4j
import org.springframework.beans.factory.annotation.Autowired

import java.time.Duration
import java.util.concurrent.Future
import java.util.concurrent.TimeUnit

import static org.awaitility.Awaitility.await

@Slf4j
class ManualStartIntSpec extends BaseIntSpec {
    private static final String BUCKET_ID = "manualStart"

    private TestClock testClock

    @Autowired
    private ITaskDao taskDao

    def setup() {
        testClock = TestClock.createAndRegister()
    }

    def "task processing in a specific bucket is not automatically started"() {
        given:
        String st = "Hello World!"
        testTaskHandlerAdapter.setProcessor(new ISyncTaskProcessor() {
            @Override
            ISyncTaskProcessor.ProcessResult process(ITask task) {
                assert st == task.getData()
            }
        })
        testTaskHandlerAdapter.setProcessingPolicy(new SimpleTaskProcessingPolicy().setProcessingBucket(BUCKET_ID))
        expect: 'Default bucket is auto started'
        testTasksService.getTasksProcessingState(IBucketsManager.DEFAULT_ID) == ITasksService.TasksProcessingState.STARTED
        when:
        log.info("Submitting a task.")
        UUID taskId = transactionsHelper.withTransaction().asNew().call({
            testTasksService.addTask(new ITasksService.AddTaskRequest().setType("test").setDataString(st))
        }).getTaskId()
        then:
        testTasksService.getTasksProcessingState(BUCKET_ID) == ITasksService.TasksProcessingState.STOPPED
        and:
        taskDao.getTask(taskId, Task.class).status == TaskStatus.SUBMITTED.name()
        when: 'lets assume task was stuck in processing'
        assert taskDao.setStatus(taskId, TaskStatus.PROCESSING, 0l)
        log.info("Moving clock forward 2 hours.")
        testClock.tick(Duration.ofHours(2))
        and: 'task gets stuck-handled'
        await().until {
            taskDao.getTask(taskId, Task.class).getStatus() == TaskStatus.ERROR.name()
        }
        then:
        1 == 1
        when:
        testTasksService.startTasksProcessing(BUCKET_ID)
        testTasksService.resumeTask(new ITasksService.ResumeTaskRequest().setTaskId(taskId)
                .setVersion(taskDao.getTaskVersion(taskId)).setForce(true))
        await().timeout(30, TimeUnit.SECONDS).until {
            return testTasksService.getFinishedTasks("test", null).size() == 1
        }
        then:
        testTasksService.getTasksProcessingState(BUCKET_ID) == ITasksService.TasksProcessingState.STARTED
        when:
        Future<Void> result = testTasksService.stopTasksProcessing(BUCKET_ID)
        result.get()
        then:
        testTasksService.getTasksProcessingState(BUCKET_ID) == ITasksService.TasksProcessingState.STOPPED
    }
}
