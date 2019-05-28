package com.transferwise.tasks.testappa

import com.transferwise.common.baseutils.transactionsmanagement.ITransactionsHelper
import com.transferwise.tasks.ITasksService
import com.transferwise.tasks.test.BaseIntSpec
import com.transferwise.tasks.test.ITestTasksService
import groovy.util.logging.Slf4j
import org.springframework.beans.factory.annotation.Autowired

import java.time.ZonedDateTime

import static org.awaitility.Awaitility.await

@Slf4j
class TaskResumingIntSpec extends BaseIntSpec {
    @Autowired
    private ITasksService tasksService
    @Autowired
    private ITestTasksService testTasksService
    @Autowired
    protected TestTaskHandler testTaskHandlerAdapter
    @Autowired
    protected IResultRegisteringSyncTaskProcessor resultRegisteringSyncTaskProcessor
    @Autowired
    protected ITransactionsHelper transactionsHelper

    def setup() {
        transactionsHelper.withTransaction().asNew().call({
            testTasksService.reset()
        })
    }

    def "a task can be successfully resumed"() {
        given:
            testTaskHandlerAdapter.setProcessor(resultRegisteringSyncTaskProcessor)
        when:
            UUID taskId = UUID.randomUUID()

            transactionsHelper.withTransaction().asNew().call({
                tasksService.addTask(new ITasksService.AddTaskRequest()
                    .setTaskId(taskId)
                    .setDataString("Hello World!")
                    .setType("test").setRunAfterTime(ZonedDateTime.now().plusHours(1)))
            })
        then:
            await().until { testTasksService.getWaitingTasks("test", null).size() > 0 }
        when:
            boolean resumed = transactionsHelper.withTransaction().asNew().call({
                tasksService.resumeTask(new ITasksService.ResumeTaskRequest().setTaskId(taskId).setVersion(0))
            })
        then:
            resumed
        when:
            await().until { resultRegisteringSyncTaskProcessor.taskResults.get(taskId) != null }
        then:
            1 == 1
    }
}
