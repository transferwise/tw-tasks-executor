package com.transferwise.tasks.testappa

import com.transferwise.common.baseutils.clock.ClockHolder
import com.transferwise.tasks.dao.ITaskDao
import com.transferwise.tasks.domain.FullTaskRecord
import com.transferwise.tasks.domain.TaskStatus
import com.transferwise.tasks.domain.TaskVersionId
import com.transferwise.tasks.management.ITasksManagementPort
import com.transferwise.tasks.stucktasks.TasksResumer
import com.transferwise.tasks.test.BaseIntSpec
import com.transferwise.tasks.test.TaskTestBuilder
import org.springframework.beans.factory.annotation.Autowired
import spock.lang.Unroll

import java.time.ZonedDateTime

@Unroll
class TasksManagementIntSpec extends BaseIntSpec {
    @Autowired
    private ITaskDao taskDao

    @Autowired
    private TasksResumer tasksResumer

    def "erronoeus tasks will be correctly found"() {
        given:
            transactionsHelper.withTransaction().asNew().call {
                TaskTestBuilder.aTask().inStatus(TaskStatus.ERROR).withMaxStuckTime(ZonedDateTime.now().plusDays(1)).build()
            }
            UUID task1Id = transactionsHelper.withTransaction().asNew().call {
                TaskTestBuilder.aTask().inStatus(TaskStatus.ERROR).withType("T1").withSubType("S1")
                    .withMaxStuckTime(ZonedDateTime.now().plusDays(2)).build()
            }
        when:
            def response = testRestTemplate.postForEntity("/v1/twTasks/getTasksInError",
                new ITasksManagementPort.GetTasksInErrorRequest().setMaxCount(1), ITasksManagementPort.GetTasksInErrorResponse)
        then:
            response.statusCodeValue == 200
            def tasksInError = response.getBody().getTasksInError()
            tasksInError.size() == 1
            tasksInError[0].taskVersionId.id == task1Id
            tasksInError[0].type == "T1"
            tasksInError[0].subType == "S1"
    }

    def "stuck tasks will be correctly found"() {
        given:
            transactionsHelper.withTransaction().asNew().call {
                TaskTestBuilder.aTask().inStatus(TaskStatus.PROCESSING).withMaxStuckTime(ZonedDateTime.now().minusDays(2)).build()
                // It should not be found as we have 10s delta by default.
                TaskTestBuilder.aTask().inStatus(TaskStatus.PROCESSING).withMaxStuckTime(ZonedDateTime.now()).build()
            }
            UUID task1Id = transactionsHelper.withTransaction().asNew().call {
                TaskTestBuilder.aTask().inStatus(TaskStatus.SUBMITTED).withMaxStuckTime(ZonedDateTime.now().minusDays(1)).build()
            }
        when:
            def response = testRestTemplate.postForEntity("/v1/twTasks/getTasksStuck",
                new ITasksManagementPort.GetTasksStuckRequest().setMaxCount(1), ITasksManagementPort.GetTasksStuckResponse)
        then:
            response.statusCodeValue == 200
            def tasksStuck = response.getBody().getTasksStuck()
            tasksStuck.size() == 1
            tasksStuck[0].taskVersionId.id == task1Id
        when:
            response = testRestTemplate.postForEntity("/v1/twTasks/getTasksStuck",
                new ITasksManagementPort.GetTasksStuckRequest().setMaxCount(10), ITasksManagementPort.GetTasksStuckResponse)
        then:
            response.getBody().getTasksStuck().size() == 2
    }

    def "find a task in #status state"() {
        given:
            tasksResumer.pause()
            UUID taskId = transactionsHelper.withTransaction().asNew().call {
                TaskTestBuilder.aTask().inStatus(status).withMaxStuckTime(ZonedDateTime.now().minusDays(2)).build()
            }
        when:
            def response = testRestTemplate.getForEntity("/v1/twTasks/task/${taskId}/noData".toString(), ITasksManagementPort.TaskWithoutData)
        then:
            response.statusCodeValue == 200
            response.body.id == taskId.toString()
            response.body.type == "test"
            response.body.status == status.name()
        cleanup:
            tasksResumer.resume()
        where:
            status                | _
            TaskStatus.NEW        | _
            TaskStatus.WAITING    | _
            TaskStatus.PROCESSING | _
            TaskStatus.ERROR      | _
            TaskStatus.DONE       | _
            TaskStatus.FAILED     | _
            TaskStatus.SUBMITTED  | _
    }

    def "marking a task as failed works"() {
        given:
            UUID task0Id = transactionsHelper.withTransaction().asNew().call {
                TaskTestBuilder.aTask().inStatus(TaskStatus.ERROR).withMaxStuckTime(ZonedDateTime.now().plusDays(1)).build()
            }
        when:
            def response = testRestTemplate.postForEntity("/v1/twTasks/markTasksAsFailed",
                new ITasksManagementPort.MarkTasksAsFailedRequest().addTaskVersionId(
                    new TaskVersionId().setId(task0Id).setVersion(0)
                ), ITasksManagementPort.MarkTasksAsFailedResponse)
        then:
            response.statusCodeValue == 200
            response.getBody().getResults()[task0Id].success
            def task = taskDao.getTask(task0Id, FullTaskRecord.class)
            task.getStatus() == TaskStatus.FAILED.name()
    }

    def "immediately resuming a task works"() {
        given:
            UUID task0Id = transactionsHelper.withTransaction().asNew().call {
                TaskTestBuilder.aTask().inStatus(TaskStatus.ERROR).withMaxStuckTime(ZonedDateTime.now().plusDays(1)).build()
            }

            tasksResumer.pause()
        when:
            def response = testRestTemplate.postForEntity("/v1/twTasks/resumeTasksImmediately",
                new ITasksManagementPort.ResumeTasksImmediatelyRequest().addTaskVersionId(
                    new TaskVersionId().setId(task0Id).setVersion(0)
                ), ITasksManagementPort.ResumeTasksImmediatelyResponse)
        then:
            response.statusCodeValue == 200
            response.getBody().getResults()[task0Id].success
            def task = taskDao.getTask(task0Id, FullTaskRecord.class)
            task.getStatus() == TaskStatus.WAITING.name()
            !task.getNextEventTime().isAfter(ZonedDateTime.now(ClockHolder.getClock()).plusSeconds(1)) // decrementing 1 because of database rounding error.
        cleanup:
            tasksResumer.resume()
    }

    def "immediately resuming all tasks works"() {
        given:
            UUID task0Id = transactionsHelper.withTransaction().asNew().call {
                TaskTestBuilder.aTask().inStatus(TaskStatus.ERROR).withMaxStuckTime(ZonedDateTime.now().plusDays(1)).build()
            }

            tasksResumer.pause()
        when:
            def response = testRestTemplate.postForEntity("/v1/twTasks/resumeAllTasksImmediately",
                new ITasksManagementPort.ResumeAllTasksImmediatelyRequest().setTaskType("test"),
                ITasksManagementPort.ResumeTasksImmediatelyResponse)
        then:
            response.statusCodeValue == 200
            response.getBody().getResults()[task0Id].success
            def task = taskDao.getTask(task0Id, FullTaskRecord.class)
            task.getStatus() == TaskStatus.WAITING.name()
            !task.getNextEventTime().isAfter(ZonedDateTime.now(ClockHolder.getClock()).plusSeconds(1)) // decrementing 1 because of database rounding error.
        cleanup:
            tasksResumer.resume()
    }
}
