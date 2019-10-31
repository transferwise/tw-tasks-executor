package com.transferwise.tasks.testappa.dao

import com.transferwise.common.baseutils.clock.ClockHolder
import com.transferwise.common.baseutils.clock.TestClock
import com.transferwise.tasks.dao.ITaskDao
import com.transferwise.tasks.domain.BaseTask1
import com.transferwise.tasks.domain.FullTaskRecord
import com.transferwise.tasks.domain.Task
import com.transferwise.tasks.domain.TaskStatus
import com.transferwise.tasks.domain.TaskVersionId
import com.transferwise.tasks.test.BaseIntSpec
import org.apache.commons.lang3.tuple.Pair
import org.springframework.beans.factory.annotation.Autowired

import java.time.Duration
import java.time.ZonedDateTime
import java.time.temporal.ChronoUnit

abstract class TaskDaoIntSpec extends BaseIntSpec {
    @Autowired
    private ITaskDao taskDao

    def "inserting a task insert only one task for a given id"() {
        given:
            UUID taskId = UUID.randomUUID()

        when:
            ITaskDao.InsertTaskResponse result1 = addTask(taskId, TaskStatus.SUBMITTED)
            ITaskDao.InsertTaskResponse result2 = addTask(taskId, TaskStatus.DONE)

        then:
            result1.taskId == taskId
            result1.inserted
            result2.taskId == null
            !result2.inserted
        and:
            BaseTask1 task = taskDao.getTask(taskId, BaseTask1.class)
            task.id == taskId
        and:
            int count = taskDao.getTasksCountInStatus(10, TaskStatus.SUBMITTED)
            count == 1
    }

    def "inserting a task with unique key twice, creates only one task"() {
        given:
            String key = "Hello World"
        when:
            // TODO: Replace with a test-builder.
            def response = taskDao.insertTask(new ITaskDao.InsertTaskRequest()
                .setStatus(TaskStatus.NEW)
                .setKey(key)
                .setData("")
                .setPriority(5)
                .setMaxStuckTime(ZonedDateTime.now().plusMinutes(30))
                .setType("Test"))
        then:
            def taskId = response.getTaskId()
            taskId
        when:
            response = taskDao.insertTask(new ITaskDao.InsertTaskRequest()
                .setStatus(TaskStatus.NEW)
                .setKey(key)
                .setData("")
                .setMaxStuckTime(ZonedDateTime.now().plusMinutes(30))
                .setType("Test")
                .setPriority(5))
        then:
            !response.inserted
            !response.taskId
    }

    def "getting a task returns the correct task for BaseTask1"() {
        given:
            UUID taskId = UUID.randomUUID()
            addTask(taskId)
            addTask()

        when:
            BaseTask1 task = taskDao.getTask(taskId, BaseTask1.class)

        then:
            task.id == taskId
            task.status == "DONE"
            task.type == "TEST"
            task.priority == 5
            task.version == 0
    }

    def "getting a task returns the correct task for FullTaskRecord"() {
        given:
            UUID taskId = UUID.randomUUID()
            addTask(taskId)
            addTask()

        when:
            FullTaskRecord task = taskDao.getTask(taskId, FullTaskRecord.class)

        then:
            task.id == taskId
            task.status == "DONE"
            task.type == "TEST"
            task.priority == 5
            task.version == 0
            task.data == "DATA"
            task.subType == "SUBTYPE"
            task.processingTriesCount == 0
            task.processingClientId == null
            task.stateTime != null
            task.nextEventTime != null
    }

    def "setting to be retried sets the correct retry time"() {
        given:
            UUID taskId = UUID.randomUUID()
            ZonedDateTime retryTime = ZonedDateTime.now().plusHours(2)
            addTask(taskId)

        when:
            boolean result = taskDao.setToBeRetried(taskId, retryTime, 0, false)

        then:
            result
            FullTaskRecord task = taskDao.getTask(taskId, FullTaskRecord.class)
            task.id == taskId
            task.status == "WAITING"
            task.version == 1
            task.processingTriesCount == 0
            task.stateTime != null
            task.nextEventTime.toInstant() == retryTime.toInstant()
    }

    def "grabbing for processing increments the processing tries count"() {
        given:
            UUID taskId = UUID.randomUUID()
            String nodeId = "testNode"
            ZonedDateTime maxProcessingEndTime = ZonedDateTime.now().plusHours(2)
            addTask(taskId, TaskStatus.SUBMITTED)
            Task task = taskDao.getTask(taskId, Task.class)

        when:
            Task returnedTask = taskDao.grabForProcessing(task.toBaseTask(), nodeId, maxProcessingEndTime)

        then:
            returnedTask.id == taskId
            returnedTask.status == "PROCESSING"
            returnedTask.version == 1
            returnedTask.processingTriesCount == 1
        and:
            FullTaskRecord fullTaskRecord = taskDao.getTask(taskId, FullTaskRecord.class)
            fullTaskRecord.nextEventTime.toInstant() == maxProcessingEndTime.toInstant()
            fullTaskRecord.processingClientId == nodeId
    }

    def "setting to be retried reset tries count if specified"() {
        given:
            UUID taskId = UUID.randomUUID()
            String nodeId = "testNode"
            ZonedDateTime retryTime = ZonedDateTime.now().plusHours(2)
            addTask(taskId, TaskStatus.SUBMITTED)
            Task task = taskDao.getTask(taskId, Task.class)
            taskDao.grabForProcessing(task.toBaseTask(), nodeId, ZonedDateTime.now().plusHours(1))

        when:
            boolean result = taskDao.setToBeRetried(taskId, retryTime, 1, true)

        then:
            result
            FullTaskRecord finalTask = taskDao.getTask(taskId, FullTaskRecord.class)
            finalTask.id == taskId
            finalTask.status == "WAITING"
            finalTask.version == 2
            finalTask.processingTriesCount == 0
            finalTask.stateTime != null
            finalTask.nextEventTime.toInstant() == retryTime.toInstant()
    }

    def "grabbing for processing returns null if no task matches the one passed"() {
        given:
            UUID taskId = UUID.randomUUID()
            String nodeId = "testNode"
            ZonedDateTime maxProcessingEndTime = ZonedDateTime.now().plusHours(2)
            addTask(taskId, TaskStatus.SUBMITTED)
            Task task = new Task().setId(UUID.randomUUID())

        when:
            Task returnedTask = taskDao.grabForProcessing(task.toBaseTask(), nodeId, maxProcessingEndTime)

        then:
            returnedTask == null
        and:
            FullTaskRecord fullTaskRecord = taskDao.getTask(taskId, FullTaskRecord.class)
            fullTaskRecord.status == "SUBMITTED"
            fullTaskRecord.version == 0
            fullTaskRecord.processingTriesCount == 0
    }

    def "setting status updates the status correctly"() {
        given:
            UUID taskId = UUID.randomUUID()
            addTask(taskId, TaskStatus.SUBMITTED)

        when:
            boolean result = taskDao.setStatus(taskId, TaskStatus.WAITING, 0)

        then:
            result
            FullTaskRecord task = taskDao.getTask(taskId, FullTaskRecord.class)
            task.id == taskId
            task.status == "WAITING"
            task.version == 1
    }

    //TODO: Flaky test. WAITING task can be picked up and moved to ERROR, before we assert.
    def "scheduling task for immediate execution puts the task in WAITING state"() {
        given:
            UUID taskId = UUID.randomUUID()
            addTask(taskId, TaskStatus.SUBMITTED)

        when:
            boolean result = taskDao.scheduleTaskForImmediateExecution(taskId, 0)
        then:
            result
            FullTaskRecord task = taskDao.getTask(taskId, FullTaskRecord.class)
            task.id == taskId
            task.status == "WAITING"
            task.version == 1
    }

    def "getStuckTasks returns all tasks to retry"() {
        given:
            TestClock testClock = TestClock.createAndRegister()
            UUID taskId = UUID.randomUUID()
            addTask(taskId, TaskStatus.SUBMITTED)
            def oldNextEventTime = taskDao.getTask(taskId, FullTaskRecord.class).nextEventTime
            addRandomTask(TaskStatus.SUBMITTED)
            testClock.tick(Duration.ofMillis(1))
        when:
            ITaskDao.GetStuckTasksResponse result = taskDao.getStuckTasks(2, TaskStatus.SUBMITTED, TaskStatus.PROCESSING)
        then:
            !result.hasMore
            result.stuckTasks.size() == 2
            result.stuckTasks[0].versionId.version == 0
            result.stuckTasks[0].status == "SUBMITTED"
            result.stuckTasks[1].versionId.version == 0
            result.stuckTasks[1].status == "SUBMITTED"
        and: "task has not changed"
            FullTaskRecord fullTaskRecord = taskDao.getTask(taskId, FullTaskRecord.class)
            fullTaskRecord.status == "SUBMITTED"
            fullTaskRecord.nextEventTime == oldNextEventTime
    }

    def "markAsSubmittedAndSetNextEventTime puts the task in SUBMITTED state and updates nextEventTime"() {
        given:
            ZonedDateTime maxStuckTime = ZonedDateTime.now().plusHours(2)
            UUID taskId = UUID.randomUUID()
            addTask(taskId, TaskStatus.NEW)
            addRandomTask(TaskStatus.NEW)

        when:
            def result = taskDao.markAsSubmittedAndSetNextEventTime(new TaskVersionId(taskId, 0), maxStuckTime)

        then:
            result
            FullTaskRecord fullTaskRecord = taskDao.getTask(taskId, FullTaskRecord.class)
            fullTaskRecord.status == "SUBMITTED"
            fullTaskRecord.version == 1
            fullTaskRecord.nextEventTime.toInstant() == maxStuckTime.toInstant()
    }

    def "preparing stuck on processing tasks for resuming puts the task in SUBMITTED state"() {
        given:
            ZonedDateTime maxStuckTime = ZonedDateTime.now().plusHours(2)
            UUID taskId = UUID.randomUUID()
            String nodeId = "testNode"
            addTask(taskId, TaskStatus.SUBMITTED)
            Task task = taskDao.getTask(taskId, Task.class)
            taskDao.grabForProcessing(task.toBaseTask(), nodeId, maxStuckTime)

        when:
            List<ITaskDao.StuckTask> tasks = taskDao.prepareStuckOnProcessingTasksForResuming(nodeId, maxStuckTime)

        then:
            tasks.size() == 1
            tasks[0].versionId.version == 2
        and:
            FullTaskRecord fullTaskRecord = taskDao.getTask(taskId, FullTaskRecord.class)
            fullTaskRecord.status == "SUBMITTED"
            fullTaskRecord.nextEventTime.toInstant() == maxStuckTime.toInstant()
    }

    def "marking as submitted updates the state correctly"() {
        given:
            ZonedDateTime maxStuckTime = ZonedDateTime.now().plusHours(2)
            UUID taskId = UUID.randomUUID()
            addTask(taskId, TaskStatus.PROCESSING)

        when:
            boolean result = taskDao.markAsSubmitted(taskId, 0, maxStuckTime)

        then:
            result
        and:
            FullTaskRecord fullTaskRecord = taskDao.getTask(taskId, FullTaskRecord.class)
            fullTaskRecord.status == "SUBMITTED"
            fullTaskRecord.nextEventTime.toInstant() == maxStuckTime.toInstant()
    }

    def "finding tasks by type returns the correct tasks"() {
        given:
            String type = "MY_TYPE"
            UUID taskId = UUID.randomUUID()
            addTask(taskId, TaskStatus.PROCESSING, type)
            addRandomTask()

        when:
            List<Task> tasks = taskDao.findTasksByTypeSubTypeAndStatus(type, null, null)

        then:
            tasks.size() == 1
            tasks[0].id == taskId
            tasks[0].type == type
            tasks[0].subType == "SUBTYPE"
            tasks[0].data == "DATA"
            tasks[0].status == "PROCESSING"
            tasks[0].version == 0
            tasks[0].processingTriesCount == 0
            tasks[0].priority == 5
    }

    def "finding tasks by type and subtype returns the correct tasks"() {
        given:
            String type = "MY_TYPE"
            String subType = "MY_SUBTYPE"
            UUID taskId = UUID.randomUUID()
            addTask(taskId, TaskStatus.PROCESSING, type, subType)
            addRandomTask()

        when:
            List<Task> tasks = taskDao.findTasksByTypeSubTypeAndStatus(type, subType, null)

        then:
            tasks.size() == 1
            tasks[0].id == taskId
            tasks[0].subType == subType
    }

    def "finding tasks by type and status returns the correct tasks"() {
        given:
            String type = "MY_TYPE"
            UUID taskId = UUID.randomUUID()
            addTask(taskId, TaskStatus.PROCESSING, type)
            addRandomTask()

        when:
            List<Task> tasks = taskDao.findTasksByTypeSubTypeAndStatus(type, null, TaskStatus.PROCESSING)

        then:
            tasks.size() == 1
            tasks[0].id == taskId
            tasks[0].status == "PROCESSING"
    }

    def "getting task count in status returns the correct value"() {
        given:
            addRandomTask(TaskStatus.PROCESSING)
            addRandomTask(TaskStatus.PROCESSING)
            addRandomTask()

        when:
            int count = taskDao.getTasksCountInStatus(10, TaskStatus.PROCESSING)

        then:
            count == 2
    }

    def "getting error tasks in group"() {
        given:
            addRandomTask()
            addRandomTask(TaskStatus.ERROR)
            addRandomTask(TaskStatus.ERROR)
            addRandomTask(TaskStatus.ERROR, 1, "XXX")
        when:
            List<Pair<String, Integer>> tasksInErrorStatus = taskDao.getTasksCountInErrorGrouped(10)
        then:
            tasksInErrorStatus.size() == 2
            tasksInErrorStatus.get(0).getLeft() == "TEST"
            tasksInErrorStatus.get(0).getRight() == 2
            tasksInErrorStatus.get(1).getLeft() == "XXX"
            tasksInErrorStatus.get(1).getRight() == 1
    }

    def "getting stuck task count considers the correct status"() {
        given:
            addRandomTask(TaskStatus.PROCESSING)
            addRandomTask(TaskStatus.SUBMITTED)
            addRandomTask(TaskStatus.NEW)
            addRandomTask(TaskStatus.WAITING)
            addRandomTask(TaskStatus.DONE)
            ZonedDateTime limitTime = ZonedDateTime.now().plusSeconds(1);

        when:
            int count = taskDao.getStuckTasksCount(limitTime, 10)

        then:
            count == 4
    }

    def "getting stuck task count considers the limit time"() {
        given:
            TestClock testClock = TestClock.createAndRegister()
            addRandomTask(TaskStatus.PROCESSING)
            addRandomTask(TaskStatus.PROCESSING)
            ZonedDateTime limitTime = ZonedDateTime.now(testClock).plus(1, ChronoUnit.MILLIS)
            testClock.tick(Duration.ofMillis(1))
            addRandomTask(TaskStatus.PROCESSING)
        when:
            int count = taskDao.getStuckTasksCount(limitTime, 10)

        then:
            count == 2
    }

    def "deleting all tasks should delete all tasks"() {
        given:
            UUID taskId = UUID.randomUUID()
            addTask(taskId, TaskStatus.PROCESSING)

        when:
            taskDao.deleteAllTasks()

        then:
            FullTaskRecord fullTaskRecord = taskDao.getTask(taskId, FullTaskRecord.class)
            fullTaskRecord == null
    }

    def "deleting tasks by type deletes the correct tasks"() {
        given:
            String type = "MY_TYPE"
            UUID taskId = UUID.randomUUID()
            addTask(taskId, TaskStatus.PROCESSING, type)
            addRandomTask()

        when:
            taskDao.deleteTasks(type, null, null)

        then:
            List<Task> deletedTasks = taskDao.findTasksByTypeSubTypeAndStatus(type, null, null)
            deletedTasks.size() == 0
            List<Task> remainingTasks = taskDao.findTasksByTypeSubTypeAndStatus("TEST", null, null)
            remainingTasks.size() == 1
    }

    def "deleting tasks by type and subtype deletes the correct tasks"() {
        given:
            String type = "MY_TYPE"
            String subType = "MY_SUBTYPE"
            UUID taskId = UUID.randomUUID()
            def taskResponse1 = addTask(taskId, TaskStatus.PROCESSING, type, subType)
            def taskResponse2 = addTask(UUID.randomUUID(), TaskStatus.PROCESSING, "TEST", "SUBTYPE")

        when:
            taskDao.deleteTasks(type, subType, null)

        then:
            List<Task> deletedTasks = taskDao.findTasksByTypeSubTypeAndStatus(type, subType, null)
            deletedTasks.size() == 0
            List<Task> remainingTasks = taskDao.findTasksByTypeSubTypeAndStatus("TEST", "SUBTYPE", null)
            remainingTasks.size() == 1
    }

    def "deleting tasks by type and status deletes the correct tasks"() {
        given:
            String type = "MY_TYPE"
            UUID taskId = UUID.randomUUID()
            addTask(taskId, TaskStatus.PROCESSING, type)
            addRandomTask()

        when:
            taskDao.deleteTasks(type, null, TaskStatus.PROCESSING)

        then:
            List<Task> deletedTasks = taskDao.findTasksByTypeSubTypeAndStatus(type, null, TaskStatus.PROCESSING)
            deletedTasks.size() == 0
            List<Task> remainingTasks = taskDao.findTasksByTypeSubTypeAndStatus("TEST", null, TaskStatus.DONE)
            remainingTasks.size() == 1
    }

    def "deleting old tasks is happening in batches"() {
        given:
            TestClock clock = TestClock.createAndRegister()

            addRandomTask()
            addRandomTask()
        when:
            clock.tick(Duration.ofMinutes(11))
            def result = taskDao.deleteOldTasks(TaskStatus.DONE, Duration.ofMinutes(10), 1)
        then:
            taskDao.getTasksCountInStatus(10, TaskStatus.DONE) == 1
            result.deletedTasksCount == 1
            result.firstDeletedTaskNextEventTime
        when:
            result = taskDao.deleteOldTasks(TaskStatus.DONE, Duration.ofMinutes(10), 1)
        then:
            taskDao.getTasksCountInStatus(10, TaskStatus.DONE) == 0
            result.deletedTasksCount == 1
            result.firstDeletedTaskNextEventTime
    }

    def "deleting task by id deleted the correct task"() {
        given:
            String type = "MY_TYPE"
            UUID taskId1 = UUID.randomUUID()
            addTask(taskId1, TaskStatus.PROCESSING, type)
            UUID taskId2 = UUID.randomUUID()
            addTask(taskId2, TaskStatus.PROCESSING, type)

        when:
            taskDao.deleteTask(taskId1, 0)

        then:
            FullTaskRecord task1 = taskDao.getTask(taskId1, FullTaskRecord.class)
            task1 == null
            FullTaskRecord task2 = taskDao.getTask(taskId2, FullTaskRecord.class)
            task2.id == taskId2
    }

    def "getting tasks in error status returns tasks in the limit of maxCount"() {
        given:
            UUID taskId = UUID.randomUUID()
            addTask(taskId, TaskStatus.ERROR, "TYPE", "1")
            addRandomTask(TaskStatus.ERROR, 2)
            addRandomTask(TaskStatus.PROCESSING, 3)
            addRandomTask(TaskStatus.ERROR, 4)

        when:
            List<ITaskDao.DaoTask1> tasks = taskDao.getTasksInErrorStatus(2)

        then:
            tasks.size() == 2
            !tasks*.subType.toSet().contains("3")
    }

    def "getting tasks in processing or waiting status returns tasks in the limit of maxCount"() {
        given:
            addRandomTask(TaskStatus.PROCESSING, 1)
            addRandomTask(TaskStatus.ERROR, 2)
            addRandomTask(TaskStatus.WAITING, 3)
            addRandomTask(TaskStatus.PROCESSING, 4)
            addRandomTask(TaskStatus.WAITING, 5)

        when:
            List<ITaskDao.DaoTask1> tasks = taskDao.getTasksInProcessingOrWaitingStatus(3)

        then:
            tasks.size() == 3
            tasks*.subType.toSet().count { it in ["1", "3", "4", "5"] } == 3
            tasks*.status.every { it in [TaskStatus.PROCESSING.name(), TaskStatus.WAITING.name()] }
    }

    def "getting stuck tasks returns tasks in the limit of maxCount"() {
        given:
            addRandomTask(TaskStatus.PROCESSING)
            addRandomTask(TaskStatus.SUBMITTED)
            addRandomTask(TaskStatus.NEW)
            addRandomTask(TaskStatus.WAITING)
            addRandomTask(TaskStatus.DONE)
            addRandomTask(TaskStatus.WAITING)

        when:
            List<ITaskDao.DaoTask2> tasks = taskDao.getStuckTasks(4, Duration.ofMillis(-1))

        then:
            tasks.size() == 4
    }

    def "clearing payload and marking done update the task correctly"() {
        given:
            UUID taskId = UUID.randomUUID()
            addTask(taskId)

        when:
            boolean result = taskDao.clearPayloadAndMarkDone(taskId, 0)

        then:
            result
        and:
            FullTaskRecord task = taskDao.getTask(taskId, FullTaskRecord.class)
            task.id == taskId
            task.status == "DONE"
            task.data == ""
    }

    def "getting task version return the correct number"() {
        given:
            UUID taskId = UUID.randomUUID()
            addTask(taskId)
            addTask()

        when:
            Integer version = taskDao.getTaskVersion(taskId,)

        then:
            version == 0
    }

    def "getting tasks by UUID list works"() {
        given:
            UUID taskId1 = UUID.randomUUID()
            UUID taskId2 = UUID.randomUUID()
            UUID taskId3 = UUID.randomUUID()
            UUID taskId4 = UUID.randomUUID()
            UUID taskId5 = UUID.randomUUID()
            UUID taskId6 = UUID.randomUUID()
            UUID taskId7 = UUID.randomUUID()

            addTask(taskId1)
            addTask()
            addTask(taskId2)
            addTask()
            addTask(taskId3)
            addTask(taskId4)
            addTask(taskId5)
            addTask(taskId6)
            addTask(taskId7)

        when:
            def tasks = taskDao.getTasks([taskId1, taskId2, taskId3, taskId4, taskId5, taskId6, taskId7])

        then:
            tasks.size() == 7
            tasks*.id.toSet() == [taskId1, taskId2, taskId3, taskId4, taskId5, taskId6, taskId7].toSet()
        and: "task is correctly populated"
            def task1 = tasks.find { it.id == taskId1 }
            task1.status == "DONE"
            task1.type == "TEST"
            task1.priority == 5
            task1.version == 0
            task1.data == "DATA"
            task1.subType == "SUBTYPE"
            task1.processingTriesCount == 0
            task1.processingClientId == null
            task1.stateTime != null
            task1.nextEventTime != null

    }

    private ITaskDao.InsertTaskResponse addRandomTask(TaskStatus taskStatus = TaskStatus.DONE, Integer id = null, String type = "TEST") {
        return addTask(UUID.randomUUID(), taskStatus, type, id?.toString() ?: "SUBTYPE")
    }

    private ITaskDao.InsertTaskResponse addTask(
        UUID id = UUID.randomUUID(),
        TaskStatus taskStatus = TaskStatus.DONE,
        String type = "TEST",
        String subType = "SUBTYPE",
        String data = "DATA"
    ) {
        return taskDao.insertTask(new ITaskDao.InsertTaskRequest().setData(data)
            .setMaxStuckTime(ZonedDateTime.now(ClockHolder.getClock()))
            .setTaskId(id)
            .setPriority(5)
            .setStatus(taskStatus)
            .setType(type)
            .setSubType(subType))
    }
}
