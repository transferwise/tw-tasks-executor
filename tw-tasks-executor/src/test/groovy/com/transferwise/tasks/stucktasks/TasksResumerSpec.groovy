package com.transferwise.tasks.stucktasks

import com.transferwise.common.baseutils.clock.ClockHolder
import com.transferwise.tasks.TasksProperties
import com.transferwise.tasks.config.IExecutorServicesProvider
import com.transferwise.tasks.dao.ITaskDao
import com.transferwise.tasks.domain.BaseTask
import com.transferwise.tasks.domain.TaskStatus
import com.transferwise.tasks.domain.TaskVersionId
import com.transferwise.tasks.handler.interfaces.ITaskHandler
import com.transferwise.tasks.handler.interfaces.ITaskHandlerRegistry
import com.transferwise.tasks.handler.interfaces.ITaskProcessingPolicy
import com.transferwise.tasks.helpers.IMeterHelper
import com.transferwise.tasks.test.BaseSpec
import com.transferwise.tasks.triggering.ITasksExecutionTriggerer
import org.apache.curator.framework.CuratorFramework
import spock.lang.Specification
import spock.lang.Unroll

import java.time.Clock
import java.time.Duration
import java.time.ZonedDateTime
import java.util.concurrent.atomic.AtomicInteger

class TasksResumerSpec extends BaseSpec {
    private ITasksExecutionTriggerer tasksExecutionTriggerer = Mock()
    private ITaskHandlerRegistry taskHandlerRegistry = Mock()
    private TasksProperties tasksProperties = Mock()
    private ITaskDao taskDao = Mock()
    private CuratorFramework curatorFramework = Mock()
    private IExecutorServicesProvider executorServicesProvider = Mock()
    private IMeterHelper meterHelper = Mock()

    private TasksResumer service = new TasksResumer(
        tasksExecutionTriggerer,
        taskHandlerRegistry,
        tasksProperties,
        taskDao,
        curatorFramework,
        executorServicesProvider,
        meterHelper
    )
    private ZonedDateTime now = ZonedDateTime.now(ClockHolder.clock)

    def setup() {
        tasksProperties.getTaskStuckTimeout() >> Duration.ofMinutes(10)
        ClockHolder.setClock(Clock.fixed(now.toInstant(), now.zone))
    }

    @Unroll
    void "handleStuckTask respects StuckTaskResolutionStrategy #resolutionStrategy"() {
        given:
            def task = new ITaskDao.StuckTask().setType("TEST").setStatus(TaskStatus.PROCESSING.name()).setVersionId(new TaskVersionId(UUID.randomUUID(), 0))
            def nResumed = new AtomicInteger()
            def nError = new AtomicInteger()
            def nFailed = new AtomicInteger()

            ITaskProcessingPolicy processingPolicy = Mock()
            processingPolicy.getStuckTaskResolutionStrategy(_) >> resolutionStrategy
            processingPolicy.getMaxProcessingEndTime(_) >> null
            taskHandlerRegistry.getTaskHandler(_) >> Stub(ITaskHandler)
            taskHandlerRegistry.getTaskHandler(null).getProcessingPolicy(_) >> processingPolicy
        when:
            service.handleStuckTask(task, nResumed, nError, nFailed)
        then:
            //noinspection GroovyAssignabilityCheck
            resumed * tasksExecutionTriggerer.trigger({ BaseTask baseTask -> baseTask.version == 1 && baseTask.id == task.getVersionId().id })
            resumed * taskDao.markAsSubmitted(task.versionId.id, task.versionId.version, now.plusMinutes(10)) >> true
            error * taskDao.setStatus(task.versionId.id, TaskStatus.ERROR, task.versionId.version) >> true
            failed * taskDao.setStatus(task.versionId.id, TaskStatus.FAILED, task.versionId.version) >> true
            nResumed.get() == resumed
            nError.get() == error
            nFailed.get() == failed

        where:
            resumed | failed | error || resolutionStrategy
            1       | 0      | 0     || ITaskProcessingPolicy.StuckTaskResolutionStrategy.RETRY
            0       | 1      | 0     || ITaskProcessingPolicy.StuckTaskResolutionStrategy.MARK_AS_FAILED
            0       | 0      | 1     || ITaskProcessingPolicy.StuckTaskResolutionStrategy.MARK_AS_ERROR
            0       | 0      | 0     || ITaskProcessingPolicy.StuckTaskResolutionStrategy.IGNORE
    }

    def "stuck tasks not in PROCESSING state will be resumed"() {
        given:
            def task = new ITaskDao.StuckTask().setType("TEST").setStatus(TaskStatus.SUBMITTED.name()).setVersionId(new TaskVersionId(UUID.randomUUID(), 0))
            def nResumed = new AtomicInteger()
        when:
            service.handleStuckTask(task, nResumed, null, null)
        then:
            1 * tasksExecutionTriggerer.trigger({ BaseTask baseTask -> baseTask.version == 1 && baseTask.id == task.getVersionId().id })
            1 * taskDao.markAsSubmitted(task.versionId.id, task.versionId.version, now.plusMinutes(10)) >> true
        and:
            nResumed.get() == 1
    }
}
