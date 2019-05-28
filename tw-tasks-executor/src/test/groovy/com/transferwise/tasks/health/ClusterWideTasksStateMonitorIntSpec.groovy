package com.transferwise.tasks.health

import com.transferwise.tasks.ITasksService
import com.transferwise.tasks.domain.ITask
import com.transferwise.tasks.domain.TaskStatus
import com.transferwise.tasks.handler.interfaces.ISyncTaskProcessor
import com.transferwise.tasks.test.BaseIntSpec
import io.micrometer.core.instrument.MeterRegistry
import org.springframework.beans.factory.annotation.Autowired

import java.util.concurrent.TimeUnit

import static org.awaitility.Awaitility.await

class ClusterWideTasksStateMonitorIntSpec extends BaseIntSpec {
    @Autowired
    private MeterRegistry meterRegistry
    @Autowired
    private ClusterWideTasksStateMonitor clusterWideTasksStateMonitor

    def "metrics are correctly registered"() {
        when:
            def hlGauges = meterRegistry.get("twTasks.health.tasksHistoryLengthSeconds").gauges()
        then:
            hlGauges.size() == 2
            hlGauges.find { it.getId().getTag("taskStatus") == "DONE" }
            hlGauges.find { it.getId().getTag("taskStatus") == "ERROR" }
        when:
            def tasksInErrorCountGauge = meterRegistry.get("twTasks.health.tasksInErrorCount").gauge()
            def stuckTasksGauge = meterRegistry.get("twTasks.health.stuckTasksCount").gauge()
        then:
            await().atMost(31, TimeUnit.SECONDS).until { tasksInErrorCountGauge.value() == 0 }
            stuckTasksGauge.value() == 0
        when:
            clusterWideTasksStateMonitor.leaderSelector.stop()
            await().until({ meterRegistry.find("twTasks.health.tasksInErrorCount").gauges().size() == 0 })
        then: 'metrics get unregistered'
            meterRegistry.find("twTasks.health.tasksInErrorCount").gauges().size() == 0
            meterRegistry.find("twTasks.health.stuckTasksCount").gauges().size() == 0
            meterRegistry.find("twTasks.health.tasksHistoryLengthSeconds").gauges().size() == 0
        when: 'everything works when the node gets leader back'
            clusterWideTasksStateMonitor.leaderSelector.start()
            await().until({ meterRegistry.find("twTasks.health.tasksInErrorCount").gauges().size() == 1 })
        then:
            1 == 1

        when:
            testTaskHandlerAdapter.setProcessor(new ISyncTaskProcessor() {
                @Override
                ISyncTaskProcessor.ProcessResult process(ITask task) {
                    throw new IllegalStateException("We want an error!")
                }
            })
            transactionsHelper.withTransaction().asNew().call({
                testTasksService.addTask(new ITasksService.AddTaskRequest()
                    .setDataString("Hello World!")
                    .setType("test"))
            })
            await().until({ testTasksService.getTasks("test", null, TaskStatus.ERROR).size() == 1 })
        and:
            clusterWideTasksStateMonitor.check()
        then:
            meterRegistry.find("twTasks.health.tasksInErrorCount").gauge().value() == 1

        when:
            transactionsHelper.withTransaction().asNew().call({
                testTasksService.resetAndDeleteTasksWithTypes("test")
            })
        and:
            clusterWideTasksStateMonitor.check()
        then:
            meterRegistry.find("twTasks.health.tasksInErrorCount").gauge().value() == 0
            meterRegistry.getMeters().findAll({ it ->
                it.getId().getName() == "twTasks.health.tasksInErrorCountPerType"
            }).isEmpty()
    }
}
