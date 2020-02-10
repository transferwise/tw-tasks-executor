package com.transferwise.tasks.stucktasks

import com.transferwise.common.incidents.Incident
import com.transferwise.tasks.health.ITasksStateMonitor
import com.transferwise.tasks.health.TasksIncidentGenerator
import com.transferwise.tasks.test.BaseSpec
import org.apache.commons.lang3.tuple.ImmutablePair

class TasksIncidentGeneratorSpec extends BaseSpec {
    private TasksIncidentGenerator generator

    def setup() {
        generator = new TasksIncidentGenerator()
        generator.tasksStateMonitor = Mock(ITasksStateMonitor)
    }

    def "create message with multiple task details works"() {
        given:
        1 * generator.tasksStateMonitor.getStuckTasksCount() >> 0
        1 * generator.tasksStateMonitor.getErroneousTasksCountPerType() >> [
                new ImmutablePair("AAA", 2),
                new ImmutablePair("BBB", 1)
        ]
        when:
        List<Incident> incidents = generator.getActiveIncidents()
        then:
        incidents.size() == 2
        incidents[0].summary == "3 tasks in ERROR state."
        incidents[0].message == """- 2 tasks of type AAA in ERROR
- 1 tasks of type BBB in ERROR
"""
    }
}
