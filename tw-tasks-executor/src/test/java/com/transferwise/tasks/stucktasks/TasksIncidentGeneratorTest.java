package com.transferwise.tasks.stucktasks;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.when;

import com.transferwise.common.incidents.Incident;
import com.transferwise.tasks.BaseTest;
import com.transferwise.tasks.TasksProperties;
import com.transferwise.tasks.health.ITasksStateMonitor;
import com.transferwise.tasks.health.TasksIncidentGenerator;
import java.util.Arrays;
import java.util.List;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.junit.jupiter.api.Test;
import org.mockito.InjectMocks;
import org.mockito.Mock;

class TasksIncidentGeneratorTest extends BaseTest {

  @Mock
  private TasksProperties tasksProperties;

  @Mock
  private ITasksStateMonitor tasksStateMonitor;

  @InjectMocks
  private TasksIncidentGenerator tasksIncidentGenerator;

  @Test
  void createMessageWithMultipleTaskDetailsWorks() {
    when(tasksStateMonitor.getStuckTasksCount()).thenReturn(0);
    when(tasksStateMonitor.getErroneousTasksCountPerType()).thenReturn(Arrays.asList(
        new ImmutablePair<>("AAA", 2),
        new ImmutablePair<>("BBB", 1)
    ));

    List<Incident> incidents = tasksIncidentGenerator.getActiveIncidents();

    assertEquals(2, incidents.size());
    assertEquals("3 tasks in ERROR state.", incidents.get(0).getSummary());
    assertEquals(
        "- 2 tasks of type AAA in ERROR\n- 1 tasks of type BBB in ERROR\n",
        incidents.get(0).getMessage()
    );
  }
}
