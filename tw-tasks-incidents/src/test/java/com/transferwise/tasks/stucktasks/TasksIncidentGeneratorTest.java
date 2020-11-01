package com.transferwise.tasks.stucktasks;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.when;

import com.transferwise.common.incidents.Incident;
import com.transferwise.tasks.entrypoints.IEntryPointsService;
import com.transferwise.tasks.health.ITasksStateMonitor;
import com.transferwise.tasks.health.TasksIncidentGenerator;
import java.util.Arrays;
import java.util.List;
import java.util.function.Supplier;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class TasksIncidentGeneratorTest {

  @Mock
  private ITasksStateMonitor tasksStateMonitor;

  @Mock
  private IEntryPointsService entryPointsService;

  @InjectMocks
  private TasksIncidentGenerator tasksIncidentGenerator;

  @Test
  void createMessageWithMultipleTaskDetailsWorks() {
    when(tasksStateMonitor.getStuckTasksCount()).thenReturn(0);
    when(tasksStateMonitor.getErroneousTasksCountPerType()).thenReturn(Arrays.asList(
        new ImmutablePair<>("AAA", 2),
        new ImmutablePair<>("BBB", 1)
    ));
    //noinspection unchecked,rawtypes
    when(entryPointsService.continueOrCreate(anyString(), anyString(), any(Supplier.class))).thenAnswer(
        invocation -> ((Supplier) invocation.getArgument(2)).get());

    List<Incident> incidents = tasksIncidentGenerator.getActiveIncidents();

    assertEquals(2, incidents.size());
    assertEquals("3 tasks in ERROR state.", incidents.get(0).getSummary());
    assertEquals(
        "- 2 tasks of type AAA in ERROR\n- 1 tasks of type BBB in ERROR\n",
        incidents.get(0).getMessage()
    );
  }
}
