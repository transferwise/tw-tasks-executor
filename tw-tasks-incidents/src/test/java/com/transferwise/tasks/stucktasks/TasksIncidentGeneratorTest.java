package com.transferwise.tasks.stucktasks;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.when;

import com.google.common.collect.ImmutableMap;
import com.transferwise.common.incidents.Incident;
import com.transferwise.tasks.entrypoints.IEntryPointsService;
import com.transferwise.tasks.health.ITasksStateMonitor;
import com.transferwise.tasks.health.TasksIncidentGenerator;
import java.util.List;
import java.util.function.Supplier;
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
    when(tasksStateMonitor.getErroneousTasksCountByType()).thenReturn(ImmutableMap.of("AAA", 2, "BBB", 1));
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
