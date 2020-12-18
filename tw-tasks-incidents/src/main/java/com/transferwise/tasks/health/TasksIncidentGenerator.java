package com.transferwise.tasks.health;

import com.transferwise.common.incidents.Incident;
import com.transferwise.common.incidents.IncidentGenerator;
import com.transferwise.tasks.TasksProperties;
import com.transferwise.tasks.entrypoints.EntryPoint;
import com.transferwise.tasks.entrypoints.EntryPointsGroups;
import com.transferwise.tasks.entrypoints.IEntryPointsService;
import java.time.Duration;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import lombok.Setter;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.springframework.beans.factory.annotation.Autowired;

public class TasksIncidentGenerator implements IncidentGenerator {

  /**
   * Key for the Incident metaData entry storing the number of stuck tasks.
   */
  public static final String TASK_CNT_KEY = "cnt";

  @Autowired
  private TasksProperties tasksProperties;
  @Autowired
  private ITasksStateMonitor tasksStateMonitor;
  @Autowired
  @Setter
  private IEntryPointsService entryPointsHelper;

  private Incident errorIncident;
  private Incident stuckIncident;

  @Override
  @EntryPoint(usesExisting = true)
  public List<Incident> getActiveIncidents() {
    return entryPointsHelper.continueOrCreate(EntryPointsGroups.TW_TASKS_ENGINE, "ActiveIncidentsFinder",
        () -> {
          List<Pair<String, Integer>> erroneousTasksCountPerType = tasksStateMonitor.getErroneousTasksCountPerType();
          if (CollectionUtils.isNotEmpty(erroneousTasksCountPerType)) {
            int cnt = erroneousTasksCountPerType.stream().mapToInt(Pair::getValue).sum();
            if (errorIncident == null) {
              errorIncident = new Incident()
                  .setId("twTasks/error")
                  .setMessage(buildDetailedErrorReport(erroneousTasksCountPerType, "in ERROR"))
                  .setSummary("" + cnt + " tasks in ERROR state.")
                  .setMetaData(Collections.singletonMap(TASK_CNT_KEY, String.valueOf(cnt)));
            }
          } else {
            errorIncident = null;
          }

          List<Pair<String, Integer>> stuckTasksCountPerType = tasksStateMonitor.getStuckTasksCountPerType();
          if (CollectionUtils.isNotEmpty(erroneousTasksCountPerType)) {
            int cnt = stuckTasksCountPerType.stream().mapToInt(Pair::getValue).sum();
            if (stuckIncident == null) {
              stuckIncident = new Incident()
                  .setId("twTasks/stuck")
                  .setMessage(buildDetailedErrorReport(stuckTasksCountPerType, "stuck"))
                  .setSummary("" + cnt + " tasks are stuck.")
                  .setMetaData(Collections.singletonMap(TASK_CNT_KEY, String.valueOf(cnt)));
            }
          } else {
            stuckIncident = null;
          }

          return Arrays.asList(errorIncident, stuckIncident);
        });
  }

  @Override
  public Duration getPollingInterval() {
    return tasksProperties.getStuckTasksPollingInterval();
  }

  private static String buildDetailedErrorReport(List<Pair<String, Integer>> tasksInErrorPerType, String status) {
    StringBuilder msg = new StringBuilder();
    for (Pair<String, Integer> entry : tasksInErrorPerType) {
      msg.append("- ")
          .append(entry.getValue())
          .append(" tasks of type ")
          .append(entry.getKey())
          .append(" ").append(status).append("\n");
    }
    return msg.toString();
  }
}
