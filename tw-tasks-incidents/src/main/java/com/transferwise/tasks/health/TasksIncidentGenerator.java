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
import java.util.Map;
import lombok.Setter;
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
          Map<String, Integer> erroneousTasksCountByType = tasksStateMonitor.getErroneousTasksCountByType();
          if (erroneousTasksCountByType != null && !erroneousTasksCountByType.isEmpty()) {
            int cnt = erroneousTasksCountByType.values().stream().mapToInt(Integer::intValue).sum();
            if (errorIncident == null) {
              errorIncident = new Incident()
                  .setId("twTasks/error")
                  .setMessage(buildDetailedErrorReport(erroneousTasksCountByType, "in ERROR"))
                  .setSummary("" + cnt + " tasks in ERROR state.")
                  .setMetaData(Collections.singletonMap(TASK_CNT_KEY, String.valueOf(cnt)));
            }
          } else {
            errorIncident = null;
          }

          Map<String, Integer> stuckTasksCountByType = tasksStateMonitor.getStuckTasksCountByType();
          if (stuckTasksCountByType != null && !stuckTasksCountByType.isEmpty()) {
            int cnt = stuckTasksCountByType.values().stream().mapToInt(Integer::intValue).sum();
            if (stuckIncident == null) {
              stuckIncident = new Incident()
                  .setId("twTasks/stuck")
                  .setMessage(buildDetailedErrorReport(stuckTasksCountByType, "stuck"))
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

  private static String buildDetailedErrorReport(Map<String, Integer> tasksInErrorPerType, String status) {
    StringBuilder msg = new StringBuilder();
    tasksInErrorPerType.forEach((type, count) -> {
      msg.append("- ")
          .append(count)
          .append(" tasks of type ")
          .append(type)
          .append(" ").append(status).append("\n");
    });

    return msg.toString();
  }
}
