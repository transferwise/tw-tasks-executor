package com.transferwise.tasks.health;

import com.newrelic.api.agent.NewRelic;
import com.newrelic.api.agent.Trace;
import com.transferwise.common.incidents.Incident;
import com.transferwise.common.incidents.IncidentGenerator;
import com.transferwise.tasks.TasksProperties;
import java.time.Duration;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
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

  private Incident errorIncident;
  private Incident stuckIncident;

  @Override
  @Trace(dispatcher = true)
  public List<Incident> getActiveIncidents() {
    NewRelic.setTransactionName("TwTasksEngine", "GetActiveIncidents");

    List<Pair<String, Integer>> erroneousTasksCountPerType = tasksStateMonitor.getErroneousTasksCountPerType();
    if (CollectionUtils.isNotEmpty(erroneousTasksCountPerType)) {
      int cnt = erroneousTasksCountPerType.stream().mapToInt(Pair::getValue).sum();
      if (errorIncident == null) {
        errorIncident = new Incident()
            .setId("twTasks/error")
            .setMessage(buildDetailedErrorReport(erroneousTasksCountPerType))
            .setSummary("" + cnt + " tasks in ERROR state.")
            .setMetaData(Collections.singletonMap(TASK_CNT_KEY, String.valueOf(cnt)));
      }
    } else {
      errorIncident = null;
    }

    Integer stuckTasksCount = tasksStateMonitor.getStuckTasksCount();
    if (stuckTasksCount != null && stuckTasksCount > 0) {
      if (stuckIncident == null) {
        stuckIncident = new Incident()
            .setId("twTasks/stuck")
            .setMessage("" + stuckTasksCount + " tasks are stuck.")
            .setSummary("" + stuckTasksCount + " tasks are stuck.")
            .setMetaData(Collections.singletonMap(TASK_CNT_KEY, String.valueOf(stuckTasksCount)));
      }
    } else {
      stuckIncident = null;
    }

    return Arrays.asList(errorIncident, stuckIncident);
  }

  @Override
  public Duration getPollingInterval() {
    return tasksProperties.getStuckTasksPollingInterval();
  }

  private static String buildDetailedErrorReport(List<Pair<String, Integer>> tasksInErrorPerType) {
    StringBuilder msg = new StringBuilder();
    for (Pair<String, Integer> entry : tasksInErrorPerType) {
      msg.append("- ")
          .append(entry.getValue())
          .append(" tasks of type ")
          .append(entry.getKey())
          .append(" in ERROR\n");
    }
    return msg.toString();
  }
}
