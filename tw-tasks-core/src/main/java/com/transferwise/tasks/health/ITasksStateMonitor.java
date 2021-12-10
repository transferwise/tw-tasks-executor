package com.transferwise.tasks.health;

import com.transferwise.tasks.domain.TaskStatus;
import java.util.Map;
import org.apache.commons.lang3.tuple.Pair;

public interface ITasksStateMonitor {

  Map<String, Integer> getErroneousTasksCountByType();

  Map<Pair<TaskStatus, String>, Integer> getStuckTasksCountByType();
}
