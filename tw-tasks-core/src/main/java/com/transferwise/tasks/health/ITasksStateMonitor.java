package com.transferwise.tasks.health;

import java.util.Map;

public interface ITasksStateMonitor {

  Map<String, Integer> getErroneousTasksCountByType();

  Map<String, Integer> getStuckTasksCountByType();
}
