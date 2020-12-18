package com.transferwise.tasks.health;

import java.util.List;
import org.apache.commons.lang3.tuple.Pair;

public interface ITasksStateMonitor {

  Integer getStuckTasksCount();

  List<Pair<String, Integer>> getErroneousTasksCountPerType();

  List<Pair<String, Integer>> getStuckTasksCountPerType();
}
