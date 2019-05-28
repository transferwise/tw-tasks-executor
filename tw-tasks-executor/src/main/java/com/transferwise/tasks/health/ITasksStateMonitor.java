package com.transferwise.tasks.health;

import org.apache.commons.lang3.tuple.Pair;

import java.util.List;

public interface ITasksStateMonitor {
    Integer getStuckTasksCount();

    List<Pair<String, Integer>> getErroneousTasksCountPerType();
}
