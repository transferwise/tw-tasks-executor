package com.transferwise.tasks.testappa;

import com.transferwise.tasks.domain.ITask;
import com.transferwise.tasks.handler.interfaces.ISyncTaskProcessor;
import java.util.Map;
import java.util.UUID;
import java.util.function.Predicate;

public interface IResultRegisteringSyncTaskProcessor extends ISyncTaskProcessor {

  void reset();

  void setTaskResults(Map<UUID, Boolean> taskResults);

  Map<UUID, Boolean> getTaskResults();

  void setResultPredicate(Predicate<ITask> resultPredicate);

  Predicate<ITask> getResultPredicate();
}
