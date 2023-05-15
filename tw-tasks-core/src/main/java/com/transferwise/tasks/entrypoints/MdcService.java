package com.transferwise.tasks.entrypoints;

import com.transferwise.common.baseutils.ExceptionUtils;
import com.transferwise.common.context.TwContext;
import com.transferwise.tasks.TasksProperties;
import com.transferwise.tasks.domain.IBaseTask;
import com.transferwise.tasks.domain.ITask;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.Callable;
import lombok.NonNull;
import org.slf4j.MDC;
import org.springframework.beans.factory.annotation.Autowired;

public class MdcService implements IMdcService {

  private static final ThreadLocal<Set<String>> keysSet = new ThreadLocal<>();

  @Autowired
  private TasksProperties tasksProperties;

  @Override
  public void put(@NonNull ITask task) {
    put((IBaseTask) task);
    putSubType(task.getSubType());
  }

  @Override
  public void put(@NonNull IBaseTask task) {
    put(task.getId(), task.getVersion());
    putType(task.getType());
  }

  @Override
  public void put(UUID taskId, Long taskVersion) {
    put(taskId);
    put0(tasksProperties.getMdc().getTaskVersionKey(), String.valueOf(taskVersion));
  }

  @Override
  public void put(UUID taskId) {
    put0(tasksProperties.getMdc().getTaskIdKey(), taskId == null ? null : taskId.toString());
  }

  @Override
  public void putType(String type) {
    put0(tasksProperties.getMdc().getTaskTypeKey(), type);
  }

  @Override
  public void putSubType(String subType) {
    put0(tasksProperties.getMdc().getTaskSubTypeKey(), subType);
  }

  protected void put0(String key, String value) {
    if (TwContext.current().isRoot()) {
      if (value == null) {
        MDC.remove(key);
        getKeysSet().remove(key);
      } else {
        MDC.put(key, value);
        getKeysSet().add(key);
      }
    } else {
      TwContext.putCurrentMdc(key, value);
    }
  }

  @Override
  public void clear() {
    MDC.clear();
  }

  @Override
  public void with(Runnable runnable) {
    with(() -> {
      runnable.run();
      return null;
    });
  }

  @Override
  public <T> T with(Callable<T> callable) {
    Map<String, String> previousMap = MDC.getCopyOfContextMap();
    try {
      return ExceptionUtils.doUnchecked(callable);
    } finally {
      for (String key : getKeysSet()) {
        if (previousMap != null && previousMap.containsKey(key)) {
          MDC.put(key, previousMap.get(key));
        } else {
          MDC.remove(key);
        }
      }
      keysSet.remove();
    }
  }

  protected Set<String> getKeysSet() {
    Set<String> localKeysSet = keysSet.get();

    if (localKeysSet == null) {
      keysSet.set(localKeysSet = new HashSet<>());
    }

    return localKeysSet;
  }
}
