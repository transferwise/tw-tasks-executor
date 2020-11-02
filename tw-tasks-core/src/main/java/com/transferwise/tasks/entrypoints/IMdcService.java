package com.transferwise.tasks.entrypoints;

import com.transferwise.tasks.domain.IBaseTask;
import com.transferwise.tasks.domain.ITask;
import java.util.UUID;
import java.util.concurrent.Callable;
import lombok.NonNull;

public interface IMdcService {

  void clear();

  void with(Runnable runnable);

  <T> T with(Callable<T> callable);

  void put(@NonNull ITask task);

  void put(@NonNull IBaseTask task);

  void put(UUID taskId, Long taskVersion);

  void put(UUID taskId);

  void putType(String type);

  void putSubType(String subType);
}
