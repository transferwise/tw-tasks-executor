package com.transferwise.tasks.testapp;

import com.transferwise.tasks.handler.interfaces.ISyncTaskProcessor;
import java.util.Map;
import java.util.UUID;

public interface IResultRegisteringSyncTaskProcessor extends ISyncTaskProcessor {

  void reset();

  Map<UUID, Boolean> getTaskResults();
}
