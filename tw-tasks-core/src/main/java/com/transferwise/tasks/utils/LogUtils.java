package com.transferwise.tasks.utils;

import com.transferwise.tasks.domain.ITaskVersionId;

public abstract class LogUtils {

  public static String asParameter(ITaskVersionId taskVersionId) {
    return "'" + taskVersionId.getId() + "-" + taskVersionId.getVersion() + "'";
  }
}
