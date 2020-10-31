package com.transferwise.tasks.utils;

import com.transferwise.tasks.domain.ITaskVersionId;

public class LogUtils {

  private LogUtils() {
    throw new AssertionError();
  }

  public static String asParameter(ITaskVersionId taskVersionId) {
    return "'" + taskVersionId.getId() + "-" + taskVersionId.getVersion() + "'";
  }
}
