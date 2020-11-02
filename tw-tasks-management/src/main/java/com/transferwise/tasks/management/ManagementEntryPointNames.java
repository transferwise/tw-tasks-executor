package com.transferwise.tasks.management;

public final class ManagementEntryPointNames {

  public static final String RESUME_IMMEDIATELY = "resumeImmediately";
  public static final String RESUME_ALL_IMMEDIATELY = "resumeAllImmediately";
  public static final String GET_TASKS_IN_ERROR = "getTasksInError";
  public static final String GET_TASKS_STUCK = "getTasksStuck";
  public static final String GET_TASKS_IN_PROCESSING_OR_WAITING = "getTasksInProcessingOrWaiting";
  public static final String GET_TASKS_BY_ID = "getTasksById";
  public static final String MARK_AS_FAILED = "markAsFailed";
  public static final String GET_TASK_WITHOUT_DATA = "getTaskWithoutData";
  public static final String GET_TASK_DATA = "getTaskData";

  private ManagementEntryPointNames() {
    throw new AssertionError();
  }
}
