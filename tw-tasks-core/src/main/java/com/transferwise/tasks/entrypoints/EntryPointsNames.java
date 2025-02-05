package com.transferwise.tasks.entrypoints;

public final class EntryPointsNames {

  public static final String PROCESSING = "Processing_";

  public static final String GRAB_TASK = "grabTask";
  public static final String TRIGGER_TASK = "triggerTask";
  public static final String CLEAN_OLD_TAKS = "cleanOldTasks";
  public static final String RESUME_STUCK_TASKS = "resumeStuckTasks";
  public static final String RESUME_WAITING_TASKS = "resumeWaitingTasks";
  public static final String RESUME_TASKS_FOR_CLIENT = "resumeTasksForClient";
  public static final String MONITOR_CHECK = "monitorCheck";
  public static final String MONITOR_RESET = "monitorReset";
  public static final String ADD_TASK = "addTask";
  public static final String RESUME_TASK = "resumeTask";
  public static final String ASYNC_HANDLE_SUCCESS = "asyncHandleSuccess";
  public static final String ASYNC_HANDLE_FAIL = "asyncHandleFail";
  public static final String RESCHEDULE_TASK = "rescheduleTask";
  public static final String DELETE_TASK = "deleteTask";
  public static final String GET_TASK = "getTask";

  private EntryPointsNames() {
    throw new AssertionError();
  }

}
