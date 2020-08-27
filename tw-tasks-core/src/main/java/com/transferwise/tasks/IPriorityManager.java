package com.transferwise.tasks;

public interface IPriorityManager {

  int getHighestPriority();

  int getLowestPriority();

  int normalize(Integer priority);
}
