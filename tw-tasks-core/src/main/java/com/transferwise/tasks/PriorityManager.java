package com.transferwise.tasks;

import org.springframework.beans.factory.annotation.Autowired;

public class PriorityManager implements IPriorityManager {

  @Autowired
  private TasksProperties tasksProperties;

  @Override
  public int getHighestPriority() {
    return tasksProperties.getHighestPriority();
  }

  @Override
  public int getLowestPriority() {
    return tasksProperties.getLowestPriority();
  }

  @Override
  public int normalize(Integer priority) {
    if (priority == null) {
      return (getLowestPriority() + 1 - getHighestPriority()) / 2;
    }
    if (priority > getLowestPriority()) {
      return getLowestPriority();
    }
    if (priority < getHighestPriority()) {
      return getHighestPriority();
    }
    return priority;
  }
}
