package com.transferwise.tasks;

import org.springframework.beans.factory.annotation.Autowired;

public class PriorityManager implements IPriorityManager {

  @Autowired
  private TasksProperties tasksProperties;

  @Override
  public int getMinPriority() {
    return tasksProperties.getMinPriority();
  }

  @Override
  public int getMaxPriority() {
    return tasksProperties.getMaxPriority();
  }

  @Override
  public int normalize(Integer priority) {
    if (priority == null) {
      return (getMaxPriority() + 1 - getMinPriority()) / 2;
    }
    if (priority > getMaxPriority()) {
      return getMaxPriority();
    }
    if (priority < getMinPriority()) {
      return getMinPriority();
    }
    return priority;
  }
}
