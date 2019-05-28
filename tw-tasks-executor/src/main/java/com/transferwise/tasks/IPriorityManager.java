package com.transferwise.tasks;

public interface IPriorityManager {
    int getMinPriority();

    int getMaxPriority();

    int normalize(Integer priority);
}
