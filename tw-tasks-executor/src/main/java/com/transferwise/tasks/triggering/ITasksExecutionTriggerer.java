package com.transferwise.tasks.triggering;

import com.transferwise.tasks.domain.BaseTask;

public interface ITasksExecutionTriggerer {

    void start();

    boolean isStarted();

    void trigger(BaseTask task);
}
