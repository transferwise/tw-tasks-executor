package com.transferwise.tasks.processing;

import com.transferwise.tasks.domain.Task;

public interface ITaskProcessingInterceptor {
    void doProcess(Task task, Runnable processor);
}
