package com.transferwise.tasks.handler.interfaces;

import com.transferwise.tasks.domain.IBaseTask;

public interface ITaskHandler {
    ITaskProcessor getProcessor(IBaseTask task);

    ITaskRetryPolicy getRetryPolicy(IBaseTask task);

    ITaskConcurrencyPolicy getConcurrencyPolicy(IBaseTask task);

    ITaskProcessingPolicy getProcessingPolicy(IBaseTask task);

    boolean handles(IBaseTask task);
}
