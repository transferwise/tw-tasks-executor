package com.transferwise.tasks.handler.interfaces;

import com.transferwise.tasks.domain.ITask;

import java.util.function.Consumer;

public interface IAsyncTaskProcessor extends ITaskProcessor {
    void process(ITask task, Runnable successCallback, Consumer<Throwable> errorCallback);
}
