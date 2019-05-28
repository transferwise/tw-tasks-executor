package com.transferwise.tasks.handler.interfaces;

import com.transferwise.tasks.domain.IBaseTask;

public interface ITaskHandlerRegistry {
    ITaskHandler getTaskHandler(IBaseTask task);
}
