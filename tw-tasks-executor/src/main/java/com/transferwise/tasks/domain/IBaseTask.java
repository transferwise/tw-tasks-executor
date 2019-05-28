package com.transferwise.tasks.domain;

public interface IBaseTask {
    ITaskVersionId getVersionId();

    @SuppressWarnings("EmptyMethod")
    String getType();

    int getPriority();
}
