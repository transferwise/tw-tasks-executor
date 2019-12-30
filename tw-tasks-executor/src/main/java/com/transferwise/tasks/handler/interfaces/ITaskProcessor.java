package com.transferwise.tasks.handler.interfaces;

public interface ITaskProcessor {
    default boolean doLogErrors() {
        return true;
    }
}
