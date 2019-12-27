package com.transferwise.tasks.handler.interfaces;

public interface ITaskProcessor {
    default boolean logErrors() {
        return true;
    }
}
