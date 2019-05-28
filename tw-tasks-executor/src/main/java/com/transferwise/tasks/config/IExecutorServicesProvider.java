package com.transferwise.tasks.config;

import com.transferwise.common.baseutils.concurrency.ScheduledTaskExecutor;

import java.util.concurrent.ExecutorService;

public interface IExecutorServicesProvider {
    //TODO: Everything should be using that.

    /**
     * Should not be bounded.
     */
    ExecutorService getGlobalExecutorService();

    /**
     * Should always have free thread for tw-task or background jobs may start lagging.
     */
    ScheduledTaskExecutor getGlobalScheduledTaskExecutor();
}
