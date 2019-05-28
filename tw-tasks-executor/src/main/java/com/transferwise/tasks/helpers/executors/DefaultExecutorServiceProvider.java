package com.transferwise.tasks.helpers.executors;

import com.transferwise.common.baseutils.concurrency.CountingThreadFactory;
import com.transferwise.common.baseutils.concurrency.ScheduledTaskExecutor;
import com.transferwise.common.baseutils.concurrency.SimpleScheduledTaskExecutor;
import com.transferwise.tasks.config.IExecutorServicesProvider;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class DefaultExecutorServiceProvider implements IExecutorServicesProvider {
    private ExecutorService executorService;
    private ScheduledTaskExecutor scheduledTaskExecutor;

    @PostConstruct
    public void init() {
        executorService = Executors.newCachedThreadPool(new CountingThreadFactory("tw-tasks"));
        scheduledTaskExecutor = new SimpleScheduledTaskExecutor(null, executorService);
        scheduledTaskExecutor.start();
    }

    @PreDestroy
    public void destory() {
        scheduledTaskExecutor.stop();
        executorService.shutdown();
    }

    @Override
    public ExecutorService getGlobalExecutorService() {
        return executorService;
    }

    @Override
    public ScheduledTaskExecutor getGlobalScheduledTaskExecutor() {
        return scheduledTaskExecutor;
    }
}
