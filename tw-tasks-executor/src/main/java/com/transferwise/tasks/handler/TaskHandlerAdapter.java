package com.transferwise.tasks.handler;

import com.transferwise.tasks.domain.IBaseTask;
import com.transferwise.tasks.handler.interfaces.ITaskConcurrencyPolicy;
import com.transferwise.tasks.handler.interfaces.ITaskHandler;
import com.transferwise.tasks.handler.interfaces.ITaskProcessingPolicy;
import com.transferwise.tasks.handler.interfaces.ITaskProcessor;
import com.transferwise.tasks.handler.interfaces.ITaskRetryPolicy;
import lombok.Getter;
import lombok.Setter;
import lombok.experimental.Accessors;

import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.function.Predicate;

@Getter
@Setter
@Accessors(chain = true)
public class TaskHandlerAdapter implements ITaskHandler {
    private ITaskProcessor processor;
    private ITaskRetryPolicy retryPolicy;
    private ITaskConcurrencyPolicy concurrencyPolicy;
    private ITaskProcessingPolicy processingPolicy;

    private Predicate<IBaseTask> handlesPredicate;

    public TaskHandlerAdapter() {
    }

    public TaskHandlerAdapter(Predicate<IBaseTask> handlesPredicate, ITaskProcessor processor) {
        this.handlesPredicate = handlesPredicate;
        this.processor = processor;
        this.retryPolicy = new NoRetryPolicy();
        this.concurrencyPolicy = new SimpleTaskConcurrencyPolicy(1);
        this.processingPolicy = new SimpleTaskProcessingPolicy().setMaxProcessingDuration(Duration.of(1, ChronoUnit.HOURS));
    }

    @Override
    public ITaskProcessor getProcessor(IBaseTask task) {
        return processor;
    }

    @Override
    public ITaskRetryPolicy getRetryPolicy(IBaseTask task) {
        return retryPolicy;
    }

    @Override
    public ITaskConcurrencyPolicy getConcurrencyPolicy(IBaseTask task) {
        return concurrencyPolicy;
    }

    @Override
    public ITaskProcessingPolicy getProcessingPolicy(IBaseTask task) {
        return processingPolicy;
    }

    @Override
    public boolean handles(IBaseTask task) {
        return handlesPredicate.test(task);
    }
}
