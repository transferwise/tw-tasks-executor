package com.transferwise.tasks.handler.interfaces;

import com.newrelic.api.agent.NewRelic;
import com.transferwise.tasks.TasksProperties;
import com.transferwise.tasks.domain.ITask;
import com.transferwise.tasks.utils.LogUtils;
import lombok.AllArgsConstructor;
import lombok.Getter;
import org.slf4j.Logger;

public interface ITaskProcessor {

    /**
     * Called when an error has occured.
     * @param ctx the execution context, never null
     * @param t the throwable
     */
    default void onError(TaskProcessorContext ctx, Throwable t) {
        ITask task = ctx.getTask();
        ctx.getLogger().error("Processing task {} type: '{}' subType: '{}' failed.",
            LogUtils.asParameter(task.getVersionId()), task.getType(), task.getSubType(), t);
    }

    /**
     * Called immediately before the task execution. Since this method is invoked in the same thread,
     * it should not be a blocking method.
     * @param ctx the execution context, never null
     */
    default void preProcess(TaskProcessorContext ctx) {
        ITask task = ctx.getTask();
        NewRelic.addCustomParameter(ctx.getTasksProperties().getTwTaskVersionIdMdcKey(), String.valueOf(task.getVersionId()));
        NewRelic.addCustomParameter("twTaskType", task.getType());
        NewRelic.addCustomParameter("twTaskSubType", task.getSubType());
        NewRelic.setTransactionName("TwTasks", "Processing_" + task.getType());
    }

    @Getter
    @AllArgsConstructor
    class TaskProcessorContext {
        private final ITask task;
        private final String bucketId;
        private final TasksProperties tasksProperties;
        private final Logger logger;
    }
}
