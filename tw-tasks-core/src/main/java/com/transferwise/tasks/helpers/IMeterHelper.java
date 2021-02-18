package com.transferwise.tasks.helpers;

import com.transferwise.tasks.domain.TaskStatus;
import com.transferwise.tasks.handler.interfaces.StuckDetectionSource;
import com.transferwise.tasks.processing.TasksProcessingService.ProcessTaskResponse;
import java.time.ZonedDateTime;
import java.util.Map;
import java.util.function.Supplier;
import javax.annotation.Nonnull;

public interface IMeterHelper {

  String METRIC_PREFIX = "twTasks.";

  void registerTaskMarkedAsError(String bucketId, String taskType);

  void registerTaskProcessingStart(String bucketId, String taskType);

  void registerFailedTaskGrabbing(String bucketId, String taskType);

  void registerTaskRetryOnError(String bucketId, String taskType);

  void registerTaskRetry(String bucketId, String taskType);

  void registerTaskResuming(String bucketId, String taskType);

  void registerTaskMarkedAsFailed(String bucketId, String taskType);

  default Object registerGauge(String name, Supplier<Number> valueSupplier) {
    return registerGauge(name, null, valueSupplier);
  }

  Object registerGauge(String name, Map<String, String> tags, Supplier<Number> valueSupplier);

  void unregisterMetric(Object handle);

  default void incrementCounter(String name, long delta) {
    incrementCounter(name, null, delta);
  }

  void incrementCounter(String name, Map<String, String> tags, long delta);

  void registerTaskProcessingEnd(String bucketId, String type, long processingStartTimeMs, String processingResult);

  void registerKafkaCoreMessageProcessing(int shard, String topic);

  void registerDuplicateTask(String taskType, boolean expected);

  void registerScheduledTaskResuming(String taskType);

  void registerStuckTaskMarkedAsFailed(@Nonnull String taskType, @Nonnull StuckDetectionSource stuckDetectionSource);

  void registerStuckTaskAsIgnored(@Nonnull String taskType, @Nonnull StuckDetectionSource stuckDetectionSource);

  void registerStuckTaskResuming(@Nonnull String taskType, @Nonnull StuckDetectionSource stuckDetectionSource);

  void registerStuckTaskMarkedAsError(@Nonnull String taskType, @Nonnull StuckDetectionSource stuckDetectionSource);

  void registerFailedStatusChange(String taskType, String fromStatus, TaskStatus toStatus);

  void registerTaskGrabbingResponse(String bucketId, String type, int priority, ProcessTaskResponse processTaskResponse);

  void debugPriorityQueueCheck(String bucketId, int priority);

  void debugRoomMapAlreadyHasType(String bucketId, int priority, String taskType);

  void debugTaskTriggeringQueueEmpty(String bucketId, int priority, String taskType);

  void registerTaskAdding(String type, String key, boolean inserted, ZonedDateTime runAfterTime, byte[] data);

  void registerLibrary();
}
