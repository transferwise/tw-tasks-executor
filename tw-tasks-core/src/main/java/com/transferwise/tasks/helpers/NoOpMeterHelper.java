package com.transferwise.tasks.helpers;

import com.transferwise.tasks.domain.TaskStatus;
import com.transferwise.tasks.handler.interfaces.StuckDetectionSource;
import com.transferwise.tasks.processing.TasksProcessingService.ProcessTaskResponse;
import java.time.ZonedDateTime;
import java.util.Map;
import java.util.function.Supplier;
import javax.annotation.Nonnull;

public class NoOpMeterHelper implements IMeterHelper {

  @Override
  public void registerTaskMarkedAsError(String bucketId, String taskType) {

  }

  @Override
  public void registerTaskProcessingStart(String bucketId, String taskType) {

  }

  @Override
  public void registerFailedTaskGrabbing(String bucketId, String taskType) {

  }

  @Override
  public void registerTaskRetryOnError(String bucketId, String taskType) {

  }

  @Override
  public void registerTaskRetry(String bucketId, String taskType) {

  }

  @Override
  public void registerTaskResuming(String bucketId, String taskType) {

  }

  @Override
  public void registerTaskMarkedAsFailed(String bucketId, String taskType) {

  }

  @Override
  public Object registerGauge(String name, Map<String, String> tags, Supplier<Number> valueSupplier) {
    return null;
  }

  @Override
  public void unregisterMetric(Object handle) {

  }

  @Override
  public void incrementCounter(String name, Map<String, String> tags, long delta) {

  }

  @Override
  public void registerTaskProcessingEnd(String bucketId, String type, long processingStartTimeMs, String processingResult) {

  }

  @Override
  public void registerKafkaCoreMessageProcessing(int shard, String topic) {

  }

  @Override
  public void registerDuplicateTask(String taskType, boolean expected) {

  }

  @Override
  public void registerScheduledTaskResuming(String taskType) {

  }

  @Override
  public void registerStuckTaskMarkedAsFailed(@Nonnull String taskType, @Nonnull StuckDetectionSource stuckDetectionSource) {

  }

  @Override
  public void registerStuckTaskAsIgnored(@Nonnull String taskType, @Nonnull StuckDetectionSource stuckDetectionSource) {

  }

  @Override
  public void registerStuckTaskResuming(@Nonnull String taskType, @Nonnull StuckDetectionSource stuckDetectionSource) {

  }

  @Override
  public void registerStuckTaskMarkedAsError(@Nonnull String taskType, @Nonnull StuckDetectionSource stuckDetectionSource) {

  }

  @Override
  public void registerFailedStatusChange(String taskType, String fromStatus, TaskStatus toStatus) {

  }

  @Override
  public void registerTaskGrabbingResponse(String bucketId, String type, int priority, ProcessTaskResponse processTaskResponse) {

  }

  @Override
  public void debugPriorityQueueCheck(String bucketId, int priority) {

  }

  @Override
  public void debugRoomMapAlreadyHasType(String bucketId, int priority, String taskType) {

  }

  @Override
  public void debugTaskTriggeringQueueEmpty(String bucketId, int priority, String taskType) {

  }

  @Override
  public void registerTaskAdding(String type, String key, boolean inserted, ZonedDateTime runAfterTime, byte[] data) {

  }

  @Override
  public void registerLibrary() {
  }
}
