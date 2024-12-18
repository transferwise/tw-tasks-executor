package com.transferwise.tasks.helpers;

import com.transferwise.tasks.domain.TaskStatus;
import com.transferwise.tasks.handler.interfaces.StuckDetectionSource;
import com.transferwise.tasks.processing.TasksProcessingService.ProcessTaskResponse;
import java.time.Instant;
import java.time.ZonedDateTime;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Supplier;
import javax.annotation.Nonnull;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.producer.Producer;

public interface ICoreMetricsTemplate {

  String METRIC_PREFIX = "twTasks.";

  void registerTaskMarkedAsError(String bucketId, String taskType);

  void registerTaskProcessingStart(String bucketId, String taskType);

  void registerTaskProcessingEnd(String bucketId, String type, long processingStartTimeMs, String processingResult);

  void registerFailedTaskGrabbing(String bucketId, String taskType);

  void registerTaskRetryOnError(String bucketId, String taskType);

  void registerTaskRetry(String bucketId, String taskType);

  void registerTaskResuming(String bucketId, String taskType);

  void registerTaskMarkedAsFailed(String bucketId, String taskType);

  void registerTaskRescheduled(String bucketId, String taskType);

  void registerDuplicateTask(String taskType, boolean expected);

  void registerScheduledTaskResuming(String taskType);

  void registerStuckTaskMarkedAsFailed(@Nonnull String taskType, @Nonnull StuckDetectionSource stuckDetectionSource);

  void registerStuckTaskAsIgnored(@Nonnull String taskType, @Nonnull StuckDetectionSource stuckDetectionSource);

  void registerStuckTaskResuming(@Nonnull String taskType, @Nonnull StuckDetectionSource stuckDetectionSource);

  void registerStuckTaskMarkedAsError(@Nonnull String taskType, @Nonnull StuckDetectionSource stuckDetectionSource);

  void registerFailedStatusChange(String taskType, String fromStatus, TaskStatus toStatus);

  void registerFailedNextEventTimeChange(String taskType, ZonedDateTime fromNextEventTime, ZonedDateTime toNextEventTime);

  void registerTaskGrabbingResponse(String bucketId, String type, int priority, ProcessTaskResponse processTaskResponse, Instant taskTriggeredAt);

  void debugPriorityQueueCheck(String bucketId, int priority);

  void debugRoomMapAlreadyHasType(String bucketId, int priority, String taskType);

  void debugTaskTriggeringQueueEmpty(String bucketId, int priority, String taskType);

  void registerTaskAdding(String type, String key, boolean inserted, ZonedDateTime runAfterTime, byte[] data);

  Object registerBucketsCount(Supplier<Integer> supplier);

  void registerLibrary();

  void registerInProgressTriggeringsCount(AtomicInteger count);

  void registerActiveTriggeringsCount(AtomicInteger count);

  void registerOngoingTasksGrabbingsCount(AtomicInteger count);

  void registerPollingBucketsCount(AtomicInteger count);

  void registerTasksCleanerTasksDeletion(TaskStatus status, int deletableTasksCount, int deletedTasksCount, int deletedUniqueKeysCount,
      int deletedTaskDatasCount);

  void registerDaoTaskDataSerialization(String taskType, int originalSize, int serializedSize);

  void registerKafkaTasksExecutionTriggererTriggersReceive(String bucketId);

  void registerKafkaTasksExecutionTriggererCommit(String bucketId, boolean sync, boolean success);

  void registerKafkaTasksExecutionTriggererAlreadyCommitedOffset(String bucketId);

  Object registerTasksCleanerDeleteLagSeconds(TaskStatus status, AtomicLong lagInSeconds);

  void unregisterMetric(Object metricHandle);

  Object registerTasksInErrorCount(AtomicInteger erroneousTasksCount);

  Object registerTasksInErrorCount(String taskType, AtomicInteger count);

  Object registerStuckTasksCount(AtomicInteger stuckTasksCount);

  Object registerStuckTasksCount(TaskStatus status, String type, AtomicInteger count);

  Object registerApproximateTasksCount(AtomicLong approximateTasksCount);

  Object registerApproximateUniqueKeysCount(AtomicLong approximateUniqueKeysCount);

  Object registerApproximateTaskDatasCount(AtomicLong approximateTaskDatasCount);

  Object registerTaskHistoryLength(TaskStatus status, AtomicLong lengthInSeconds);

  Object registerProcessingTriggersCount(String bucketId, String taskType, Supplier<Number> countSupplier);

  Object registerProcessingTriggersCount(String bucketId, Supplier<Number> countSupplier);

  Object registerRunningTasksCount(String bucketId, Supplier<Number> countSupplier);

  Object registerInProgressTasksGrabbingCount(String bucketId, Supplier<Number> countSupplier);

  Object registerProcessingStateVersion(String bucketId, Supplier<Number> countSupplier);

  Object registerKafkaTasksExecutionTriggererOffsetsToBeCommitedCount(String bucketId, Supplier<Number> countSupplier);

  Object registerKafkaTasksExecutionTriggererOffsetsCompletedCount(String bucketId, Supplier<Number> countSupplier);

  Object registerKafkaTasksExecutionTriggererUnprocessedFetchedRecordsCount(String bucketId, Supplier<Number> countSupplier);

  Object registerKafkaTasksExecutionTriggererOffsetsCount(String bucketId, Supplier<Number> countSupplier);

  @SuppressWarnings("rawtypes")
  AutoCloseable registerKafkaConsumer(Consumer consumer);

  @SuppressWarnings("rawtypes")
  void registerKafkaProducer(Producer producer);
}
