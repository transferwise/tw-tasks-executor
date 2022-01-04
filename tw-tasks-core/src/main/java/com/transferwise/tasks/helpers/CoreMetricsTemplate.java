package com.transferwise.tasks.helpers;

import com.transferwise.common.baseutils.meters.cache.IMeterCache;
import com.transferwise.common.baseutils.meters.cache.TagsSet;
import com.transferwise.common.context.TwContextClockHolder;
import com.transferwise.tasks.domain.TaskStatus;
import com.transferwise.tasks.handler.interfaces.StuckDetectionSource;
import com.transferwise.tasks.processing.TasksProcessingService.ProcessTaskResponse;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.ImmutableTag;
import io.micrometer.core.instrument.Meter;
import io.micrometer.core.instrument.Tags;
import io.micrometer.core.instrument.binder.kafka.KafkaClientMetrics;
import java.time.ZonedDateTime;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Supplier;
import javax.annotation.Nonnull;
import lombok.Data;
import lombok.experimental.Accessors;
import org.apache.commons.lang3.tuple.Triple;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.producer.Producer;
import org.springframework.beans.factory.annotation.Autowired;

public class CoreMetricsTemplate implements ICoreMetricsTemplate {

  private static final String METRIC_LIBRARY_INFO = "tw.library.info";

  private static final String METRIC_TASKS_MARKED_AS_ERROR_COUNT = METRIC_PREFIX + "tasks.markedAsErrorCount";
  private static final String METRIC_TASKS_PROCESSINGS_COUNT = METRIC_PREFIX + "tasks.processingsCount";
  private static final String METRIC_TASKS_ONGOING_PROCESSINGS_COUNT = METRIC_PREFIX + "tasks.ongoingProcessingsCount";
  private static final String METRIC_TASKS_PROCESSED_COUNT = METRIC_PREFIX + "tasks.processedCount";
  private static final String METRIC_TASKS_PROCESSING_TIME = METRIC_PREFIX + "tasks.processingTime";
  private static final String METRIC_TASKS_FAILED_STATUS_CHANGE_COUNT = METRIC_PREFIX + "tasks.failedStatusChangeCount";
  private static final String METRIC_TASKS_DEBUG_PRIORITY_QUEUE_CHECK = METRIC_PREFIX + "tasks.debug.priorityQueueCheck";
  private static final String METRIC_TASKS_TASK_GRABBING = METRIC_PREFIX + "tasks.taskGrabbing";
  private static final String METRIC_TASKS_DEBUG_ROOM_MAP_ALREADY_HAS_TYPE = METRIC_PREFIX + "tasks.debug.roomMapAlreadyHasType";
  private static final String METRIC_TASKS_DEBUG_TASK_TRIGGERING_QUEUE_EMPTY = METRIC_PREFIX + "tasks.debug.taskTriggeringQueueEmpty";
  private static final String METRIC_TASKS_DUPLICATES_COUNT = METRIC_PREFIX + "tasks.duplicatesCount";
  private static final String METRIC_TASKS_RESUMER_SCHEDULED_TASKS_RESUMED_COUNT = METRIC_PREFIX + "tasksResumer.scheduledTasks.resumedCount";
  private static final String METRIC_TASKS_RESUMER_STUCK_TASKS_MARK_FAILED_COUNT = METRIC_PREFIX + "tasksResumer.stuckTasks.markFailedCount";
  private static final String METRIC_TASKS_RESUMER_STUCK_TASKS_IGNORED_COUNT = METRIC_PREFIX + "tasksResumer.stuckTasks.ignoredCount";
  private static final String METRIC_TASKS_RESUMER_STUCK_TASKS_RESUMED_COUNT = METRIC_PREFIX + "tasksResumer.stuckTasks.resumedCount";
  private static final String METRIC_TASKS_RESUMER_STUCK_TASKS_MARK_ERROR_COUNT = METRIC_PREFIX + "tasksResumer.stuckTasks.markErrorCount";
  private static final String METRIC_TASKS_FAILED_GRABBINGS_COUNT = METRIC_PREFIX + "tasks.failedGrabbingsCount";
  private static final String METRIC_TASKS_RETRIES_COUNT = METRIC_PREFIX + "tasks.retriesCount";
  private static final String METRIC_TASKS_RESUMINGS_COUNT = METRIC_PREFIX + "tasks.resumingsCount";
  private static final String METRIC_TASKS_MARKED_AS_FAILED_COUNT = METRIC_PREFIX + "tasks.markedAsFailedCount";
  private static final String METRIC_TASKS_ADDINGS_COUNT = METRIC_PREFIX + "task.addings.count";
  private static final String METRIC_TASKS_SERVICE_IN_PROGRESS_TRIGGERINGS_COUNT = METRIC_PREFIX + "tasksService.inProgressTriggeringsCount";
  private static final String METRIC_TASKS_SERVICE_ACTIVE_TRIGGERINGS_COUNT = METRIC_PREFIX + "tasksService.activeTriggeringsCount";
  private static final String METRIC_BUCKETS_MANAGER_BUCKETS_COUNT = METRIC_PREFIX + "bucketsManager.bucketsCount";
  private static final String METRIC_PROCESSING_ONGOING_TASKS_GRABBINGS_COUNT = METRIC_PREFIX + "processing.ongoingTasksGrabbingsCount";
  private static final String METRIC_TASKS_CLEANER_DELETABLE_TASKS_COUNT = METRIC_PREFIX + "tasksCleaner.deletableTasksCount";
  private static final String METRIC_TASKS_CLEANER_DELETED_TASKS_COUNT = METRIC_PREFIX + "tasksCleaner.deletedTasksCount";
  private static final String METRIC_TASKS_CLEANER_DELETED_UNIQUE_KEYS_COUNT = METRIC_PREFIX + "tasksCleaner.deletedUniqueKeysCount";
  private static final String METRIC_TASKS_CLEANER_DELETED_TASK_DATAS_COUNT = METRIC_PREFIX + "tasksCleaner.deletedTaskDatasCount";
  private static final String METRIC_TASKS_CLEANER_DELETE_LAG_SECONDS = METRIC_PREFIX + "tasksCleaner.deleteLagSeconds";
  private static final String METRIC_DAO_DATA_SIZE = METRIC_PREFIX + "dao.data.size";
  private static final String METRIC_DAO_DATA_SERIALIZED_SIZE = METRIC_PREFIX + "dao.data.serialized.size";
  private static final String METRIC_KAFKA_TASKS_EXECUTION_TRIGGERER_POLLING_BUCKETS_COUNT =
      METRIC_PREFIX + "kafkaTasksExecutionTriggerer.pollingBucketsCount";
  private static final String METRIC_KAFKA_TASKS_EXECUTION_TRIGGERER_RECEIVED_TRIGGERS_COUNT =
      METRIC_PREFIX + "kafkaTasksExecutionTriggerer.receivedTriggersCount";
  private static final String METRIC_KAFKA_TASKS_EXECUTION_TRIGGERER_COMMITS_COUNT = METRIC_PREFIX + "kafkaTasksExecutionTriggerer.commitsCount";
  private static final String METRIC_KAFKA_TASKS_EXECUTION_TRIGGERER_FAILED_COMMITS_COUNT =
      METRIC_PREFIX + "kafkaTasksExecutionTriggerer.failedCommitsCount";
  private static final String METRIC_KAFKA_TASKS_EXECUTION_TRIGGERER_OFFSET_ALREADY_COMMITTED =
      METRIC_PREFIX + "kafkaTasksExecutionTriggerer.offsetAlreadyCommitted";
  private static final String METRIC_KAFKA_TASKS_EXECUTION_TRIGGERER_OFFSETS_TO_BE_COMMITED_COUNT =
      METRIC_PREFIX + "kafkaTasksExecutionTriggerer.offsetsToBeCommitedCount";
  private static final String METRIC_KAFKA_TASKS_EXECUTION_TRIGGERER_OFFSETS_COMPLETED_COUNT =
      METRIC_PREFIX + "kafkaTasksExecutionTriggerer.offsetsCompletedCount";
  private static final String METRIC_KAFKA_TASKS_EXECUTION_TRIGGERER_UNPROCESSED_FETCHED_RECORDS_COUNT =
      METRIC_PREFIX + "kafkaTasksExecutionTriggerer.unprocessedFetchedRecordsCount";
  private static final String METRIC_KAFKA_TASKS_EXECUTION_TRIGGERER_OFFSETS_COUNT =
      METRIC_PREFIX + "kafkaTasksExecutionTriggerer.offsetsCount";
  private static final String METRIC_HEALTH_TASKS_IN_ERROR_COUNT = METRIC_PREFIX + "health.tasksInErrorCount";
  private static final String METRIC_HEALTH_STUCK_TASKS_COUNT = METRIC_PREFIX + "health.stuckTasksCount";
  private static final String METRIC_HEALTH_TASK_HISTORY_LENGTH_SECONDS = METRIC_PREFIX + "health.tasksHistoryLengthSeconds";
  private static final String METRIC_HEALTH_TASKS_IN_ERROR_COUNT_PER_TYPE = METRIC_PREFIX + "health.tasksInErrorCountPerType";
  private static final String METRIC_HEALTH_STUCK_TASKS_COUNT_PER_TYPE = METRIC_PREFIX + "health.stuckTasksCountPerType";
  private static final String METRIC_STATE_APPROXIMATE_TASKS = METRIC_PREFIX + "state.approximateTasks";
  private static final String METRIC_STATE_APPROXIMATE_UNIQUE_KEYS = METRIC_PREFIX + "state.approximateUniqueKeys";
  private static final String METRIC_STATE_APPROXIMATE_TASK_DATAS = METRIC_PREFIX + "state.approximateTaskDatas";
  private static final String METRIC_PROCESSING_TYPE_TRIGGERS_COUNT = METRIC_PREFIX + "processing.typeTriggersCount";
  private static final String METRIC_PROCESSING_RUNNING_TASKS_COUNT = METRIC_PREFIX + "processing.runningTasksCount";
  private static final String METRIC_PROCESSING_IN_PROGRESS_TASKS_GRABBING_COUNT = METRIC_PREFIX + "processing.inProgressTasksGrabbingCount";
  private static final String METRIC_PROCESSING_TRIGGERS_COUNT = METRIC_PREFIX + "processing.triggersCount";
  private static final String METRIC_PROCESSING_STATE_VERSION = METRIC_PREFIX + "processing.stateVersion";

  private static final String TAG_PROCESSING_RESULT = "processingResult";
  private static final String TAG_FROM_STATUS = "fromStatus";
  private static final String TAG_TO_STATUS = "toStatus";
  private static final String TAG_TASK_STATUS = "taskStatus";
  private static final String TAG_BUCKET_ID = "bucketId";
  private static final String TAG_TASK_TYPE = "taskType";
  private static final String TAG_SOURCE = "source";
  private static final String TAG_REASON = "reason";
  private static final String TAG_PRIORITY = "priority";
  private static final String TAG_GRABBING_RESPONSE = "grabbingResponse";
  private static final String TAG_GRABBING_CODE = "grabbingCode";
  private static final String TAG_HAS_KEY = "hasKey";
  private static final String TAG_IS_DUPLICATE = "isDuplicate";
  private static final String TAG_IS_SCHEDULED = "isScheduled";
  private static final String TAG_DATA_SIZE = "dataSize";
  private static final String TAG_EXPECTED = "expected";

  private static final String[] DATA_SIZE_BUCKET_VALUES = {"64", "256", "1024", "4096", "16384", "65536"};
  private static final int[] DATA_SIZE_BUCKETS = {64, 256, 1024, 4096, 16384, 65536};

  @Autowired
  private IMeterCache meterCache;

  private final AtomicInteger kafkaClientId = new AtomicInteger();

  private final Map<Triple<String, String, String>, AtomicInteger> taskProcessingGauges = new ConcurrentHashMap<>();

  @Override
  public void registerTaskMarkedAsError(String bucketId, String taskType) {
    meterCache.counter(METRIC_TASKS_MARKED_AS_ERROR_COUNT, TagsSet.of(TAG_BUCKET_ID, resolveBucketId(bucketId), TAG_TASK_TYPE, taskType))
        .increment();
  }

  @Override
  public void registerTaskProcessingStart(String bucketId, String taskType) {
    String resolvedBucketId = resolveBucketId(bucketId);
    meterCache.counter(METRIC_TASKS_PROCESSINGS_COUNT, TagsSet.of(TAG_BUCKET_ID, resolvedBucketId, TAG_TASK_TYPE, taskType))
        .increment();

    taskProcessingGauges.computeIfAbsent(Triple.of(METRIC_TASKS_ONGOING_PROCESSINGS_COUNT, resolvedBucketId, taskType), (t) -> {
      AtomicInteger counter = new AtomicInteger(0);
      meterCache.getMeterRegistry()
          .gauge(METRIC_TASKS_ONGOING_PROCESSINGS_COUNT, Tags.of(TAG_BUCKET_ID, resolvedBucketId, TAG_TASK_TYPE, taskType), counter);
      return counter;
    }).incrementAndGet();
  }

  @Override
  public void registerTaskProcessingEnd(String bucketId, String taskType, long processingStartTimeMs, String processingResult) {
    String resolvedBucketId = resolveBucketId(bucketId);
    meterCache.counter(METRIC_TASKS_PROCESSED_COUNT, TagsSet.of(TAG_BUCKET_ID, resolvedBucketId, TAG_TASK_TYPE, taskType,
        TAG_PROCESSING_RESULT, processingResult))
        .increment();
    meterCache.timer(METRIC_TASKS_PROCESSING_TIME, TagsSet.of(TAG_BUCKET_ID, resolvedBucketId, TAG_TASK_TYPE, taskType,
        TAG_PROCESSING_RESULT, processingResult))
        .record(TwContextClockHolder.getClock().millis() - processingStartTimeMs, TimeUnit.MILLISECONDS);
    taskProcessingGauges.get(Triple.of(METRIC_TASKS_ONGOING_PROCESSINGS_COUNT, resolvedBucketId, taskType))
        .decrementAndGet();
  }

  @Override
  public void registerFailedStatusChange(String taskType, String fromStatus, TaskStatus toStatus) {
    meterCache.counter(METRIC_TASKS_FAILED_STATUS_CHANGE_COUNT, TagsSet.of(TAG_TASK_TYPE, taskType,
        TAG_FROM_STATUS, fromStatus, TAG_TO_STATUS, toStatus.name()))
        .increment();
  }

  @Override
  public void registerTaskGrabbingResponse(String bucketId, String taskType, int priority, ProcessTaskResponse processTaskResponse) {
    meterCache.counter(METRIC_TASKS_TASK_GRABBING, TagsSet.of(TAG_TASK_TYPE, taskType,
        TAG_BUCKET_ID, bucketId, TAG_PRIORITY, String.valueOf(priority), TAG_GRABBING_RESPONSE, processTaskResponse.getResult().name(),
        TAG_GRABBING_CODE, processTaskResponse.getCode() == null ? "UNKNOWN" : processTaskResponse.getCode().name()))
        .increment();
  }

  @Override
  public void debugPriorityQueueCheck(String bucketId, int priority) {
    meterCache.counter(METRIC_TASKS_DEBUG_PRIORITY_QUEUE_CHECK, TagsSet.of(TAG_BUCKET_ID, bucketId, TAG_PRIORITY, String.valueOf(priority)))
        .increment();
  }

  @Override
  public void debugRoomMapAlreadyHasType(String bucketId, int priority, String taskType) {
    meterCache.counter(METRIC_TASKS_DEBUG_ROOM_MAP_ALREADY_HAS_TYPE, TagsSet.of(TAG_BUCKET_ID, bucketId, TAG_PRIORITY, String.valueOf(priority),
        TAG_TASK_TYPE, taskType))
        .increment();
  }

  @Override
  public void debugTaskTriggeringQueueEmpty(String bucketId, int priority, String taskType) {
    meterCache.counter(METRIC_TASKS_DEBUG_TASK_TRIGGERING_QUEUE_EMPTY, TagsSet.of(TAG_BUCKET_ID, bucketId, TAG_PRIORITY, String.valueOf(priority),
        TAG_TASK_TYPE, taskType))
        .increment();
  }

  @Override
  public void registerDuplicateTask(String taskType, boolean expected) {
    meterCache.counter(METRIC_TASKS_DUPLICATES_COUNT, TagsSet.of(TAG_TASK_TYPE, taskType, TAG_EXPECTED, String.valueOf(expected)))
        .increment();
  }

  @Override
  public void registerScheduledTaskResuming(String taskType) {
    meterCache.counter(METRIC_TASKS_RESUMER_SCHEDULED_TASKS_RESUMED_COUNT, TagsSet.of(TAG_TASK_TYPE, taskType))
        .increment();
  }

  @Override
  public void registerStuckTaskMarkedAsFailed(@Nonnull String taskType, @Nonnull StuckDetectionSource stuckDetectionSource) {
    meterCache
        .counter(METRIC_TASKS_RESUMER_STUCK_TASKS_MARK_FAILED_COUNT, TagsSet.of(TAG_TASK_TYPE, taskType, TAG_SOURCE, stuckDetectionSource.name()))
        .increment();
  }

  @Override
  public void registerStuckTaskAsIgnored(@Nonnull String taskType, @Nonnull StuckDetectionSource stuckDetectionSource) {
    meterCache.counter(METRIC_TASKS_RESUMER_STUCK_TASKS_IGNORED_COUNT, TagsSet.of(TAG_TASK_TYPE, taskType, TAG_SOURCE, stuckDetectionSource.name()))
        .increment();
  }

  @Override
  public void registerStuckTaskResuming(@Nonnull String taskType, @Nonnull StuckDetectionSource stuckDetectionSource) {
    meterCache.counter(METRIC_TASKS_RESUMER_STUCK_TASKS_RESUMED_COUNT, TagsSet.of(TAG_TASK_TYPE, taskType, TAG_SOURCE, stuckDetectionSource.name()))
        .increment();
  }

  @Override
  public void registerStuckTaskMarkedAsError(@Nonnull String taskType, @Nonnull StuckDetectionSource stuckDetectionSource) {
    meterCache
        .counter(METRIC_TASKS_RESUMER_STUCK_TASKS_MARK_ERROR_COUNT, TagsSet.of(TAG_TASK_TYPE, taskType, TAG_SOURCE, stuckDetectionSource.name()))
        .increment();
  }

  @Override
  public void registerFailedTaskGrabbing(String bucketId, String taskType) {
    meterCache.counter(METRIC_TASKS_FAILED_GRABBINGS_COUNT, TagsSet.of(TAG_BUCKET_ID, resolveBucketId(bucketId), TAG_TASK_TYPE, taskType))
        .increment();
  }

  @Override
  public void registerTaskRetryOnError(String bucketId, String taskType) {
    meterCache.counter(METRIC_TASKS_RETRIES_COUNT, TagsSet.of(TAG_BUCKET_ID, resolveBucketId(bucketId), TAG_TASK_TYPE, taskType,
        TAG_REASON, "ERROR"))
        .increment();
  }

  @Override
  public void registerTaskRetry(String bucketId, String taskType) {
    meterCache.counter(METRIC_TASKS_RETRIES_COUNT, TagsSet.of(TAG_BUCKET_ID, resolveBucketId(bucketId), TAG_TASK_TYPE, taskType,
        TAG_REASON, "CONTINUE"))
        .increment();
  }

  @Override
  public void registerTaskResuming(String bucketId, String taskType) {
    meterCache.counter(METRIC_TASKS_RESUMINGS_COUNT, TagsSet.of(TAG_BUCKET_ID, resolveBucketId(bucketId), TAG_TASK_TYPE, taskType))
        .increment();
  }

  @Override
  public void registerTaskMarkedAsFailed(String bucketId, String taskType) {
    meterCache.counter(METRIC_TASKS_MARKED_AS_FAILED_COUNT, TagsSet.of(TAG_BUCKET_ID, resolveBucketId(bucketId), TAG_TASK_TYPE, taskType))
        .increment();
  }

  @Override
  public void registerTaskAdding(String type, String key, boolean inserted, ZonedDateTime runAfterTime, byte[] data) {
    meterCache.counter(METRIC_TASKS_ADDINGS_COUNT,
        TagsSet.of(
            TAG_TASK_TYPE, type,
            TAG_HAS_KEY, Boolean.toString(key != null),
            TAG_IS_DUPLICATE, Boolean.toString(!inserted),
            TAG_IS_SCHEDULED, Boolean.toString(runAfterTime != null),
            TAG_DATA_SIZE, getDataSizeBucket(data)
        ))
        .increment();
  }

  @Override
  public void registerInProgressTriggeringsCount(AtomicInteger count) {
    Gauge.builder(METRIC_TASKS_SERVICE_IN_PROGRESS_TRIGGERINGS_COUNT, count::get)
        .register(meterCache.getMeterRegistry());
  }

  @Override
  public void registerActiveTriggeringsCount(AtomicInteger count) {
    Gauge.builder(METRIC_TASKS_SERVICE_ACTIVE_TRIGGERINGS_COUNT, count::get)
        .register(meterCache.getMeterRegistry());
  }

  @Override
  public void registerOngoingTasksGrabbingsCount(AtomicInteger count) {
    Gauge.builder(METRIC_PROCESSING_ONGOING_TASKS_GRABBINGS_COUNT, count::get)
        .register(meterCache.getMeterRegistry());
  }

  @Override
  public void registerPollingBucketsCount(AtomicInteger count) {
    Gauge.builder(METRIC_KAFKA_TASKS_EXECUTION_TRIGGERER_POLLING_BUCKETS_COUNT, count::get)
        .register(meterCache.getMeterRegistry());
  }

  @Override
  public void registerTasksCleanerTasksDeletion(TaskStatus status, int deletableTasksCount, int deletedTasksCount, int deletedUniqueKeysCount,
      int deletedTaskDatasCount) {
    TagsSet tags = TagsSet.of(TAG_TASK_TYPE, status.name());
    meterCache.counter(METRIC_TASKS_CLEANER_DELETABLE_TASKS_COUNT, tags)
        .increment(deletableTasksCount);
    meterCache.counter(METRIC_TASKS_CLEANER_DELETED_TASKS_COUNT, tags)
        .increment(deletedTasksCount);
    meterCache.counter(METRIC_TASKS_CLEANER_DELETED_UNIQUE_KEYS_COUNT, tags)
        .increment(deletedUniqueKeysCount);
    meterCache.counter(METRIC_TASKS_CLEANER_DELETED_TASK_DATAS_COUNT, tags)
        .increment(deletedTaskDatasCount);
  }

  @Override
  public void registerDaoTaskDataSerialization(String taskType, int originalSize, int serializedSize) {
    TagsSet tags = TagsSet.of(TAG_TASK_TYPE, taskType);
    meterCache.counter(METRIC_DAO_DATA_SIZE, tags)
        .increment(originalSize);

    meterCache.counter(METRIC_DAO_DATA_SERIALIZED_SIZE, tags)
        .increment(serializedSize);
  }

  @Override
  public void registerKafkaTasksExecutionTriggererTriggersReceive(String bucketId) {
    meterCache.counter(METRIC_KAFKA_TASKS_EXECUTION_TRIGGERER_RECEIVED_TRIGGERS_COUNT, TagsSet.of(TAG_BUCKET_ID, resolveBucketId(bucketId)))
        .increment();
  }

  @Override
  public void registerKafkaTasksExecutionTriggererCommit(String bucketId) {
    meterCache.counter(METRIC_KAFKA_TASKS_EXECUTION_TRIGGERER_COMMITS_COUNT, TagsSet.of(TAG_BUCKET_ID, resolveBucketId(bucketId)))
        .increment();
  }

  @Override
  public void registerKafkaTasksExecutionTriggererFailedCommit(String bucketId) {
    meterCache.counter(METRIC_KAFKA_TASKS_EXECUTION_TRIGGERER_FAILED_COMMITS_COUNT, TagsSet.of(TAG_BUCKET_ID, resolveBucketId(bucketId)))
        .increment();
  }

  @Override
  public void registerKafkaTasksExecutionTriggererAlreadyCommitedOffset(String bucketId) {
    meterCache.counter(METRIC_KAFKA_TASKS_EXECUTION_TRIGGERER_OFFSET_ALREADY_COMMITTED, TagsSet.of(TAG_BUCKET_ID, resolveBucketId(bucketId)))
        .increment();
  }

  @Override
  public Object registerTasksCleanerDeleteLagSeconds(TaskStatus status, AtomicLong lagInSeconds) {
    return registerGauge(METRIC_TASKS_CLEANER_DELETE_LAG_SECONDS, lagInSeconds::get, TAG_TASK_STATUS, status.name());
  }

  @Override
  public void unregisterMetric(Object rawMetricHandle) {
    MetricHandle metricHandle = (MetricHandle) rawMetricHandle;

    if (metricHandle.cached) {
      meterCache.removeMeter(metricHandle.getMeter().getId().getName(), metricHandle.getTags());
    }
    meterCache.getMeterRegistry().remove(metricHandle.getMeter());
  }

  @Override
  public Object registerTasksInErrorCount(AtomicInteger erroneousTasksCount) {
    return registerGauge(METRIC_HEALTH_TASKS_IN_ERROR_COUNT, erroneousTasksCount::get);
  }

  @Override
  public Object registerTasksInErrorCount(String taskType, AtomicInteger count) {
    return registerGauge(METRIC_HEALTH_TASKS_IN_ERROR_COUNT_PER_TYPE, count::get, TAG_TASK_TYPE, taskType);
  }

  @Override
  public Object registerStuckTasksCount(AtomicInteger stuckTasksCount) {
    return registerGauge(METRIC_HEALTH_STUCK_TASKS_COUNT, stuckTasksCount::get);
  }

  @Override
  public Object registerStuckTasksCount(TaskStatus status, String type, AtomicInteger count) {
    return registerGauge(METRIC_HEALTH_STUCK_TASKS_COUNT_PER_TYPE, count::get, TAG_TASK_STATUS, status.name(), TAG_TASK_TYPE, type);
  }

  @Override
  public Object registerApproximateTasksCount(AtomicLong approximateTasksCount) {
    return registerGauge(METRIC_STATE_APPROXIMATE_TASKS, approximateTasksCount::get);
  }

  @Override
  public Object registerApproximateUniqueKeysCount(AtomicLong approximateUniqueKeysCount) {
    return registerGauge(METRIC_STATE_APPROXIMATE_UNIQUE_KEYS, approximateUniqueKeysCount::get);
  }

  @Override
  public Object registerApproximateTaskDatasCount(AtomicLong approximateTaskDatasCount) {
    return registerGauge(METRIC_STATE_APPROXIMATE_TASK_DATAS, approximateTaskDatasCount::get);
  }

  @Override
  public Object registerTaskHistoryLength(TaskStatus status, AtomicLong lengthInSeconds) {
    return registerGauge(METRIC_HEALTH_TASK_HISTORY_LENGTH_SECONDS, lengthInSeconds::get, TAG_TASK_STATUS, status.name());
  }

  @Override
  public Object registerProcessingTriggersCount(String bucketId, String taskType, Supplier<Number> countSupplier) {
    return registerGauge(METRIC_PROCESSING_TYPE_TRIGGERS_COUNT, countSupplier, TAG_BUCKET_ID, resolveBucketId(bucketId),
        TAG_TASK_TYPE, taskType);
  }

  @Override
  public Object registerProcessingTriggersCount(String bucketId, Supplier<Number> countSupplier) {
    return registerGauge(METRIC_PROCESSING_TRIGGERS_COUNT, countSupplier, TAG_BUCKET_ID, resolveBucketId(bucketId));
  }

  @Override
  public Object registerRunningTasksCount(String bucketId, Supplier<Number> countSupplier) {
    return registerGauge(METRIC_PROCESSING_RUNNING_TASKS_COUNT, countSupplier, TAG_BUCKET_ID, resolveBucketId(bucketId));
  }

  @Override
  public Object registerInProgressTasksGrabbingCount(String bucketId, Supplier<Number> countSupplier) {
    return registerGauge(METRIC_PROCESSING_IN_PROGRESS_TASKS_GRABBING_COUNT, countSupplier, TAG_BUCKET_ID, resolveBucketId(bucketId));
  }

  @Override
  public Object registerProcessingStateVersion(String bucketId, Supplier<Number> countSupplier) {
    return registerGauge(METRIC_PROCESSING_STATE_VERSION, countSupplier, TAG_BUCKET_ID, resolveBucketId(bucketId));
  }

  @Override
  public Object registerKafkaTasksExecutionTriggererOffsetsToBeCommitedCount(String bucketId, Supplier<Number> countSupplier) {
    return registerGauge(METRIC_KAFKA_TASKS_EXECUTION_TRIGGERER_OFFSETS_TO_BE_COMMITED_COUNT, countSupplier, TAG_BUCKET_ID,
        resolveBucketId(bucketId));
  }

  @Override
  public Object registerKafkaTasksExecutionTriggererOffsetsCompletedCount(String bucketId, Supplier<Number> countSupplier) {
    return registerGauge(METRIC_KAFKA_TASKS_EXECUTION_TRIGGERER_OFFSETS_COMPLETED_COUNT, countSupplier, TAG_BUCKET_ID, resolveBucketId(bucketId));
  }

  @Override
  public Object registerKafkaTasksExecutionTriggererUnprocessedFetchedRecordsCount(String bucketId, Supplier<Number> countSupplier) {
    return registerGauge(METRIC_KAFKA_TASKS_EXECUTION_TRIGGERER_UNPROCESSED_FETCHED_RECORDS_COUNT, countSupplier, TAG_BUCKET_ID,
        resolveBucketId(bucketId));
  }

  @Override
  public Object registerKafkaTasksExecutionTriggererOffsetsCount(String bucketId, Supplier<Number> countSupplier) {
    return registerGauge(METRIC_KAFKA_TASKS_EXECUTION_TRIGGERER_OFFSETS_COUNT, countSupplier, TAG_BUCKET_ID, resolveBucketId(bucketId));
  }

  @Override
  public Object registerBucketsCount(Supplier<Integer> supplier) {
    return registerGauge(METRIC_BUCKETS_MANAGER_BUCKETS_COUNT, supplier::get);
  }

  @Override
  public void registerLibrary() {
    String version = this.getClass().getPackage().getImplementationVersion();
    if (version == null) {
      version = "Unknown";
    }

    Gauge.builder(METRIC_LIBRARY_INFO, () -> 1d).tags("version", version, "library", "tw-tasks-core")
        .description("Provides metadata about the library, for example the version.")
        .register(meterCache.getMeterRegistry());

  }

  @SuppressWarnings("rawtypes")
  public void registerKafkaConsumer(Consumer consumer){
    // Spring application are setting the tag `spring.id`, so we need to set it as well.
    new KafkaClientMetrics(consumer, List.of(new ImmutableTag("spring.id", "tw-tasks-" + kafkaClientId.incrementAndGet())))
        .bindTo(meterCache.getMeterRegistry());
  }

  @SuppressWarnings("rawtypes")
  public void registerKafkaProducer(Producer producer){
    // Spring application are setting the tag `spring.id`, so we need to set it as well.
    new KafkaClientMetrics(producer, List.of(new ImmutableTag("spring.id", "tw-tasks-" + kafkaClientId.incrementAndGet())))
        .bindTo(meterCache.getMeterRegistry());
  }

  protected String getDataSizeBucket(byte[] data) {
    int dataSize = data == null ? 0 : data.length;
    for (int i = 0; i < DATA_SIZE_BUCKETS.length; i++) {
      if (dataSize < DATA_SIZE_BUCKETS[i]) {
        return DATA_SIZE_BUCKET_VALUES[i];
      }
    }
    return "HUGE";
  }

  protected String resolveBucketId(String bucketId) {
    return bucketId == null ? "unknown" : bucketId;
  }

  protected MetricHandle registerGauge(String name, Supplier<Number> supplier, String... tags) {
    return new MetricHandle().setMeter(Gauge.builder(name, supplier)
        .tags(tags).register(meterCache.getMeterRegistry()));
  }

  @Data
  @Accessors(chain = true)
  protected static class MetricHandle {

    private boolean cached;
    private TagsSet tags;
    private Meter meter;

  }
}
