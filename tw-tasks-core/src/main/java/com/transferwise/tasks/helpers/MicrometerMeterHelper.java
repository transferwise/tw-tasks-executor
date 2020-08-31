package com.transferwise.tasks.helpers;

import com.transferwise.common.context.TwContextClockHolder;
import com.transferwise.tasks.domain.TaskStatus;
import com.transferwise.tasks.processing.TasksProcessingService.ProcessTaskResponse;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.Meter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Tag;
import io.micrometer.core.instrument.Tags;
import java.time.ZonedDateTime;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections4.MapUtils;
import org.apache.commons.lang3.tuple.Triple;

@SuppressWarnings("checkstyle:MultipleStringLiterals")
@RequiredArgsConstructor
@Slf4j
public class MicrometerMeterHelper implements IMeterHelper {

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
  private static final String METRIC_CORE_KAFKA_PROCESSED_MESSAGES_COUNT = METRIC_PREFIX + "coreKafka.processedMessagesCount";
  private static final String METRIC_TASKS_DUPLICATES_COUNT = METRIC_PREFIX + "tasks.duplicatesCount";
  private static final String METRIC_TASKS_RESUMER_SCHEDULED_TASKS_RESUMED_COUNT = METRIC_PREFIX + "tasksResumer.scheduledTasks.resumedCount";
  private static final String METRIC_TASKS_RESUMER_STUCK_TASKS_MARK_FAILED_COUNT = METRIC_PREFIX + "tasksResumer.stuckTasks.markFailedCount";
  private static final String METRIC_TASKS_RESUMER_STUCK_TASKS_IGNORED_COUNT = METRIC_PREFIX + "tasksResumer.stuckTasks.ignoredCount";
  private static final String METRIC_TASKS_RESUMER_STUCK_TASKS_RESUMED_COUNT = METRIC_PREFIX + "tasksResumer.stuckTasks.resumedCount";
  private static final String METRIC_TASKS_RESUMER_STUCK_TASKS_MARK_ERROR_COUNT = METRIC_PREFIX + "tasksResumer.stuckTasks.markErrorCount";
  private static final String METRIC_TASKS_RESUMER_STUCK_TASKS_CLIENT_RESUMED_COUNT = METRIC_PREFIX + "tasksResumer.stuckTasks.clientResumedCount";
  private static final String METRIC_TASKS_FAILED_GRABBINGS_COUNT = METRIC_PREFIX + "tasks.failedGrabbingsCount";
  private static final String METRIC_TASKS_RETRIES_COUNT = METRIC_PREFIX + "tasks.retriesCount";
  private static final String METRIC_TASKS_RESUMINGS_COUNT = METRIC_PREFIX + "tasks.resumingsCount";
  private static final String METRIC_TASKS_MARKED_AS_FAILED_COUNT = METRIC_PREFIX + "tasks.markedAsFailedCount";
  private static final String METRIC_TASKS_ADDINGS_COUNT = METRIC_PREFIX + "task.addings.count";

  private static final String TAG_PROCESSING_RESULT = "processingResult";
  private static final String TAG_FROM_STATUS = "fromStatus";
  private static final String TAG_TO_STATUS = "toStatus";
  private static final String TAG_BUCKET_ID = "bucketId";
  private static final String TAG_TASK_TYPE = "taskType";
  private static final String TAG_REASON = "reason";
  private static final String TAG_PRIORITY = "priority";
  private static final String TAG_GRABBING_RESPONSE = "grabbingResponse";
  private static final String TAG_GRABBING_CODE = "grabbingCode";
  private static final String TAG_HAS_KEY = "hasKey";
  private static final String TAG_IS_DUPLICATE = "isDuplicate";
  private static final String TAG_IS_SCHEDULED = "isScheduled";
  private static final String TAG_DATA_SIZE = "dataSize";

  private static final String[] DATA_SIZE_BUCKET_VALUES = {"64", "256", "1024", "4096", "16384", "65536"};
  private static final int[] DATA_SIZE_BUCKETS = {64, 256, 1024, 4096, 16384, 65536};

  private final MeterRegistry meterRegistry;

  private final Map<Triple<String, String, String>, AtomicInteger> gauges = new ConcurrentHashMap<>();

  @Override
  public void registerTaskMarkedAsError(String bucketId, String taskType) {
    meterRegistry.counter(METRIC_TASKS_MARKED_AS_ERROR_COUNT, TAG_BUCKET_ID, resolveBucketId(bucketId), TAG_TASK_TYPE, taskType).increment();
  }

  @Override
  public void registerTaskProcessingStart(String bucketId, String taskType) {
    String resolvedBucketId = resolveBucketId(bucketId);
    meterRegistry.counter(METRIC_TASKS_PROCESSINGS_COUNT, TAG_BUCKET_ID, resolvedBucketId, TAG_TASK_TYPE, taskType).increment();

    gauges.computeIfAbsent(Triple.of("tasks.ongoingProcessingsCount", resolvedBucketId, taskType), (t) -> {
      AtomicInteger counter = new AtomicInteger(0);
      meterRegistry
          .gauge(METRIC_TASKS_ONGOING_PROCESSINGS_COUNT, Tags.of(TAG_BUCKET_ID, resolvedBucketId, TAG_TASK_TYPE, taskType), counter);
      return counter;
    }).incrementAndGet();
  }

  @Override
  public void registerTaskProcessingEnd(String bucketId, String taskType, long processingStartTimeMs, String processingResult) {
    String resolvedBucketId = resolveBucketId(bucketId);
    meterRegistry.counter(METRIC_TASKS_PROCESSED_COUNT, TAG_BUCKET_ID, resolvedBucketId, TAG_TASK_TYPE, taskType,
        TAG_PROCESSING_RESULT, processingResult).increment();
    meterRegistry.timer(METRIC_TASKS_PROCESSING_TIME, TAG_BUCKET_ID, resolvedBucketId, TAG_TASK_TYPE, taskType,
        TAG_PROCESSING_RESULT, processingResult)
        .record(TwContextClockHolder.getClock().millis() - processingStartTimeMs, TimeUnit.MILLISECONDS);
    gauges.get(Triple.of("tasks.ongoingProcessingsCount", resolvedBucketId, taskType)).decrementAndGet();
  }

  @Override
  public void registerFailedStatusChange(String taskType, String fromStatus, TaskStatus toStatus) {
    meterRegistry.counter(METRIC_TASKS_FAILED_STATUS_CHANGE_COUNT, TAG_TASK_TYPE, taskType,
        TAG_FROM_STATUS, fromStatus, TAG_TO_STATUS, toStatus.name()).increment();
  }

  @Override
  public void registerTaskGrabbingResponse(String bucketId, String taskType, int priority, ProcessTaskResponse processTaskResponse) {
    meterRegistry.counter(METRIC_TASKS_TASK_GRABBING, TAG_TASK_TYPE, taskType,
        TAG_BUCKET_ID, bucketId, TAG_PRIORITY, String.valueOf(priority), TAG_GRABBING_RESPONSE, processTaskResponse.getResult().name(),
        TAG_GRABBING_CODE, processTaskResponse.getCode() == null ? "UNKNOWN" : processTaskResponse.getCode().name())
        .increment();
  }

  @Override
  public void debugPriorityQueueCheck(String bucketId, int priority) {
    meterRegistry.counter(METRIC_TASKS_DEBUG_PRIORITY_QUEUE_CHECK, TAG_BUCKET_ID, bucketId, TAG_PRIORITY, String.valueOf(priority))
        .increment();
  }

  @Override
  public void debugRoomMapAlreadyHasType(String bucketId, int priority, String taskType) {
    meterRegistry.counter(METRIC_TASKS_DEBUG_ROOM_MAP_ALREADY_HAS_TYPE, TAG_BUCKET_ID, bucketId, TAG_PRIORITY, String.valueOf(priority),
        TAG_TASK_TYPE, taskType).increment();
  }

  @Override
  public void debugTaskTriggeringQueueEmpty(String bucketId, int priority, String taskType) {
    meterRegistry.counter(METRIC_TASKS_DEBUG_TASK_TRIGGERING_QUEUE_EMPTY, TAG_BUCKET_ID, bucketId, TAG_PRIORITY, String.valueOf(priority),
        TAG_TASK_TYPE, taskType).increment();
  }

  @Override
  public void registerKafkaCoreMessageProcessing(int shard, String topic) {
    meterRegistry.counter(METRIC_CORE_KAFKA_PROCESSED_MESSAGES_COUNT, Tags.of("topic", topic, "shard", String.valueOf(shard))).increment();
  }

  @Override
  public void registerDuplicateTask(String taskType, boolean expected) {
    meterRegistry.counter(METRIC_TASKS_DUPLICATES_COUNT, TAG_TASK_TYPE, taskType, "expected", String.valueOf(expected)).increment();
  }

  @Override
  public void registerScheduledTaskResuming(String taskType) {
    meterRegistry.counter(METRIC_TASKS_RESUMER_SCHEDULED_TASKS_RESUMED_COUNT, TAG_TASK_TYPE, taskType).increment();
  }

  @Override
  public void registerStuckTaskMarkedAsFailed(String taskType) {
    meterRegistry.counter(METRIC_TASKS_RESUMER_STUCK_TASKS_MARK_FAILED_COUNT, TAG_TASK_TYPE, taskType).increment();
  }

  @Override
  public void registerStuckTaskAsIgnored(String taskType) {
    meterRegistry.counter(METRIC_TASKS_RESUMER_STUCK_TASKS_IGNORED_COUNT, TAG_TASK_TYPE, taskType).increment();
  }

  @Override
  public void registerStuckTaskResuming(String taskType) {
    meterRegistry.counter(METRIC_TASKS_RESUMER_STUCK_TASKS_RESUMED_COUNT, TAG_TASK_TYPE, taskType).increment();
  }

  @Override
  public void registerStuckTaskMarkedAsError(String taskType) {
    meterRegistry.counter(METRIC_TASKS_RESUMER_STUCK_TASKS_MARK_ERROR_COUNT, TAG_TASK_TYPE, taskType).increment();
  }

  @Override
  public void registerStuckClientTaskResuming(String taskType) {
    meterRegistry.counter(METRIC_TASKS_RESUMER_STUCK_TASKS_CLIENT_RESUMED_COUNT, TAG_TASK_TYPE, taskType).increment();
  }

  @Override
  public void registerFailedTaskGrabbing(String bucketId, String taskType) {
    meterRegistry.counter(METRIC_TASKS_FAILED_GRABBINGS_COUNT, TAG_BUCKET_ID, resolveBucketId(bucketId), TAG_TASK_TYPE, taskType)
        .increment();
  }

  @Override
  public void registerTaskRetryOnError(String bucketId, String taskType) {
    meterRegistry.counter(METRIC_TASKS_RETRIES_COUNT, TAG_BUCKET_ID, resolveBucketId(bucketId), TAG_TASK_TYPE, taskType,
        TAG_REASON, "ERROR").increment();
  }

  @Override
  public void registerTaskRetry(String bucketId, String taskType) {
    meterRegistry.counter(METRIC_TASKS_RETRIES_COUNT, TAG_BUCKET_ID, resolveBucketId(bucketId), TAG_TASK_TYPE, taskType,
        TAG_REASON, "CONTINUE").increment();
  }

  @Override
  public void registerTaskResuming(String bucketId, String taskType) {
    meterRegistry.counter(METRIC_TASKS_RESUMINGS_COUNT, TAG_BUCKET_ID, resolveBucketId(bucketId), TAG_TASK_TYPE, taskType).increment();
  }

  @Override
  public void registerTaskMarkedAsFailed(String bucketId, String taskType) {
    meterRegistry.counter(METRIC_TASKS_MARKED_AS_FAILED_COUNT, TAG_BUCKET_ID, resolveBucketId(bucketId), TAG_TASK_TYPE, taskType).increment();
  }

  @Override
  public void registerTaskAdding(String type, String key, boolean inserted, ZonedDateTime runAfterTime, String data) {
    meterRegistry.counter(
        METRIC_TASKS_ADDINGS_COUNT,
        TAG_TASK_TYPE, type,
        TAG_HAS_KEY, Boolean.toString(key != null),
        TAG_IS_DUPLICATE, Boolean.toString(!inserted),
        TAG_IS_SCHEDULED, Boolean.toString(runAfterTime != null),
        TAG_DATA_SIZE, getDataSizeBucket(data)
    ).increment();
  }

  @Override
  public Object registerGauge(String name, Map<String, String> tags, Supplier<Number> valueSupplier) {
    return Gauge.builder(name, valueSupplier)
        .tags(convert(tags)).register(meterRegistry);
  }

  @Override
  public void unregisterMetric(Object handle) {
    if (handle instanceof Meter) {
      meterRegistry.remove((Meter) handle);
    } else if (handle instanceof Meter.Id) {
      meterRegistry.remove((Meter.Id) handle);
    } else {
      throw new IllegalArgumentException("Can not unregister metric. Provided handle '" + handle + "' is not supported.");
    }
  }

  @Override
  public void incrementCounter(String name, Map<String, String> tags, long delta) {
    meterRegistry.counter(name, convert(tags)).increment(delta);
  }

  protected String resolveBucketId(String bucketId) {
    return bucketId == null ? "unknown" : bucketId;
  }

  //TODO: Don't like the efficiency of this method.
  //      Unfortunately the micrometer Tags api is very unhelpful for this use case.
  protected Tags convert(Map<String, String> tagsMap) {
    if (MapUtils.isNotEmpty(tagsMap)) {
      return Tags.of(tagsMap.entrySet().stream().map(e -> Tag.of(e.getKey(), e.getValue())).collect(Collectors.toList()));
    }
    return Tags.empty();
  }
  
  protected String getDataSizeBucket(String data) {
    int dataSize = data == null ? 0 : data.length();
    for (int i = 0; i < DATA_SIZE_BUCKETS.length; i++) {
      if (dataSize < DATA_SIZE_BUCKETS[i]) {
        return DATA_SIZE_BUCKET_VALUES[i];
      }
    }
    return "HUGE";
  }

}
