package com.transferwise.tasks.helpers;

import com.transferwise.common.baseutils.clock.ClockHolder;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.Meter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Tag;
import io.micrometer.core.instrument.Tags;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections4.MapUtils;
import org.apache.commons.lang3.tuple.Triple;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;
import java.util.stream.Collectors;

@SuppressWarnings("checkstyle:MultipleStringLiterals")
@RequiredArgsConstructor
@Slf4j
public class MicrometerMeterHelper implements IMeterHelper {
    private static String TAG_BUCKET_ID = "bucketId";
    private static String TAG_TASK_TYPE = "taskType";
    private static String TAG_REASON = "reason";
    private static String TAG_PROCESSING_RESULT = "processingResult";

    private final MeterRegistry meterRegistry;

    private Map<Triple<String, String, String>, AtomicInteger> gauges = new ConcurrentHashMap<>();

    @Override
    public void registerTaskMarkedAsError(String bucketId, String taskType) {
        meterRegistry.counter(METRIC_PREFIX + "tasks.markedAsErrorCount", TAG_BUCKET_ID, resolveBucketId(bucketId), TAG_TASK_TYPE, taskType).increment();
    }

    @Override
    public void registerTaskProcessingStart(String bucketId, String taskType) {
        String resolvedBucketId = resolveBucketId(bucketId);
        meterRegistry.counter(METRIC_PREFIX + "tasks.processingsCount", TAG_BUCKET_ID, resolvedBucketId, TAG_TASK_TYPE, taskType).increment();
        log.info("Increased tasks.processingsCount");
        gauges.computeIfAbsent(Triple.of("tasks.ongoingProcessingsCount", resolvedBucketId, taskType), (t) -> {
            AtomicInteger counter = new AtomicInteger(0);
            meterRegistry.gauge(METRIC_PREFIX + "tasks.ongoingProcessingsCount", Tags.of(TAG_BUCKET_ID, resolvedBucketId, TAG_TASK_TYPE, taskType), counter);
            return counter;
        }).incrementAndGet();
    }

    @Override
    public void registerTaskProcessingEnd(String bucketId, String taskType, long processingStartTimeMs, String processingResult) {
        String resolvedBucketId = resolveBucketId(bucketId);
        meterRegistry.counter(METRIC_PREFIX + "tasks.processedCount", TAG_BUCKET_ID, resolvedBucketId, TAG_TASK_TYPE, taskType,
            TAG_PROCESSING_RESULT, processingResult).increment();
        meterRegistry.timer(METRIC_PREFIX + "tasks.processingTime", TAG_BUCKET_ID, resolvedBucketId, TAG_TASK_TYPE, taskType,
            TAG_PROCESSING_RESULT, processingResult)
            .record(ClockHolder.getClock().millis() - processingStartTimeMs, TimeUnit.MILLISECONDS);
        gauges.get(Triple.of("tasks.ongoingProcessingsCount", resolvedBucketId, taskType)).decrementAndGet();
    }

    @Override
    public void registerKafkaCoreMessageProcessing(String topic) {
        meterRegistry.counter(METRIC_PREFIX + "coreKafka.processedMessagesCount", Tags.of("topic", topic)).increment();
    }

    @Override
    public void registerDuplicateTask(String taskType, boolean expected) {
        meterRegistry.counter(METRIC_PREFIX + "tasks.duplicatesCount", TAG_TASK_TYPE, taskType, "expected", String.valueOf(expected)).increment();
    }

    @Override
    public void registerScheduledTaskResuming(String taskType) {
        meterRegistry.counter(METRIC_PREFIX + "tasksResumer.scheduledTasks.resumedCount", TAG_TASK_TYPE, taskType).increment();
    }

    @Override
    public void registerStuckTaskMarkedAsFailed(String taskType) {
        meterRegistry.counter(METRIC_PREFIX + "tasksResumer.stuckTasks.markFailedCount", TAG_TASK_TYPE, taskType).increment();
    }

    @Override
    public void registerStuckTaskAsIgnored(String taskType) {
        meterRegistry.counter(METRIC_PREFIX + "tasksResumer.stuckTasks.ignoredCount", TAG_TASK_TYPE, taskType).increment();
    }

    @Override
    public void registerStuckTaskResuming(String taskType) {
        meterRegistry.counter(METRIC_PREFIX + "tasksResumer.stuckTasks.resumedCount", TAG_TASK_TYPE, taskType).increment();
    }

    @Override
    public void registerStuckTaskMarkedAsError(String taskType) {
        meterRegistry.counter(METRIC_PREFIX + "tasksResumer.stuckTasks.markErrorCount", TAG_TASK_TYPE, taskType).increment();
    }

    @Override
    public void registerStuckClientTaskResuming(String taskType) {
        meterRegistry.counter(METRIC_PREFIX + "tasksResumer.stuckTasks.clientResumedCount", TAG_TASK_TYPE, taskType).increment();
    }

    @Override
    public void registerFailedTaskGrabbing(String bucketId, String taskType) {
        meterRegistry.counter(METRIC_PREFIX + "tasks.failedGrabbingsCount", TAG_BUCKET_ID, resolveBucketId(bucketId), TAG_TASK_TYPE, taskType).increment();
    }

    @Override
    public void registerTaskRetryOnError(String bucketId, String taskType) {
        meterRegistry.counter(METRIC_PREFIX + "tasks.retriesCount", TAG_BUCKET_ID, resolveBucketId(bucketId), TAG_TASK_TYPE, taskType,
            TAG_REASON, "ERROR").increment();
    }

    @Override
    public void registerTaskRetry(String bucketId, String taskType) {
        meterRegistry.counter(METRIC_PREFIX + "tasks.retriesCount", TAG_BUCKET_ID, resolveBucketId(bucketId), TAG_TASK_TYPE, taskType,
            TAG_REASON, "CONTINUE").increment();
    }

    @Override
    public void registerTaskResuming(String bucketId, String taskType) {
        meterRegistry.counter(METRIC_PREFIX + "tasks.resumingsCount", TAG_BUCKET_ID, resolveBucketId(bucketId), TAG_TASK_TYPE, taskType).increment();
    }

    @Override
    public void registerTaskMarkedAsFailed(String bucketId, String taskType) {
        meterRegistry.counter(METRIC_PREFIX + "tasks.markedAsFailedCount", TAG_BUCKET_ID, resolveBucketId(bucketId), TAG_TASK_TYPE, taskType).increment();
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
}
