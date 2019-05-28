package com.transferwise.tasks.helpers;

import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.Meter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Tag;
import io.micrometer.core.instrument.Tags;
import lombok.RequiredArgsConstructor;
import org.apache.commons.collections4.MapUtils;
import org.apache.commons.lang3.tuple.Triple;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;
import java.util.stream.Collectors;

@SuppressWarnings("checkstyle:MultipleStringLiterals")
@RequiredArgsConstructor
public class MicrometerMeterHelper implements IMeterHelper {
    private final MeterRegistry meterRegistry;

    private Map<Triple<String, String, String>, AtomicInteger> gauges = new ConcurrentHashMap<>();

    @Override
    public void registerTaskMarkedAsError(String bucketId, String taskType) {
        meterRegistry.counter(METRIC_PREFIX + "tasks.markedAsErrorCount", "bucketId", resolveBucketId(bucketId), "taskType", taskType).increment();
    }

    @Override
    public void registerTaskProcessingStart(String bucketId, String taskType) {
        String resolvedBucketId = resolveBucketId(bucketId);
        meterRegistry.counter(METRIC_PREFIX + "tasks.processingsCount", "bucketId", resolvedBucketId, "taskType", taskType).increment();
        gauges.computeIfAbsent(Triple.of("tasks.ongoingProcessingsCount", resolvedBucketId, taskType), (t) -> {
            AtomicInteger counter = new AtomicInteger(0);
            meterRegistry.gauge(METRIC_PREFIX + "tasks.ongoingProcessingsCount", Tags.of("bucketId", resolvedBucketId, "taskType", taskType), counter);
            return counter;
        }).incrementAndGet();
    }

    @Override
    public void registerTaskProcessingEnd(String bucketId, String taskType) {
        String resolvedBucketId = resolveBucketId(bucketId);
        meterRegistry.counter(METRIC_PREFIX + "tasks.processingsCount", "bucketId", resolvedBucketId, "taskType", taskType).increment();
        gauges.get(Triple.of("tasks.ongoingProcessingsCount", resolvedBucketId, taskType)).decrementAndGet();
    }

    @Override
    public void registerKafkaCoreMessageProcessing(String topic) {
        meterRegistry.counter(METRIC_PREFIX + "coreKafka.processedMessagesCount", Tags.of("topic", topic));
    }

    @Override
    public void registerDuplicateTask(String taskType, boolean expected) {
        meterRegistry.counter(METRIC_PREFIX + "tasks.duplicatesCount", "taskType", taskType, "expected", String.valueOf(expected)).increment();
    }

    @Override
    public void registerFailedTaskGrabbing(String bucketId, String taskType) {
        meterRegistry.counter(METRIC_PREFIX + "tasks.failedGrabbingsCount", "bucketId", resolveBucketId(bucketId), "taskType", taskType).increment();
    }

    @Override
    public void registerTaskRetryOnError(String bucketId, String taskType) {
        meterRegistry.counter(METRIC_PREFIX + "tasks.retriesCount", "bucketId", resolveBucketId(bucketId), "taskType", taskType,
            "reason", "ERROR").increment();
    }

    @Override
    public void registerTaskRetry(String bucketId, String taskType) {
        meterRegistry.counter(METRIC_PREFIX + "tasks.retriesCount", "bucketId", resolveBucketId(bucketId), "taskType", taskType,
            "reason", "CONTINUE").increment();
    }

    @Override
    public void registerTaskResuming(String bucketId, String taskType) {
        meterRegistry.counter(METRIC_PREFIX + "tasks.resumingsCount", "bucketId", resolveBucketId(bucketId), "taskType", taskType).increment();
    }

    @Override
    public void registerTaskMarkedAsFailed(String bucketId, String taskType) {
        meterRegistry.counter(METRIC_PREFIX + "tasks.markedAsFailedCount", "bucketId", resolveBucketId(bucketId), "taskType", taskType).increment();
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
