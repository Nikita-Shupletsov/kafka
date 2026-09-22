/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.kafka.common.telemetry.internals;

import org.apache.kafka.common.metrics.stats.AdaptiveHistogramWindow;
import org.apache.kafka.common.metrics.stats.FixedHistogramWindow;

import java.time.Instant;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.OptionalDouble;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import io.opentelemetry.proto.common.v1.AnyValue;
import io.opentelemetry.proto.common.v1.KeyValue;
import io.opentelemetry.proto.metrics.v1.AggregationTemporality;
import io.opentelemetry.proto.metrics.v1.ExponentialHistogramDataPoint;
import io.opentelemetry.proto.metrics.v1.HistogramDataPoint;
import io.opentelemetry.proto.metrics.v1.Metric;
import io.opentelemetry.proto.metrics.v1.NumberDataPoint;

/**
 * This class represents a telemetry metric that does not yet contain resource tags.
 * These additional resource tags will be added before emitting metrics by the telemetry reporter.
 */
public class SinglePointMetric implements MetricKeyable {

    private final MetricKey key;
    private final Metric.Builder metricBuilder;

    private SinglePointMetric(MetricKey key, Metric.Builder metricBuilder) {
        this.key = key;
        this.metricBuilder = metricBuilder;
    }

    @Override
    public MetricKey key() {
        return key;
    }

    public Metric.Builder builder() {
        return metricBuilder;
    }

    // Visible for testing
    boolean hasSum() {
        return metricBuilder.hasSum();
    }

    // Visible for testing
    boolean hasGauge() {
        return metricBuilder.hasGauge();
    }

    // Visible for testing
    boolean hasExponentialHistogram() {
        return metricBuilder.hasExponentialHistogram();
    }

    // Visible for testing
    boolean isExponentialHistogramDeltaTemporality() {
        return metricBuilder.getExponentialHistogram().getAggregationTemporality()
            == AggregationTemporality.AGGREGATION_TEMPORALITY_DELTA;
    }

    // Visible for testing
    long exponentialHistogramCount() {
        return exponentialHistogramDataPoint().getCount();
    }

    // Visible for testing
    double exponentialHistogramSum() {
        return exponentialHistogramDataPoint().getSum();
    }

    // Visible for testing
    OptionalDouble exponentialHistogramMin() {
        ExponentialHistogramDataPoint point = exponentialHistogramDataPoint();
        return point.hasMin() ? OptionalDouble.of(point.getMin()) : OptionalDouble.empty();
    }

    // Visible for testing
    OptionalDouble exponentialHistogramMax() {
        ExponentialHistogramDataPoint point = exponentialHistogramDataPoint();
        return point.hasMax() ? OptionalDouble.of(point.getMax()) : OptionalDouble.empty();
    }

    // Visible for testing
    int exponentialHistogramScale() {
        return exponentialHistogramDataPoint().getScale();
    }

    // Visible for testing
    long exponentialHistogramZeroCount() {
        return exponentialHistogramDataPoint().getZeroCount();
    }

    // Visible for testing
    int exponentialHistogramPositiveOffset() {
        return exponentialHistogramDataPoint().getPositive().getOffset();
    }

    // Visible for testing
    List<Long> exponentialHistogramPositiveBucketCounts() {
        return exponentialHistogramDataPoint().getPositive().getBucketCountsList();
    }

    // Visible for testing
    List<Long> exponentialHistogramNegativeBucketCounts() {
        return exponentialHistogramDataPoint().getNegative().getBucketCountsList();
    }

    private ExponentialHistogramDataPoint exponentialHistogramDataPoint() {
        return metricBuilder.getExponentialHistogram().getDataPoints(0);
    }

    // Visible for testing
    boolean hasExplicitHistogram() {
        return metricBuilder.hasHistogram();
    }

    // Visible for testing
    boolean isExplicitHistogramDeltaTemporality() {
        return metricBuilder.getHistogram().getAggregationTemporality()
            == AggregationTemporality.AGGREGATION_TEMPORALITY_DELTA;
    }

    // Visible for testing
    long explicitHistogramCount() {
        return explicitHistogramDataPoint().getCount();
    }

    // Visible for testing
    double explicitHistogramSum() {
        return explicitHistogramDataPoint().getSum();
    }

    // Visible for testing
    OptionalDouble explicitHistogramMin() {
        HistogramDataPoint point = explicitHistogramDataPoint();
        return point.hasMin() ? OptionalDouble.of(point.getMin()) : OptionalDouble.empty();
    }

    // Visible for testing
    OptionalDouble explicitHistogramMax() {
        HistogramDataPoint point = explicitHistogramDataPoint();
        return point.hasMax() ? OptionalDouble.of(point.getMax()) : OptionalDouble.empty();
    }

    // Visible for testing
    List<Double> explicitHistogramBounds() {
        return explicitHistogramDataPoint().getExplicitBoundsList();
    }

    // Visible for testing
    List<Long> explicitHistogramBucketCounts() {
        return explicitHistogramDataPoint().getBucketCountsList();
    }

    private HistogramDataPoint explicitHistogramDataPoint() {
        return metricBuilder.getHistogram().getDataPoints(0);
    }

    // Visible for testing
    boolean isMonotonic() {
        return metricBuilder.getSum().getIsMonotonic();
    }

    // Visible for testing
    boolean isDeltaTemporality() {
        return metricBuilder.getSum().getAggregationTemporality() == AggregationTemporality.AGGREGATION_TEMPORALITY_DELTA;
    }

    // Visible for testing
    int dataPointsCount() {
        return metricBuilder.hasSum()
            ? metricBuilder.getSum().getDataPointsCount()
            : metricBuilder.getGauge().getDataPointsCount();
    }

    // Visible for testing
    long timeUnixNano() {
        return dataPoint().getTimeUnixNano();
    }

    // Visible for testing
    long startTimeUnixNano() {
        return dataPoint().getStartTimeUnixNano();
    }

    // Visible for testing
    double doubleValue() {
        return dataPoint().getAsDouble();
    }

    // Visible for testing
    long longValue() {
        return dataPoint().getAsInt();
    }

    // Visible for testing
    int attributesCount() {
        return dataPoint().getAttributesCount();
    }

    // Visible for testing
    Map<String, String> attributes() {
        Map<String, String> attributes = new LinkedHashMap<>();
        for (KeyValue attribute : dataPoint().getAttributesList()) {
            attributes.put(attribute.getKey(), attribute.getValue().getStringValue());
        }
        return attributes;
    }

    private NumberDataPoint dataPoint() {
        return metricBuilder.hasSum() ? metricBuilder.getSum().getDataPoints(0) : metricBuilder.getGauge().getDataPoints(0);
    }

    /*
        Methods to construct gauge metric type.
     */
    public static SinglePointMetric gauge(MetricKey metricKey, Number value, Instant timestamp, Set<String> excludeLabels) {
        NumberDataPoint.Builder point = point(timestamp, value);
        return gauge(metricKey, point, excludeLabels);
    }

    public static SinglePointMetric gauge(MetricKey metricKey, double value, Instant timestamp, Set<String> excludeLabels) {
        NumberDataPoint.Builder point = point(timestamp, value);
        return gauge(metricKey, point, excludeLabels);
    }

    /*
        Methods to construct sum metric type.
     */

    public static SinglePointMetric sum(MetricKey metricKey, double value, boolean monotonic, Instant timestamp, Set<String> excludeLabels) {
        return sum(metricKey, value, monotonic, timestamp, null, excludeLabels);
    }

    public static SinglePointMetric sum(MetricKey metricKey, double value, boolean monotonic, Instant timestamp,
        Instant startTimestamp, Set<String> excludeLabels) {
        NumberDataPoint.Builder point = point(timestamp, value);
        if (startTimestamp != null) {
            point.setStartTimeUnixNano(toTimeUnixNanos(startTimestamp));
        }

        return sum(metricKey, AggregationTemporality.AGGREGATION_TEMPORALITY_CUMULATIVE, monotonic, point, excludeLabels);
    }

    public static SinglePointMetric deltaSum(MetricKey metricKey, double value, boolean monotonic,
        Instant timestamp, Instant startTimestamp, Set<String> excludeLabels) {
        NumberDataPoint.Builder point = point(timestamp, value)
            .setStartTimeUnixNano(toTimeUnixNanos(startTimestamp));

        return sum(metricKey, AggregationTemporality.AGGREGATION_TEMPORALITY_DELTA, monotonic, point, excludeLabels);
    }

    /*
        Method to construct the OTLP exponential-histogram metric type. A snapshot covers one or more closed
        windows that no earlier point covered, so it is always delta temporality.
     */
    public static SinglePointMetric deltaExponentialHistogram(MetricKey metricKey, AdaptiveHistogramWindow snapshot,
        Instant timestamp, Instant startTimestamp, Set<String> excludeLabels) {
        ExponentialHistogramDataPoint.Builder point = ExponentialHistogramDataPoint.newBuilder()
            .setTimeUnixNano(toTimeUnixNanos(timestamp))
            .setStartTimeUnixNano(toTimeUnixNanos(startTimestamp))
            .setScale(snapshot.scale())
            .setCount(snapshot.count())
            .setSum(snapshot.sum())
            .setPositive(buckets(snapshot.offset(), snapshot.binCounts()));
        // min/max are only meaningful for a non-empty window.
        if (snapshot.count() > 0) {
            point.setMin(snapshot.min()).setMax(snapshot.max());
        }
        point.addAllAttributes(asAttributes(metricKey.tags(), excludeLabels));

        Metric.Builder metric = Metric.newBuilder().setName(metricKey.name());
        metric.getExponentialHistogramBuilder()
            .setAggregationTemporality(AggregationTemporality.AGGREGATION_TEMPORALITY_DELTA)
            .addDataPoints(point);
        return new SinglePointMetric(metricKey, metric);
    }

    /*
        Method to construct the OTLP explicit-bucket histogram metric type, for a fixed-bin store's windows.
        Delta temporality for the same reason as deltaExponentialHistogram.
     */
    public static SinglePointMetric deltaExplicitHistogram(MetricKey metricKey, FixedHistogramWindow snapshot,
        Instant timestamp, Instant startTimestamp, Set<String> excludeLabels) {
        HistogramDataPoint.Builder point = HistogramDataPoint.newBuilder()
            .setTimeUnixNano(toTimeUnixNanos(timestamp))
            .setStartTimeUnixNano(toTimeUnixNanos(startTimestamp))
            .setCount(snapshot.count())
            .setSum(snapshot.sum());
        for (double bound : snapshot.bounds()) {
            point.addExplicitBounds(bound);
        }
        for (long count : snapshot.binCounts()) {
            point.addBucketCounts(count);
        }
        // min/max are only meaningful for a non-empty window.
        if (snapshot.count() > 0) {
            point.setMin(snapshot.min()).setMax(snapshot.max());
        }
        point.addAllAttributes(asAttributes(metricKey.tags(), excludeLabels));

        Metric.Builder metric = Metric.newBuilder().setName(metricKey.name());
        metric.getHistogramBuilder()
            .setAggregationTemporality(AggregationTemporality.AGGREGATION_TEMPORALITY_DELTA)
            .addDataPoints(point);
        return new SinglePointMetric(metricKey, metric);
    }

    private static ExponentialHistogramDataPoint.Buckets buckets(int offset, long[] counts) {
        ExponentialHistogramDataPoint.Buckets.Builder buckets = ExponentialHistogramDataPoint.Buckets.newBuilder()
            .setOffset(offset);
        for (long count : counts) {
            buckets.addBucketCounts(count);
        }
        return buckets.build();
    }

    /*
        Helper methods to support metric construction.
     */
    private static SinglePointMetric sum(MetricKey metricKey, AggregationTemporality aggregationTemporality,
        boolean monotonic, NumberDataPoint.Builder point, Set<String> excludeLabels) {
        point.addAllAttributes(asAttributes(metricKey.tags(), excludeLabels));

        Metric.Builder metric = Metric.newBuilder().setName(metricKey.name());
        metric
            .getSumBuilder()
            .setAggregationTemporality(aggregationTemporality)
            .setIsMonotonic(monotonic)
            .addDataPoints(point);
        return new SinglePointMetric(metricKey, metric);
    }

    private static SinglePointMetric gauge(MetricKey metricKey, NumberDataPoint.Builder point, Set<String> excludeLabels) {
        point.addAllAttributes(asAttributes(metricKey.tags(), excludeLabels));

        Metric.Builder metric = Metric.newBuilder().setName(metricKey.name());
        metric.getGaugeBuilder().addDataPoints(point);
        return new SinglePointMetric(metricKey, metric);
    }

    private static NumberDataPoint.Builder point(Instant timestamp, Number value) {
        if (value instanceof Long || value instanceof Integer) {
            return point(timestamp, value.longValue());
        }

        return point(timestamp, value.doubleValue());
    }

    private static NumberDataPoint.Builder point(Instant timestamp, long value) {
        return NumberDataPoint.newBuilder()
            .setTimeUnixNano(toTimeUnixNanos(timestamp))
            .setAsInt(value);
    }

    private static NumberDataPoint.Builder point(Instant timestamp, double value) {
        return NumberDataPoint.newBuilder()
            .setTimeUnixNano(toTimeUnixNanos(timestamp))
            .setAsDouble(value);
    }

    private static Iterable<KeyValue> asAttributes(Map<String, String> labels, Set<String> excludeLabels) {
        return labels.entrySet().stream().filter(entry -> !excludeLabels.contains(entry.getKey())).map(
            entry -> KeyValue.newBuilder()
                .setKey(entry.getKey())
                .setValue(AnyValue.newBuilder().setStringValue(entry.getValue())).build()
        )::iterator;
    }

    private static long toTimeUnixNanos(Instant t) {
        return TimeUnit.SECONDS.toNanos(t.getEpochSecond()) + t.getNano();
    }
}
