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
package org.apache.kafka.coordinator.common.runtime;

import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.metrics.CompoundStat;
import org.apache.kafka.common.metrics.MetricConfig;
import org.apache.kafka.common.metrics.stats.Max;
import org.apache.kafka.common.metrics.stats.Percentile;
import org.apache.kafka.common.metrics.stats.Percentiles;
import org.apache.kafka.common.metrics.stats.Percentiles.BucketSizing;
import org.apache.kafka.common.utils.Utils;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

/**
 * A compound stat providing a {@code max} plus a fixed set of latency percentiles, backed by the
 * standard Kafka Metrics histogram framework ({@link Percentiles} over geometric/log-scale bins,
 * plus a {@link Max}). Log-scale bins give uniform <em>relative</em> precision across the whole
 * range, which is the right model for latencies (values of interest span orders of magnitude), the
 * same property the previous HdrHistogram-based implementation provided.
 *
 * <p>Because it is a {@link CompoundStat} recorded through a {@code Sensor}, updates are already
 * serialized by the sensor lock; the percentile bins and the max share that single storage model
 * rather than a separate concurrent data structure.
 */
public final class KafkaMetricHistogram implements CompoundStat {

    /**
     * Max latency value.
     */
    public static final long MAX_LATENCY_MS = Duration.ofMinutes(1).toMillis();

    /**
     * The smallest trackable value. Log-scale bins require a strictly positive lower bound;
     * latencies are recorded in whole milliseconds, so 1ms is the natural floor.
     */
    private static final double MIN_LATENCY_MS = 1.0;

    /**
     * Target relative error of the percentile bins (i.e. the width of each geometric bin).
     * 0.25% over 1 ms to 60 s takes about 4,400 bins, close to the precision of the HdrHistogram this
     * replaced at under a third of its memory.
     */
    private static final double RELATIVE_ERROR = 0.0025;

    /**
     * Suffix used for the histogram's max value.
     */
    private static final String MAX_NAME = "max";

    /**
     * Set list of percentiles we will provide metrics for.
     */
    private static final Map<Double, String> PERCENTILE_NAMES =
        Utils.mkMap(
            Utils.mkEntry(50.0, "p50"),
            Utils.mkEntry(95.0, "p95"),
            Utils.mkEntry(99.0, "p99"),
            Utils.mkEntry(99.9, "p999")
        );

    /**
     * Factory for creating a {@link MetricName} based on a metric name with suffix.
     */
    private final Function<String, MetricName> metricNameFactory;
    private final long highestTrackableValue;
    private final Percentiles percentiles;
    private final Max max;

    /**
     * Creates a new histogram with the purpose of tracking latency values, exposing a {@code max}
     * and the {@code p50}/{@code p95}/{@code p99}/{@code p999} percentiles over the range
     * {@code [0, MAX_LATENCY_MS]}.
     *
     * @param metricNameFactory A factory for creating a {@link MetricName} based on a metric name
     *                          string.
     * @return A new {@link KafkaMetricHistogram} with the configured properties.
     */
    public static KafkaMetricHistogram newLatencyHistogram(
        Function<String, MetricName> metricNameFactory
    ) {
        return new KafkaMetricHistogram(metricNameFactory, MAX_LATENCY_MS);
    }

    private KafkaMetricHistogram(
        Function<String, MetricName> metricNameFactory,
        long highestTrackableValue
    ) {
        this.metricNameFactory = metricNameFactory;
        this.highestTrackableValue = highestTrackableValue;

        Percentile[] percentileConfigs = PERCENTILE_NAMES.entrySet().stream()
            .map(e -> new Percentile(metricNameFactory.apply(e.getValue()), e.getKey()))
            .toArray(Percentile[]::new);
        int bins = binsFor(MIN_LATENCY_MS, highestTrackableValue, RELATIVE_ERROR);
        this.percentiles = new Percentiles(
            bins * 4,
            MIN_LATENCY_MS,
            highestTrackableValue,
            BucketSizing.EXPONENTIAL,
            percentileConfigs);
        this.max = new Max();
    }

    /**
     * The number of geometric bins needed to cover {@code [min, max]} with the given per-bin
     * relative error (each bin spans a factor of {@code 1 + relativeError}).
     */
    private static int binsFor(double min, double max, double relativeError) {
        return (int) Math.ceil(Math.log(max / min) / Math.log(1 + relativeError));
    }

    @Override
    public List<NamedMeasurable> stats() {
        List<NamedMeasurable> stats = new ArrayList<>();
        stats.add(new NamedMeasurable(metricNameFactory.apply(MAX_NAME), max));
        stats.addAll(percentiles.stats());
        return stats;
    }

    @Override
    public void record(MetricConfig config, double value, long timeMs) {
        double capped = Math.min(Math.max(value, 0.0), highestTrackableValue);
        percentiles.record(config, capped, timeMs);
        max.record(config, capped, timeMs);
    }
}
