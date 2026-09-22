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
import org.apache.kafka.common.metrics.KafkaMetric;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.metrics.Sensor;

import org.junit.jupiter.api.Test;

import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.apache.kafka.coordinator.common.runtime.KafkaMetricHistogram.MAX_LATENCY_MS;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class KafkaMetricHistogramTest {

    @Test
    public void testStats() {
        try (Metrics metrics = new Metrics()) {
            KafkaMetricHistogram histogram = KafkaMetricHistogram.newLatencyHistogram(
                suffix -> metrics.metricName(
                    "test-metric-" + suffix,
                    "test-group",
                    "test description"
                )
            );

            Set<MetricName> expected = Set.of(
                new MetricName("test-metric-max", "test-group", "test description", Map.of()),
                new MetricName("test-metric-p999", "test-group", "test description", Map.of()),
                new MetricName("test-metric-p99", "test-group", "test description", Map.of()),
                new MetricName("test-metric-p95", "test-group", "test description", Map.of()),
                new MetricName("test-metric-p50", "test-group", "test description", Map.of())
            );
            Set<MetricName> actual = histogram.stats().stream().map(CompoundStat.NamedMeasurable::name).collect(Collectors.toSet());
            assertEquals(expected, actual);
        }
    }

    @Test
    public void testMaxIsExactAndPercentilesAreMonotonic() {
        try (Metrics metrics = new Metrics()) {
            KafkaMetricHistogram histogram = KafkaMetricHistogram.newLatencyHistogram(
                suffix -> metrics.metricName("lat-" + suffix, "grp", "desc"));
            Sensor sensor = metrics.sensor("latency");
            sensor.add(histogram);

            // Record 1..1000ms.
            IntStream.rangeClosed(1, 1000).forEach(sensor::record);

            // max is tracked exactly (not binned).
            assertEquals(1000.0, value(metrics, "lat-max"));

            double p50 = value(metrics, "lat-p50");
            double p95 = value(metrics, "lat-p95");
            double p99 = value(metrics, "lat-p99");
            double p999 = value(metrics, "lat-p999");
            // Percentiles are ordered and each within two 0.25% bins of the true quantile.
            assertTrue(p50 < p95 && p95 < p99 && p99 < p999, "percentiles must be monotonic");
            assertEquals(500.0, p50, 500.0 * 0.005);
            assertEquals(999.0, p999, 999.0 * 0.005);
        }
    }

    @Test
    public void testRecordingIsCappedToMaxLatency() {
        try (Metrics metrics = new Metrics()) {
            KafkaMetricHistogram histogram = KafkaMetricHistogram.newLatencyHistogram(
                suffix -> metrics.metricName("lat-" + suffix, "grp", "desc"));
            Sensor sensor = metrics.sensor("latency");
            sensor.add(histogram);

            // Values above the trackable ceiling are capped rather than rejected or overflowing.
            IntStream.range(0, 100).forEach(__ -> sensor.record(MAX_LATENCY_MS + 5000L));
            assertEquals((double) MAX_LATENCY_MS, value(metrics, "lat-max"));
        }
    }

    private static double value(Metrics metrics, String name) {
        KafkaMetric metric = metrics.metrics().get(metrics.metricName(name, "grp", "desc"));
        return (Double) metric.metricValue();
    }
}
