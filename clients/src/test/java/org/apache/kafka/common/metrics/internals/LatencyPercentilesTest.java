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
package org.apache.kafka.common.metrics.internals;

import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.metrics.KafkaMetric;
import org.apache.kafka.common.metrics.MetricConfig;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.metrics.Sensor;
import org.apache.kafka.common.metrics.stats.AdaptiveHistogramWindow;
import org.apache.kafka.common.metrics.stats.HistogramWindows;
import org.apache.kafka.common.utils.MockTime;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class LatencyPercentilesTest {

    private MockTime time;
    private Metrics metrics;

    @BeforeEach
    public void setup() {
        time = new MockTime();
        metrics = new Metrics(new MetricConfig(), new ArrayList<>(), time);
    }

    @AfterEach
    public void tearDown() {
        metrics.close();
    }

    @Test
    public void testOneAddRegistersPercentilesAndHistogram() {
        MetricName scalarBase = metrics.metricName("request-latency", "producer-node-metrics", Map.of("node-id", "node-1"));
        Sensor sensor = metrics.sensor("node-1.latency");
        LatencyPercentiles.addTo(sensor, scalarBase, metrics.config());

        // Percentiles are ordinary numeric metrics.
        assertNoMetric("request-latency-p90", "producer-node-metrics");
        for (String p : new String[]{"p50", "p95", "p99"}) {
            MetricName name = metrics.metricName("request-latency-" + p, "producer-node-metrics", Map.of("node-id", "node-1"));
            assertTrue(metrics.metrics().containsKey(name), "expected percentile metric request-latency-" + p);
            assertInstanceOf(Double.class, metrics.metric(name).metricValue());
        }

        // The raw histogram is under the "-histogram" sibling name; its value is the ring's closed windows.
        MetricName histogram = LatencyPercentiles.histogramMetricName(scalarBase);
        assertEquals("request-latency-histogram", histogram.name());
        assertInstanceOf(HistogramWindows.class, metrics.metric(histogram).metricValue());
        // No bare "request-latency" metric (independent selection; no prefix collision).
        assertNoMetric("request-latency", "producer-node-metrics");
    }

    @Test
    public void testOneRecordingPathFeedsBothPercentilesAndHistogram() {
        MetricName scalarBase = metrics.metricName("request-latency", "producer-node-metrics", Map.of());
        Sensor sensor = metrics.sensor("latency");
        LatencyPercentiles.addTo(sensor, scalarBase, metrics.config());
        MetricName histogram = LatencyPercentiles.histogramMetricName(scalarBase);
        MetricName p50 = metrics.metricName("request-latency-p50", "producer-node-metrics", Map.of());

        for (int i = 1; i <= 1000; i++) {
            sensor.record(i);   // all in the first (open) window at t=0
        }

        // Percentiles come from the adaptive ring's recent window.
        assertEquals(500.0, (double) metrics.metric(p50).metricValue(), 500.0 * 0.15);
        // The open window is never in the raw output.
        assertTrue(windows(histogram).windows().isEmpty());

        // Cross one 30s window boundary (the client default) and record once, closing the first window.
        time.sleep(30_000);
        sensor.record(1.0);

        HistogramWindows windows = windows(histogram);
        assertEquals(1, windows.windows().size());
        AdaptiveHistogramWindow closed =
            assertInstanceOf(AdaptiveHistogramWindow.class, windows.windows().get(0));
        assertEquals(1000L, closed.count());
        assertEquals(500500.0, closed.sum(), 0.0);
        assertEquals(1.0, closed.min(), 0.0);
        assertEquals(1000.0, closed.max(), 0.0);
        assertTrue(closed.binCounts().length > 0);
    }

    @Test
    public void testUsesTheClientConfig() {
        MetricConfig clientConfig = new MetricConfig().samples(3).timeWindow(10, TimeUnit.SECONDS);
        MetricName scalarBase = metrics.metricName("request-latency", "producer-node-metrics", Map.of());
        LatencyPercentiles.addTo(metrics.sensor("latency"), scalarBase, clientConfig);

        MetricName p50 = metrics.metricName("request-latency-p50", "producer-node-metrics", Map.of());
        MetricName histogram = LatencyPercentiles.histogramMetricName(scalarBase);
        // Retention for telemetry never changes the config, so the stat can share the client's.
        assertSame(clientConfig, metrics.metric(histogram).config());
        assertSame(clientConfig, metrics.metric(p50).config());
    }

    @Test
    public void testPercentileSpanIsSamplesTimesWindow() {
        // Two 30s samples by default: the percentiles look back 60s even when the ring retains more for telemetry.
        MetricName scalarBase = metrics.metricName("request-latency", "producer-node-metrics", Map.of());
        Sensor sensor = metrics.sensor("latency");
        LatencyPercentiles.addTo(sensor, scalarBase, metrics.config());
        MetricName p99 = metrics.metricName("request-latency-p99", "producer-node-metrics", Map.of());
        // What the telemetry reporter does on subscription: six minutes of 30s windows.
        metrics.metric(LatencyPercentiles.histogramMetricName(scalarBase)).retainFor(360_000L);

        sensor.record(1000.0);
        time.sleep(30_000);
        sensor.record(10.0);
        time.sleep(30_000);
        sensor.record(10.0);
        time.sleep(30_000);
        sensor.record(10.0);
        // The 1000 ms window ended 60s ago, so it is retained but outside the percentile span.
        assertEquals(10.0, (double) metrics.metric(p99).metricValue(), 10.0 * 0.03);
        assertEquals(3, windows(LatencyPercentiles.histogramMetricName(scalarBase)).windows().size());
    }

    /**
     * Asserts that the standard latency distribution for {@code base} is registered: numeric percentiles and the
     * raw histogram. For the registration sites' own tests.
     */
    public static void assertLatencyDistributionRegistered(Metrics metrics, String base, String group,
                                                           Map<String, String> tags) {
        for (String suffix : List.of("-p50", "-p95", "-p99")) {
            KafkaMetric percentile = metrics.metric(new MetricName(base + suffix, group, "", tags));
            assertNotNull(percentile, "expected a metric " + group + ":" + base + suffix + " " + tags);
            assertInstanceOf(Double.class, percentile.metricValue());
        }
        KafkaMetric histogram = metrics.metric(new MetricName(base + LatencyPercentiles.HISTOGRAM_SUFFIX, group, "", tags));
        assertNotNull(histogram, "expected a metric " + group + ":" + base + LatencyPercentiles.HISTOGRAM_SUFFIX + " " + tags);
        assertInstanceOf(HistogramWindows.class, histogram.metricValue());
    }

    private HistogramWindows windows(MetricName histogram) {
        return (HistogramWindows) metrics.metric(histogram).metricValue();
    }

    private void assertNoMetric(String name, String group) {
        boolean present = metrics.metrics().keySet().stream()
            .anyMatch(m -> m.name().equals(name) && m.group().equals(group));
        assertTrue(!present, "did not expect a metric " + group + ":" + name);
    }
}
