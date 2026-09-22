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
package org.apache.kafka.common.metrics.stats;

import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.metrics.CompoundStat.NamedMeasurable;
import org.apache.kafka.common.metrics.CompoundStat.NamedMetricValueProvider;
import org.apache.kafka.common.metrics.MetricConfig;
import org.apache.kafka.common.metrics.MetricValueProvider;
import org.apache.kafka.common.metrics.stats.Percentiles.BucketSizing;

import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class PercentilesTest {

    // 1s windows (five samples) so windows close deterministically here.
    private final MetricConfig config = new MetricConfig().timeWindow(1, TimeUnit.SECONDS).samples(5);

    private MetricName name(String n) {
        return new MetricName(n, "grp", "", Map.of());
    }

    private Percentiles adaptive() {
        return new Percentiles(160, 8, new RawHistogram(name("latency-histogram")),
            new Percentile(name("latency-p50"), 50.0),
            new Percentile(name("latency-p99"), 99.0));
    }

    // Ten constant bins over [0, 100), so bounds 10, 20, ..., 90.
    private Percentiles fixed(RawHistogram rawHistogram) {
        Percentile p99 = new Percentile(name("latency-p99"), 99.0);
        return rawHistogram == null
            ? new Percentiles(40, 0.0, 100.0, BucketSizing.CONSTANT, p99)
            : new Percentiles(40, 0.0, 100.0, BucketSizing.CONSTANT, rawHistogram, p99);
    }

    @Test
    public void testConstructorGuards() {
        // The adaptive spec is validated up front, although stores are built lazily.
        assertThrows(IllegalArgumentException.class, () ->
            new Percentiles(1, 8, new Percentile(name("p50"), 50.0)));
        assertThrows(IllegalArgumentException.class, () ->
            new Percentiles(160, ExponentialHistogram.MAX_SCALE + 1, new Percentile(name("p50"), 50.0)));
        // The publishing constructors need something to publish under.
        assertThrows(NullPointerException.class, () ->
            new Percentiles(160, 8, (RawHistogram) null, new Percentile(name("p50"), 50.0)));
        assertThrows(NullPointerException.class, () ->
            new Percentiles(512, 1.0, 300_000.0, BucketSizing.EXPONENTIAL, (RawHistogram) null));
        // The fixed layouts keep their own range checks.
        assertThrows(IllegalArgumentException.class, () ->
            new Percentiles(512, 0.0, 300_000.0, BucketSizing.EXPONENTIAL, new RawHistogram(name("h"))));
        // A raw histogram needs a name to publish under.
        assertThrows(NullPointerException.class, () -> new RawHistogram(null));
    }

    @Test
    public void testAdaptiveWithoutRawHistogram() {
        Percentiles stat = new Percentiles(160, 8, new Percentile(name("latency-p50"), 50.0));
        assertEquals(1, stat.providers().size());   // just the percentile, no windows metric
        for (int i = 1; i <= 1000; i++) {
            stat.record(config, i, 0L);
        }
        assertEquals(500.0, percentile(stat, "latency-p50", 0L), 500.0 * 0.15);
    }

    @Test
    public void testOutOfRangeValueIsPinnedForBinningButKeptExact() {
        // LinearBinScheme rejects negative values, so Percentiles pins them to min = 0 for binning, as it always
        // has; the published window still reports the raw value.
        Percentiles stat = new Percentiles(40, 0.0, 100.0, BucketSizing.LINEAR,
            new RawHistogram(name("latency-histogram")), new Percentile(name("latency-p99"), 99.0));
        stat.record(config, -5.0, 0L);
        stat.record(config, 150.0, 0L);
        stat.record(config, 1.0, 1000L);   // closes the first window

        FixedHistogramWindow closed = assertInstanceOf(FixedHistogramWindow.class, windows(stat, 1000L).windows().get(0));
        assertEquals(1L, closed.binCounts()[0]);
        assertEquals(1L, closed.binCounts()[closed.binCounts().length - 1]);
        assertEquals(-5.0, closed.min(), 0.0);
        assertEquals(150.0, closed.max(), 0.0);
    }

    @Test
    public void testFixedReadReportsInfinityWhenTheQuantileIsNeverCrossed() {
        // As Percentiles has always done: the cumulative share never exceeds 1.0, so p100 is never crossed.
        Percentiles stat = new Percentiles(40, 0.0, 100.0, BucketSizing.CONSTANT, new Percentile(name("latency-p100"), 100.0));
        stat.record(config, 50.0, 0L);
        assertEquals(Double.POSITIVE_INFINITY, percentile(stat, "latency-p100", 0L), 0.0);
    }

    @Test
    public void testReadSkipsAPurgedSampleInTheRing() {
        // Three 1s samples; the first is purged (idle past samples x window) while later ones are still recorded.
        MetricConfig threeSamples = new MetricConfig().timeWindow(1, TimeUnit.SECONDS).samples(3);
        Percentiles stat = fixed(null);
        stat.record(threeSamples, 95.0, 0L);
        stat.record(threeSamples, 5.0, 1000L);
        stat.record(threeSamples, 5.0, 3500L);   // by now the 95 window has aged out and is purged on read
        assertEquals(0.0, stat.value(threeSamples, 3500L, 0.99), 0.0);
    }

    @Test
    public void testRingEvictsTheOldestWindow() {
        // Six 1s windows into a five-sample ring: the sixth reuses the first's slot.
        Percentiles stat = adaptive();
        for (int i = 0; i <= 5; i++) {
            stat.record(config, 10.0 + i, i * 1000L);
        }
        List<HistogramWindow> closed = windows(stat, 5000L).windows();
        assertEquals(4, closed.size());   // starts 1000..4000; the open one at 5000 is not closed yet
        assertEquals(1000L, closed.get(0).windowStartMs());
        assertEquals(11.0, closed.get(0).min(), 0.0);
        assertEquals(4000L, closed.get(3).windowStartMs());
    }

    @Test
    public void testIdleDataAgesOut() {
        // Nothing recorded for samples x window: the purge resets the sample, for publishing and percentiles alike.
        Percentiles stat = adaptive();
        stat.record(config, 10.0, 0L);
        assertEquals(1, windows(stat, 1000L).windows().size());
        assertEquals(10.0, percentile(stat, "latency-p50", 1000L), 10.0 * 0.05);

        assertTrue(windows(stat, 5000L).windows().isEmpty());
        assertEquals(Double.NaN, percentile(stat, "latency-p50", 5000L));
    }

    @Test
    public void testFixedModePublishesExplicitWindows() {
        Percentiles stat = fixed(new RawHistogram(name("latency-histogram")));
        assertEquals(2, stat.providers().size());   // the percentile + the raw histogram
        stat.record(config, 5.0, 0L);
        stat.record(config, 15.0, 0L);
        stat.record(config, 17.0, 0L);
        stat.record(config, 250.0, 0L);   // beyond max: clamped into the last bin
        stat.record(config, 1.0, 1000L);   // rotates: closes the first window

        HistogramWindows windows = windows(stat, 1000L);
        assertEquals(1, windows.windows().size());
        FixedHistogramWindow closed = assertInstanceOf(FixedHistogramWindow.class, windows.windows().get(0));
        assertArrayEquals(new double[]{10, 20, 30, 40, 50, 60, 70, 80, 90}, closed.bounds(), 1e-9);
        assertArrayEquals(new long[]{1, 2, 0, 0, 0, 0, 0, 0, 0, 1}, closed.binCounts());
        // count, sum, min and max are exact, including for the clamped value.
        assertEquals(4L, closed.count());
        assertEquals(287.0, closed.sum(), 0.0);
        assertEquals(5.0, closed.min(), 0.0);
        assertEquals(250.0, closed.max(), 0.0);
        assertEquals(0L, closed.windowStartMs());
        assertEquals(1000L, closed.windowEndMs());
    }

    @Test
    public void testFixedModeWindowsMergeIntoOneDelta() {
        Percentiles stat = fixed(new RawHistogram(name("latency-histogram")));
        stat.record(config, 5.0, 0L);
        stat.record(config, 15.0, 1000L);
        stat.record(config, 95.0, 2000L);   // closes the first two windows

        HistogramWindow merged = windows(stat, 2000L).mergedSince(Long.MIN_VALUE).orElseThrow();
        FixedHistogramWindow explicit = assertInstanceOf(FixedHistogramWindow.class, merged);
        assertArrayEquals(new long[]{1, 1, 0, 0, 0, 0, 0, 0, 0, 0}, explicit.binCounts());
        assertEquals(2L, explicit.count());
        assertEquals(0L, explicit.windowStartMs());
        assertEquals(2000L, explicit.windowEndMs());
        // Nothing newer than the merged window's end.
        assertTrue(windows(stat, 2000L).mergedSince(explicit.windowEndMs()).isEmpty());
    }

    @Test
    public void testWindowClosedByEventCountIsTaggedWithItsSuccessorsStart() {
        MetricConfig countConfig = new MetricConfig().timeWindow(1, TimeUnit.SECONDS).samples(5).eventWindow(2);
        Percentiles stat = adaptive();
        stat.record(countConfig, 1.0, 0L);
        stat.record(countConfig, 2.0, 50L);   // the first sample is now full, but nothing follows it yet
        assertTrue(windows(stat, countConfig, 50L).windows().isEmpty());

        stat.record(countConfig, 3.0, 100L);   // finds the first sample full: the successor starts here
        stat.record(countConfig, 4.0, 200L);
        stat.record(countConfig, 5.0, 300L);   // and again
        List<HistogramWindow> closed = windows(stat, countConfig, 300L).windows();
        assertEquals(2, closed.size());
        assertEquals(0L, closed.get(0).windowStartMs());
        assertEquals(100L, closed.get(0).windowEndMs());
        assertEquals(100L, closed.get(1).windowStartMs());
        assertEquals(300L, closed.get(1).windowEndMs());
        // The tags do not move on a later read.
        assertEquals(100L, windows(stat, countConfig, 900L).windows().get(0).windowEndMs());
    }

    @Test
    public void testRetentionForTheReaderDoesNotWidenThePercentiles() {
        // Two 1s samples. The raw histogram's reader asks for 10s, so the ring retains 11 samples, but the
        // percentiles still read over the config's two: the 95 recorded 3s ago drops out of p99 exactly as it
        // does for a stat that publishes nothing, while it is still retained for publishing.
        MetricConfig twoSamples = new MetricConfig().timeWindow(1, TimeUnit.SECONDS).samples(2);
        Percentiles publishing = fixed(new RawHistogram(name("latency-histogram")));
        rawHistogramProvider(publishing).retainFor(10_000L, twoSamples);
        Percentiles plain = fixed(null);
        for (Percentiles stat : List.of(publishing, plain)) {
            stat.record(twoSamples, 95.0, 0L);
            stat.record(twoSamples, 5.0, 3000L);
        }
        assertEquals(0.0, publishing.value(twoSamples, 3000L, 0.99), 0.0);
        assertEquals(0.0, plain.value(twoSamples, 3000L, 0.99), 0.0);
        assertEquals(1, windows(publishing, twoSamples, 3000L).windows().size());   // the 95 window is still published
    }

    @Test
    public void testProvidersExposePercentilesPlusHistogram() {
        List<NamedMetricValueProvider> ps = adaptive().providers();
        assertEquals(3, ps.size());   // two percentiles + the raw histogram
        NamedMetricValueProvider histogram = ps.stream()
            .filter(p -> p.name().name().equals("latency-histogram"))
            .findFirst().orElseThrow();
        assertInstanceOf(HistogramWindows.class, histogram.metricValueProvider().value(config, 0L));
    }

    @Test
    public void testRawHistogramIsNotJmxExportableAndRetainsForItsReader() {
        Percentiles stat = adaptive();
        MetricValueProvider<?> histogram = rawHistogramProvider(stat);
        assertFalse(histogram.jmxExportable());

        // Five minutes read with 1s windows: ceil(300 / 1) + 1, uncapped, and never shrunk by a shorter interval.
        // The config is left alone, so the percentiles keep reading over its five samples.
        MetricConfig ownConfig = new MetricConfig().timeWindow(1, TimeUnit.SECONDS).samples(5);
        assertEquals(5, stat.retainedSamples(ownConfig));
        histogram.retainFor(300_000L, ownConfig);
        assertEquals(301, stat.retainedSamples(ownConfig));
        histogram.retainFor(60_000L, ownConfig);
        assertEquals(301, stat.retainedSamples(ownConfig));
        assertEquals(5, ownConfig.samples());

        // The percentiles are ordinary metrics: exported over JMX, and they ignore retention.
        Percentiles other = adaptive();
        NamedMeasurable p50 = other.stats().get(0);
        assertTrue(p50.metricValueProvider().jmxExportable());
        p50.metricValueProvider().retainFor(300_000L, ownConfig);
        assertEquals(5, other.retainedSamples(ownConfig));
    }

    private static MetricValueProvider<?> rawHistogramProvider(Percentiles stat) {
        return stat.providers().stream()
            .filter(p -> p.name().name().equals("latency-histogram"))
            .findFirst().orElseThrow()
            .metricValueProvider();
    }

    @Test
    public void testFixedModeWithoutRawHistogramHasNoProvider() {
        assertEquals(1, fixed(null).providers().size());   // just the percentile, no windows metric
    }

    @Test
    public void testStatsExposesOnlyNumericPercentiles() {
        List<NamedMeasurable> numeric = adaptive().stats();
        assertEquals(2, numeric.size());
        assertEquals("latency-p50", numeric.get(0).name().name());
        assertEquals("latency-p99", numeric.get(1).name().name());
    }

    @Test
    public void testOneRecordingFeedsPercentilesAndHistogram() {
        Percentiles stat = adaptive();
        for (int i = 1; i <= 1000; i++)
            stat.record(config, i, 0L);   // all inside the first (open) window

        assertEquals(500.0, percentile(stat, "latency-p50", 0L), 500.0 * 0.15);
        assertTrue(windows(stat, 0L).windows().isEmpty());   // the open window is never shipped

        // Advancing past the boundary with a later record closes the first window.
        stat.record(config, 1.0, 1000L);
        HistogramWindows windows = windows(stat, 1000L);
        assertEquals(1, windows.windows().size());
        AdaptiveHistogramWindow closed =
            assertInstanceOf(AdaptiveHistogramWindow.class, windows.windows().get(0));
        assertEquals(1000L, closed.count());
        assertEquals(500500.0, closed.sum(), 0.0);   // raw-value sum of 1..1000
        assertEquals(1.0, closed.min(), 0.0);
        assertEquals(1000.0, closed.max(), 0.0);
        assertEquals(0L, closed.windowStartMs());
        assertEquals(1000L, closed.windowEndMs());

        // Reads do not drain: the same closed window is still there on a second read.
        assertEquals(1, windows(stat, 1000L).windows().size());
    }

    private double percentile(Percentiles stat, String metricName, long now) {
        return stat.stats().stream()
            .filter(s -> s.name().name().equals(metricName))
            .findFirst().orElseThrow()
            .stat().measure(config, now);
    }

    private HistogramWindows windows(Percentiles stat, long now) {
        return windows(stat, config, now);
    }

    private HistogramWindows windows(Percentiles stat, MetricConfig config, long now) {
        return (HistogramWindows) stat.providers().stream()
            .filter(p -> p.name().name().equals("latency-histogram"))
            .findFirst().orElseThrow()
            .metricValueProvider().value(config, now);
    }
}
