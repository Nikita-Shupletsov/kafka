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

import org.apache.kafka.common.annotation.InterfaceAudience;
import org.apache.kafka.common.metrics.CompoundStat;
import org.apache.kafka.common.metrics.MetricConfig;
import org.apache.kafka.common.metrics.MetricValueProvider;
import org.apache.kafka.common.metrics.stats.Histogram.BinScheme;
import org.apache.kafka.common.metrics.stats.Histogram.ConstantBinScheme;
import org.apache.kafka.common.metrics.stats.Histogram.ExponentialBinScheme;
import org.apache.kafka.common.metrics.stats.Histogram.LinearBinScheme;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

/**
 * A compound stat that reports one or more percentiles off the {@link SampledStat} ring — a small ring
 * of time windows that rotates on record and ages out on read. The constructor selects only the
 * per-window store and the quantile arithmetic over it; everything else (rotation, recycling, the window
 * length and sample count from {@link MetricConfig}) is the shared ring:
 * <ul>
 *   <li>a {@link BucketSizing} ({@code CONSTANT}, {@code LINEAR}, {@code EXPONENTIAL}) gives a fixed-bin
 *       {@link Histogram} per sample over a declared range;</li>
 *   <li>a bin budget and initial scale give an adaptive base-2 exponential histogram per sample over values
 *       from 1 up, whose scale bends to the data and which needs no declared upper bound.</li>
 * </ul>
 * Either way a percentile is read by walking the samples' bins in place, through the store's
 * reader, without copying them.
 *
 * <p>Either store can additionally publish its closed windows ({@link HistogramWindows}) for the telemetry
 * collector, by passing a {@link RawHistogram} that names them.
 */
@InterfaceAudience.Public
public class Percentiles extends SampledStat implements CompoundStat {

    private static final Logger log = LoggerFactory.getLogger(Percentiles.class);

    public enum BucketSizing {
        CONSTANT, LINEAR, EXPONENTIAL
    }

    private final Percentile[] percentiles;
    // Everything that depends on the kind of store: building one, reading percentiles off it, publishing it.
    private final StoreKind storeKind;
    // Names the published windows; null when nothing is published.
    private final RawHistogram rawHistogram;
    // Samples the raw histogram's reader needs the ring to retain, set by retainFor; 0 until then.
    private int readerSamples;

    public Percentiles(int sizeInBytes, double max, BucketSizing bucketing, Percentile... percentiles) {
        this(sizeInBytes, 0.0, max, bucketing, percentiles);
    }

    public Percentiles(int sizeInBytes, double min, double max, BucketSizing bucketing, Percentile... percentiles) {
        this(new FixedStoreKind(binScheme(sizeInBytes / 4, min, max, bucketing), min, max), null, percentiles);
    }

    /**
     * A fixed-bin store, as {@link #Percentiles(int, double, double, BucketSizing, Percentile...)}, that
     * also publishes its closed windows under {@code rawHistogram}'s name as {@link FixedHistogramWindow}s.
     */
    public Percentiles(int sizeInBytes, double min, double max, BucketSizing bucketing,
                       RawHistogram rawHistogram, Percentile... percentiles) {
        this(new FixedStoreKind(binScheme(sizeInBytes / 4, min, max, bucketing), min, max),
            Objects.requireNonNull(rawHistogram, "rawHistogram"), percentiles);
    }

    /**
     * An adaptive base-2 exponential store per sample, over values from 1 up: it starts each window at
     * {@code initialScale} and downscales whenever the occupied span would exceed {@code maxBins}, so no
     * upper bound is declared and nothing above 1 is clamped. A value below 1 is counted in the first bin.
     */
    public Percentiles(int maxBins, int initialScale, Percentile... percentiles) {
        this(new AdaptiveStoreKind(maxBins, initialScale), null, percentiles);
    }

    /**
     * An adaptive store, as {@link #Percentiles(int, int, Percentile...)}, that also publishes its closed
     * windows under {@code rawHistogram}'s name as {@link AdaptiveHistogramWindow}s.
     */
    public Percentiles(int maxBins, int initialScale, RawHistogram rawHistogram, Percentile... percentiles) {
        this(new AdaptiveStoreKind(maxBins, initialScale), Objects.requireNonNull(rawHistogram, "rawHistogram"),
            percentiles);
    }

    private Percentiles(StoreKind storeKind, RawHistogram rawHistogram, Percentile[] percentiles) {
        super(0.0);
        this.percentiles = percentiles;
        this.storeKind = storeKind;
        this.rawHistogram = rawHistogram;
    }

    private static BinScheme binScheme(int bins, double min, double max, BucketSizing bucketing) {
        if (bucketing == BucketSizing.CONSTANT) {
            return new ConstantBinScheme(bins, min, max);
        } else if (bucketing == BucketSizing.LINEAR) {
            if (min != 0.0d)
                throw new IllegalArgumentException("Linear bucket sizing requires min to be 0.0.");
            return new LinearBinScheme(bins, max);
        } else if (bucketing == BucketSizing.EXPONENTIAL) {
            if (min <= 0.0d)
                throw new IllegalArgumentException("Exponential bucket sizing requires min > 0.0.");
            return new ExponentialBinScheme(bins, min, max);
        } else {
            throw new IllegalArgumentException("Unknown bucket type: " + bucketing);
        }
    }

    @Override
    public List<NamedMeasurable> stats() {
        List<NamedMeasurable> ms = new ArrayList<>(this.percentiles.length);
        for (Percentile percentile : this.percentiles) {
            final double pct = percentile.percentile();
            ms.add(new NamedMeasurable(
                percentile.name(),
                (config, now) -> value(config, now, pct / 100.0))
            );
        }
        return ms;
    }

    @Override
    public List<NamedMetricValueProvider> providers() {
        List<NamedMetricValueProvider> ms = new ArrayList<>(stats());
        // Publish the ring's closed windows under the raw histogram's name. The value is non-numeric
        // (HistogramWindows) and the read only ages the ring (the purge it does anyway), so the telemetry
        // collector keeps a cursor over the window ends and ships only the windows newer than it.
        if (rawHistogram != null) {
            ms.add(new NamedMetricValueProvider(rawHistogram.name(), new WindowsProvider()));
        }
        return ms;
    }

    // The raw histogram's value: the ring's closed windows. It has no JMX representation, and its reader must see
    // every window, so it has the ring retain enough of them to cover the reader's interval.
    private final class WindowsProvider implements MetricValueProvider<HistogramWindows> {
        @Override
        public HistogramWindows value(MetricConfig config, long now) {
            return new HistogramWindows(closedWindows(config, now));
        }

        @Override
        public boolean jmxExportable() {
            return false;
        }

        /*
         ceil(readInterval / window) + 1 samples. The + 1 covers the purge, which evicts by idle time from a
         sample's last event and so can drop a window up to one window length early. Retention only grows: a
         later, shorter interval leaves spare windows, which is harmless. The config is left alone, so the
         percentiles keep reading over config.samples().
        */
        @Override
        public void retainFor(long readIntervalMs, MetricConfig config) {
            long windowMs = config.timeWindowMs();
            long needed = (readIntervalMs + windowMs - 1) / windowMs + 1;
            readerSamples = (int) Math.max(readerSamples, Math.min(needed, Integer.MAX_VALUE));
        }
    }

    // The ring retains enough samples for the percentiles (config.samples()) and for the raw histogram's reader,
    // whichever is more.
    @Override
    int retainedSamples(MetricConfig config) {
        return Math.max(config.samples(), readerSamples);
    }

    public double value(MetricConfig config, long now, double quantile) {
        purgeObsoleteSamples(config, now);
        // The percentiles read the samples the ring would hold if it retained only config.samples(), as for every
        // other SampledStat: the newest config.samples() + 1 samples, less those idle for config.samples() windows.
        // Any further samples are retained for the raw histogram's reader only.
        int maxSamples = config.samples() + 1;
        long expireAge = (long) config.samples() * config.timeWindowMs();
        int[] visited = {0};
        QuantileReader reader = storeKind.newReader();
        walkNewestFirst(config, (sample, end) -> {
            if (visited[0]++ == maxSamples || now - sample.lastEventMs >= expireAge) {
                return false;
            }
            reader.add(store(sample));
            return true;
        });
        return reader.valueAt(quantile);
    }

    // The completed windows, oldest first: samples that hold observations and can no longer receive any,
    // either because their time window has elapsed or because the ring has advanced past them (a sample
    // closed early by the config's event window). Each is tagged with its [start, end]. purge has already
    // aged out the obsolete.
    private List<HistogramWindow> closedWindows(MetricConfig config, long now) {
        purgeObsoleteSamples(config, now);
        List<HistogramWindow> closed = new ArrayList<>();
        walkNewestFirst(config, (sample, end) -> {
            if (end <= now) {
                closed.add(storeKind.snapshot(store(sample)).withWindow(sample.startTimeMs, end));
            }
            return true;
        });
        Collections.reverse(closed);
        return closed;
    }

    /*
     Visits the samples that hold observations, newest first, each with the end of its window, until the visitor
     returns false. The ring's slots are written in index order, wrapping, so walking back from the newest slot
     visits the samples in descending time; slots with no observations (unused, or reset by the purge) are
     skipped, which changes nothing since they contribute nothing.

     A window ends at min(start + window, the next newer sample's start). For a sample closed by time the next
     one started at or after start + window, so this is the nominal window; for one closed by event count the
     next one started at the record that found it full, so this is exact. Either way consecutive windows tile,
     and the end never changes once the sample is closed. The newest sample gets the nominal end.
    */
    private void walkNewestFirst(MetricConfig config, WindowVisitor visitor) {
        int n = this.samples.size();
        int newest = -1;
        for (int i = 0; i < n; i++) {
            Sample s = this.samples.get(i);
            if (s.eventCount > 0 && (newest < 0 || s.startTimeMs > this.samples.get(newest).startTimeMs)) {
                newest = i;
            }
        }
        if (newest < 0) {
            return;
        }
        long nextStart = Long.MAX_VALUE;
        for (int k = 0; k < n; k++) {
            Sample s = this.samples.get((newest - k + n) % n);
            if (s.eventCount == 0) {
                continue;
            }
            long windowMs = s.timeWindowMs > 0 ? s.timeWindowMs : config.timeWindowMs();
            if (!visitor.visit(s, Math.min(s.startTimeMs + windowMs, nextStart))) {
                return;
            }
            nextStart = s.startTimeMs;
        }
    }

    private interface WindowVisitor {
        boolean visit(Sample sample, long windowEnd);
    }

    private static RecordingStore store(Sample sample) {
        return ((HistogramSample) sample).store;
    }

    @Override
    public double combine(List<Sample> samples, MetricConfig config, long now) {
        return value(config, now, 0.5);
    }

    @Override
    protected Sample newSample(long timeMs) {
        return new HistogramSample(storeKind.newStore(), timeMs);
    }

    @Override
    protected void update(Sample sample, MetricConfig config, double value, long timeMs) {
        storeKind.record(((HistogramSample) sample).store, value);
    }

    // The operations that depend on the kind of store, chosen once at construction.
    private interface StoreKind {
        RecordingStore newStore();

        // Records the raw value; the store does the binning (toBin for a fixed Histogram, log-scale indexing
        // for an adaptive ExponentialHistogram).
        default void record(RecordingStore store, double value) {
            store.record(value);
        }

        QuantileReader newReader();

        // An immutable copy of one store, for publishing a closed window.
        HistogramWindow snapshot(RecordingStore store);
    }

    private static final class FixedStoreKind implements StoreKind {
        private final BinScheme binScheme;
        private final double min;
        private final double max;
        // The interior bin boundaries every published window shares; computed on the first one.
        private double[] bounds;

        FixedStoreKind(BinScheme binScheme, double min, double max) {
            this.binScheme = binScheme;
            this.min = min;
            this.max = max;
        }

        // A value outside the declared range is pinned to it for binning, as Percentiles always has, so it lands
        // in the edge bin and never reaches a scheme that rejects it. The store keeps the raw value for the
        // published window's sum, min and max.
        @Override
        public void record(RecordingStore store, double value) {
            double pinned = value;
            if (value > max) {
                log.debug("Received value {} which is greater than max recordable value {}, will be pinned to the max value",
                    value, max);
                pinned = max;
            } else if (value < min) {
                log.debug("Received value {} which is less than min recordable value {}, will be pinned to the min value",
                    value, min);
                pinned = min;
            }
            ((Histogram) store).record(value, pinned);
        }

        @Override
        public RecordingStore newStore() {
            return new Histogram(binScheme);
        }

        @Override
        public QuantileReader newReader() {
            return new FixedQuantileReader(binScheme);
        }

        @Override
        public HistogramWindow snapshot(RecordingStore store) {
            if (bounds == null) {
                bounds = Histogram.bounds(binScheme);
            }
            return ((Histogram) store).snapshot(bounds);
        }
    }

    private static final class AdaptiveStoreKind implements StoreKind {
        private final int maxBins;
        private final int initialScale;

        AdaptiveStoreKind(int maxBins, int initialScale) {
            // Validated up front, although stores are only built on the first record.
            ExponentialHistogram.validate(maxBins, initialScale);
            this.maxBins = maxBins;
            this.initialScale = initialScale;
        }

        @Override
        public RecordingStore newStore() {
            return new ExponentialHistogram(maxBins, initialScale);
        }

        @Override
        public QuantileReader newReader() {
            return new ExponentialQuantileReader();
        }

        @Override
        public HistogramWindow snapshot(RecordingStore store) {
            return ((ExponentialHistogram) store).snapshot();
        }
    }
}
