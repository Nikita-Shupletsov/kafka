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

import java.util.Arrays;

/**
 * A base-2 exponential (log-scale) histogram over values of at least 1, with a hard cap on the number of
 * bins it keeps.
 *
 * <p>A value {@code v} maps to bin index {@code floor(log2(v) * 2^scale)}, so bin {@code i} covers
 * {@code [base^i, base^(i+1))} where {@code base = 2^(2^-scale)}. This gives <em>constant relative
 * precision</em> at every magnitude (≈{@code base - 1} per bin), so the same instance resolves
 * milliseconds and minutes equally well without pre-declaring an upper bound.
 *
 * <p>The lower bound is 1: a value below it, including zero or a negative value, is counted in the first
 * bin, {@code [1, base)}, as a fixed-range {@link Histogram} counts it in its first bin. The store is
 * meant for latencies in milliseconds, which are recorded as whole milliseconds, so this only merges 0 ms
 * into 1 ms. Indices are therefore never negative.
 *
 * <p>{@code maxBins} caps the occupied span. When a newly recorded value would push the span over it,
 * the histogram <em>downscales</em>: it lowers {@code scale} by one, which merges every adjacent pair of
 * bins ({@code index -> index >> 1}). Merging is lossless in counts and only halves resolution, and it
 * repeats until the span fits. Because {@code scale} only ever decreases, two histograms can always be
 * merged by reducing the finer one to the coarser one's scale.
 *
 * <p>{@code count}, {@code sum}, {@code min} and {@code max} are tracked exactly, including for values
 * below 1, and remain meaningful even if the bins are too coarse to resolve the middle of the
 * distribution.
 *
 * <p>Instances are not thread-safe; callers that record and read concurrently must synchronize
 * externally ({@link Percentiles} is recorded and read under its sensor's lock).
 */
class ExponentialHistogram implements RecordingStore {

    /** OTLP caps the scale at 20; finer than this yields no practical benefit and risks overflow. */
    static final int MAX_SCALE = 20;

    private static final double LN2 = Math.log(2);

    private final int maxBins;
    private final int initialScale;

    private int scale;
    private final Bins bins;

    private long count;
    private double sum;
    private double min = Double.POSITIVE_INFINITY;
    private double max = Double.NEGATIVE_INFINITY;

    ExponentialHistogram(int maxBins, int initialScale) {
        validate(maxBins, initialScale);
        this.maxBins = maxBins;
        this.initialScale = initialScale;
        this.scale = initialScale;
        this.bins = new Bins(maxBins);
    }

    // Shared with Percentiles, which validates its adaptive spec up front but builds stores lazily.
    static void validate(int maxBins, int initialScale) {
        if (maxBins < 2) {
            throw new IllegalArgumentException("maxBins must be at least 2");
        }
        if (initialScale > MAX_SCALE) {
            throw new IllegalArgumentException("initialScale must be at most " + MAX_SCALE);
        }
    }

    @Override
    public void record(double value) {
        if (Double.isNaN(value) || Double.isInfinite(value)) {
            return;
        }

        count++;
        sum += value;
        if (value < min) {
            min = value;
        }
        if (value > max) {
            max = value;
        }

        // Downscale until the occupied span fits the budget. Indices are never negative, so repeated
        // halving brings every index to 0 eventually, which always fits.
        int idx = index(Math.max(value, 1.0), scale);
        while (bins.spanIfAdded(idx) > maxBins) {
            bins.downscale();
            scale--;
            idx = index(Math.max(value, 1.0), scale);
        }
        bins.record(idx);
    }

    /**
     * Bin index for a value of at least 1 at the given scale: {@code floor(log2(value) * 2^scale)}.
     */
    static int index(double value, int scale) {
        // log(value) / ln2 * 2^scale == log2(value) * 2^scale
        double scaled = Math.scalb(Math.log(value) / LN2, scale);
        return (int) Math.floor(scaled);
    }

    AdaptiveHistogramWindow snapshot() {
        double snapMin = count == 0 ? Double.NaN : min;
        double snapMax = count == 0 ? Double.NaN : max;
        return new AdaptiveHistogramWindow(scale, bins.offset(), bins.toArray(), count, sum, snapMin, snapMax);
    }

    @Override
    public void clear() {
        bins.reset();
        scale = initialScale;
        count = 0;
        sum = 0.0;
        min = Double.POSITIVE_INFINITY;
        max = Double.NEGATIVE_INFINITY;
    }

    int scale() {
        return scale;
    }

    long count() {
        return count;
    }

    double max() {
        return count == 0 ? Double.NaN : max;
    }

    // Read in place by ExponentialQuantileReader.
    Bins bins() {
        return bins;
    }

    // Visible for testing: occupied span of the bins.
    int occupiedSpan() {
        return bins.span();
    }

    /**
     * The log bins: a fixed-size count array plus the bookkeeping that maps a logical bin index to a
     * physical array slot ({@code slot = index - indexBase}). Scale lives in the enclosing histogram; this
     * class only stores counts and merges/rebases them, both of which are scale-independent.
     */
    static final class Bins {

        private final int maxBins;
        private long[] counts;
        private int indexBase;
        // Occupied slots [minPos, maxPos] inclusive; maxPos < minPos means empty.
        private int minPos;
        private int maxPos;

        Bins(int maxBins) {
            this.maxBins = maxBins;
            // counts is allocated lazily on first record, so a histogram that never records costs no array memory.
            this.minPos = 0;
            this.maxPos = -1;
        }

        boolean isEmpty() {
            return maxPos < minPos;
        }

        /** The occupied span this store would have if a value in bin {@code idx} were added. */
        int spanIfAdded(int idx) {
            if (isEmpty()) {
                return 1;
            }
            int occMin = Math.min(idx, indexBase + minPos);
            int occMax = Math.max(idx, indexBase + maxPos);
            return occMax - occMin + 1;
        }

        /** Records one observation in bin {@code idx}; the caller must have ensured it fits the budget. */
        void record(int idx) {
            if (counts == null) {
                counts = new long[maxBins];
            }
            if (isEmpty()) {
                // Center the first value so the window has headroom in both directions.
                int pos = maxBins / 2;
                indexBase = idx - pos;
                counts[pos]++;
                minPos = pos;
                maxPos = pos;
                return;
            }
            if (idx - indexBase < 0 || idx - indexBase >= maxBins) {
                rebase(Math.min(idx, indexBase + minPos), Math.max(idx, indexBase + maxPos));
            }
            int pos = idx - indexBase;
            counts[pos]++;
            if (pos < minPos) {
                minPos = pos;
            }
            if (pos > maxPos) {
                maxPos = pos;
            }
        }

        /**
         * Recenters the existing counts (same scale) into a window spanning {@code [windowMin, windowMax]}
         * inclusive, which is guaranteed to be at most {@code maxBins} wide.
         */
        private void rebase(int windowMin, int windowMax) {
            int span = windowMax - windowMin + 1;
            int pad = (maxBins - span) / 2;
            int newBase = windowMin - pad;

            long[] newCounts = new long[maxBins];
            int newMinPos = Integer.MAX_VALUE;
            int newMaxPos = Integer.MIN_VALUE;
            for (int p = minPos; p <= maxPos; p++) {
                if (counts[p] != 0) {
                    int newPos = (indexBase + p) - newBase;
                    newCounts[newPos] = counts[p];
                    if (newPos < newMinPos) {
                        newMinPos = newPos;
                    }
                    if (newPos > newMaxPos) {
                        newMaxPos = newPos;
                    }
                }
            }
            this.counts = newCounts;
            this.indexBase = newBase;
            this.minPos = newMinPos;
            this.maxPos = newMaxPos;
        }

        /**
         * Halves the resolution: every bin {@code i} folds into {@code i >> 1}, merging adjacent
         * pairs. The occupied span at least halves, so repeated calls always make progress.
         */
        void downscale() {
            if (isEmpty()) {
                return;
            }
            int newMinIdx = (indexBase + minPos) >> 1;
            int newMaxIdx = (indexBase + maxPos) >> 1;
            int mergedSpan = newMaxIdx - newMinIdx + 1;

            long[] merged = new long[mergedSpan];
            for (int p = minPos; p <= maxPos; p++) {
                if (counts[p] != 0) {
                    int newIdx = (indexBase + p) >> 1;
                    merged[newIdx - newMinIdx] += counts[p];
                }
            }

            int pad = (maxBins - mergedSpan) / 2;
            long[] newCounts = new long[maxBins];
            System.arraycopy(merged, 0, newCounts, pad, mergedSpan);

            this.counts = newCounts;
            this.indexBase = newMinIdx - pad;
            this.minPos = pad;
            this.maxPos = pad + mergedSpan - 1;
        }

        /** The logical index of the lowest occupied bin; 0 when empty. */
        int offset() {
            return isEmpty() ? 0 : indexBase + minPos;
        }

        /** The count of the {@code k}-th occupied bin, {@code 0 <= k < span()}: logical index {@code offset() + k}. */
        long countAt(int k) {
            return counts[minPos + k];
        }

        long[] toArray() {
            return isEmpty() ? new long[0] : Arrays.copyOfRange(counts, minPos, maxPos + 1);
        }

        int span() {
            return isEmpty() ? 0 : maxPos - minPos + 1;
        }

        void reset() {
            // Drop the array so a store that goes unused after a reset reclaims its memory.
            counts = null;
            minPos = 0;
            maxPos = -1;
            indexBase = 0;
        }
    }
}
