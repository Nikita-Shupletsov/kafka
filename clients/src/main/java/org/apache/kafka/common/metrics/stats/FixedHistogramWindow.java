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

import java.util.Arrays;

/**
 * One window of a fixed-range {@link Percentiles} store, immutable, shaped like an OTLP explicit-bucket
 * {@code HistogramDataPoint}: {@code bounds} are the {@code n - 1} interior bin boundaries in increasing
 * order and {@code binCounts} the {@code n} per-bin counts. The first bin is open below and the
 * last open above, so values the {@link Histogram.BinScheme} clamped into an edge bin are reported as lying
 * beyond the edge, which is exactly what is known about them.
 *
 * <p>A {@link Histogram} bin includes its lower boundary, whereas OTLP buckets include their upper one; the
 * two differ only for values falling exactly on a boundary.
 *
 * <p>The arrays are shared, not copied, by the accessors. Treat them as
 * immutable — never mutate a returned array.
 */
@InterfaceAudience.Public
public class FixedHistogramWindow extends HistogramWindow {

    private final double[] bounds;
    private final long[] binCounts;

    FixedHistogramWindow(double[] bounds, long[] binCounts,
                              long count, double sum, double min, double max) {
        this(bounds, binCounts, count, sum, min, max, 0L, 0L);
    }

    FixedHistogramWindow(double[] bounds, long[] binCounts,
                              long count, double sum, double min, double max,
                              long windowStartMs, long windowEndMs) {
        super(count, sum, min, max, windowStartMs, windowEndMs);
        if (binCounts.length != bounds.length + 1) {
            throw new IllegalArgumentException("Expected " + (bounds.length + 1) + " bin counts for "
                + bounds.length + " bounds, got " + binCounts.length);
        }
        this.bounds = bounds;
        this.binCounts = binCounts;
    }

    @Override
    FixedHistogramWindow withWindow(long startMs, long endMs) {
        return new FixedHistogramWindow(bounds, binCounts, count(), sum(), min(), max(), startMs, endMs);
    }

    @Override
    FixedHistogramWindow merge(HistogramWindow other) {
        FixedHistogramWindow that = (FixedHistogramWindow) other;
        if (!Arrays.equals(bounds, that.bounds)) {
            throw new IllegalArgumentException("Cannot merge explicit histograms with different bounds");
        }
        long[] merged = new long[binCounts.length];
        for (int i = 0; i < merged.length; i++) {
            merged[i] = binCounts[i] + that.binCounts[i];
        }
        // An empty side has NaN min/max, which Math.min/max would propagate.
        double min;
        double max;
        if (that.count() == 0) {
            min = min();
            max = max();
        } else if (count() == 0) {
            min = that.min();
            max = that.max();
        } else {
            min = Math.min(min(), that.min());
            max = Math.max(max(), that.max());
        }
        return new FixedHistogramWindow(bounds, merged, count() + that.count(), sum() + that.sum(), min, max);
    }

    public double[] bounds() {
        return bounds;
    }

    public long[] binCounts() {
        return binCounts;
    }

    @Override
    public String toString() {
        return "FixedHistogramWindow(count=" + count() + ", sum=" + sum() + ", min=" + min()
            + ", max=" + max() + ", bounds=" + Arrays.toString(bounds)
            + ", bins=" + Arrays.toString(binCounts) + ")";
    }
}
