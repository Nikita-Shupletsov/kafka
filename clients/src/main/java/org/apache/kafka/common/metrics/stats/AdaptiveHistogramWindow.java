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
import java.util.List;

/**
 * One window of an adaptive {@link Percentiles} store, immutable, shaped like the positive range of an OTLP
 * {@code ExponentialHistogramDataPoint}: a {@code scale}, plus the bins given as an {@code offset} (the
 * logical index of the first bin) and contiguous {@code binCounts}. Values below 1 are counted in the
 * bin of 1, so there is no zero or negative range. {@code count}, {@code sum}, {@code min} and {@code max}
 * are exact.
 *
 * <p>The bin array holds the window's per-bin counts, not cumulative ones. It is shared, not copied, by
 * the accessor; a retained window hands the same array to every reader. Treat it as immutable — never mutate
 * the returned array.
 */
@InterfaceAudience.Public
public class AdaptiveHistogramWindow extends HistogramWindow {

    private final int scale;
    private final int offset;
    private final long[] binCounts;

    AdaptiveHistogramWindow(int scale, int offset, long[] binCounts,
                            long count, double sum, double min, double max) {
        this(scale, offset, binCounts, count, sum, min, max, 0L, 0L);
    }

    AdaptiveHistogramWindow(int scale, int offset, long[] binCounts,
                            long count, double sum, double min, double max,
                            long windowStartMs, long windowEndMs) {
        super(count, sum, min, max, windowStartMs, windowEndMs);
        this.scale = scale;
        this.offset = offset;
        this.binCounts = binCounts;
    }

    @Override
    AdaptiveHistogramWindow withWindow(long startMs, long endMs) {
        return new AdaptiveHistogramWindow(scale, offset, binCounts, count(), sum(), min(), max(), startMs, endMs);
    }

    @Override
    AdaptiveHistogramWindow merge(HistogramWindow other) {
        return ExponentialHistograms.merge(List.of(this, (AdaptiveHistogramWindow) other));
    }

    public int scale() {
        return scale;
    }

    public int offset() {
        return offset;
    }

    public long[] binCounts() {
        return binCounts;
    }

    @Override
    public String toString() {
        return "AdaptiveHistogramWindow(scale=" + scale + ", count=" + count() + ", sum=" + sum()
            + ", min=" + min() + ", max=" + max() + ", offset=" + offset
            + ", bins=" + Arrays.toString(binCounts) + ")";
    }
}
