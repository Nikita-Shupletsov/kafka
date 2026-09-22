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

/**
 * One closed time window of a {@link Percentiles} store, immutable, as published under its {@link RawHistogram}
 * name: what was recorded between {@link #windowStartMs()} and {@link #windowEndMs()}. The bin layout depends
 * on the store: an adaptive store yields an {@link AdaptiveHistogramWindow}, a fixed-range one a
 * {@link FixedHistogramWindow}. {@code count}, {@code sum}, {@code min} and {@code max} are exact in both;
 * {@code min} and {@code max} are {@code NaN} when the window is empty.
 */
@InterfaceAudience.Public
public abstract class HistogramWindow {

    private final long count;
    private final double sum;
    private final double min;
    private final double max;
    // The window's bounds, epoch millis; 0/0 until tagged (a store's contents before the stat tags them).
    private final long windowStartMs;
    private final long windowEndMs;

    HistogramWindow(long count, double sum, double min, double max, long windowStartMs, long windowEndMs) {
        this.count = count;
        this.sum = sum;
        this.min = min;
        this.max = max;
        this.windowStartMs = windowStartMs;
        this.windowEndMs = windowEndMs;
    }

    /** A copy of this window tagged with the bounds {@code [startMs, endMs]}. */
    abstract HistogramWindow withWindow(long startMs, long endMs);

    /**
     * This window merged with {@code other}, which must come from the same kind of store. The result is not
     * tagged with bounds.
     */
    abstract HistogramWindow merge(HistogramWindow other);

    public long count() {
        return count;
    }

    public double sum() {
        return sum;
    }

    public double min() {
        return min;
    }

    public double max() {
        return max;
    }

    public long windowStartMs() {
        return windowStartMs;
    }

    public long windowEndMs() {
        return windowEndMs;
    }
}
