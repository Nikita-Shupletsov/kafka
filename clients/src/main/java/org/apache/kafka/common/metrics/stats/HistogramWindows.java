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

import java.util.List;
import java.util.Optional;

/**
 * The value of a raw histogram metric: the closed windows of a {@link Percentiles} constructed with a
 * {@link RawHistogram}, oldest first, each a {@link HistogramWindow} tagged with its
 * {@code [windowStartMs, windowEndMs]}. All windows of one metric are of one type: an
 * {@link AdaptiveHistogramWindow} for an adaptive store, a {@link FixedHistogramWindow} for a fixed-range one. Distinct from a plain number so the telemetry collector can recognize it, keep a cursor
 * over the window ends, and ship only the windows newer than it (merged into one delta point). A read never
 * mutates the ring's samples, so a stale or repeated read is safe.
 */
@InterfaceAudience.Public
public class HistogramWindows {

    private final List<HistogramWindow> windows;

    HistogramWindows(List<HistogramWindow> windows) {
        this.windows = windows;
    }

    public List<HistogramWindow> windows() {
        return windows;
    }

    /**
     * Merge the closed windows whose end is strictly newer than {@code cursorEndMs} into one window spanning
     * {@code [earliest start, latest end]}, so consecutive reads tile without gap or overlap. Empty when no
     * window has closed since the cursor, for example when the metric recorded nothing, so the caller emits
     * nothing rather than an empty window. The caller advances its cursor to the returned window's
     * {@link HistogramWindow#windowEndMs()}.
     */
    public Optional<HistogramWindow> mergedSince(long cursorEndMs) {
        HistogramWindow merged = null;
        long earliestStart = Long.MAX_VALUE;
        long latestEnd = Long.MIN_VALUE;
        for (HistogramWindow window : windows) {
            if (window.windowEndMs() > cursorEndMs) {
                merged = merged == null ? window : merged.merge(window);
                earliestStart = Math.min(earliestStart, window.windowStartMs());
                latestEnd = Math.max(latestEnd, window.windowEndMs());
            }
        }
        if (merged == null) {
            return Optional.empty();
        }
        return Optional.of(merged.withWindow(earliestStart, latestEnd));
    }
}
