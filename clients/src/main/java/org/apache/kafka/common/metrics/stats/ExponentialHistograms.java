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

import java.util.List;

/**
 * Merging {@link AdaptiveHistogramWindow}s into one, used to combine several closed windows into one delta
 * point. Merging is scale-aware: it downscales the finer snapshots to the coarsest common scale, which is
 * lossless in counts. Percentiles are read off the live stores instead, by {@link ExponentialQuantileReader}.
 */
final class ExponentialHistograms {

    private ExponentialHistograms() {
    }

    /** The bin base for a scale: {@code 2^(2^-scale)}. */
    static double base(int scale) {
        return Math.pow(2.0, Math.pow(2.0, -scale));
    }

    /**
     * Merge windows into one at the coarsest scale among them. Empty windows are ignored; merging
     * nothing yields an empty window. Downscaling and accumulation run over arrays (scales and offsets
     * are known), so there is no per-bin boxing.
     */
    static AdaptiveHistogramWindow merge(List<AdaptiveHistogramWindow> parts) {
        int targetScale = Integer.MAX_VALUE;
        boolean any = false;
        for (AdaptiveHistogramWindow s : parts) {
            if (s.count() == 0) {
                continue;
            }
            any = true;
            targetScale = Math.min(targetScale, s.scale());
        }
        if (!any) {
            return new AdaptiveHistogramWindow(0, 0, new long[0], 0L, 0.0, Double.NaN, Double.NaN);
        }

        // Downscale every part's bins to targetScale and sum them into one contiguous array. Two passes
        // over the (small) part list: find the merged index span (from each part's own bin range,
        // shifted), then fill.
        int lo = Integer.MAX_VALUE;
        int hi = Integer.MIN_VALUE;
        long count = 0;
        double sum = 0.0;
        double min = Double.POSITIVE_INFINITY;
        double max = Double.NEGATIVE_INFINITY;
        for (AdaptiveHistogramWindow s : parts) {
            if (s.count() == 0) {
                continue;
            }
            int shift = s.scale() - targetScale;   // >= 0; downscaling merges 2^shift bins into one
            lo = Math.min(lo, s.offset() >> shift);
            hi = Math.max(hi, (s.offset() + s.binCounts().length - 1) >> shift);
            count += s.count();
            sum += s.sum();
            min = Math.min(min, s.min());
            max = Math.max(max, s.max());
        }
        long[] merged = new long[hi - lo + 1];
        for (AdaptiveHistogramWindow s : parts) {
            long[] counts = s.binCounts();
            int shift = s.scale() - targetScale;
            for (int k = 0; k < counts.length; k++) {
                merged[((s.offset() + k) >> shift) - lo] += counts[k];
            }
        }

        return new AdaptiveHistogramWindow(targetScale, lo, merged, count, sum, min, max);
    }
}
