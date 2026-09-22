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

import org.apache.kafka.common.metrics.stats.ExponentialHistogram.Bins;

import java.util.ArrayList;
import java.util.List;

/**
 * The {@link QuantileReader} for adaptive {@link ExponentialHistogram} stores. Each store adapts its scale on its
 * own, so the read works at the coarsest scale among them: because the scales nest, target bin {@code t}
 * holds exactly the source bins {@code [t << shift, ((t + 1) << shift) - 1]} of a store whose scale is
 * {@code shift} finer. Walking target bins in value order with one cursor per store visits every source
 * bin once, without copying any, and gives the same answer as merging the stores and walking the result:
 * the quantile is estimated at the geometric midpoint of the target bin it falls in.
 */
final class ExponentialQuantileReader implements QuantileReader {

    private final List<ExponentialHistogram> stores = new ArrayList<>();
    private long total;
    private int targetScale = Integer.MAX_VALUE;
    private double max = Double.NEGATIVE_INFINITY;

    @Override
    public void add(RecordingStore store) {
        ExponentialHistogram histogram = (ExponentialHistogram) store;
        if (histogram.count() == 0) {
            return;
        }
        stores.add(histogram);
        total += histogram.count();
        targetScale = Math.min(targetScale, histogram.scale());
        max = Math.max(max, histogram.max());
    }

    /*
     Walks the target bins in ascending index, adding each store's bins that fall in the current target
     bin, and answers with the geometric midpoint of the target bin at which the cumulative count reaches
     rank. Every observation is in some bin, so the walk reaches the total; the max is only a guard against
     floating-point rounding of the rank.
    */
    @Override
    public double valueAt(double quantile) {
        if (total == 0) {
            return Double.NaN;
        }
        double rank = quantile * total;
        double base = ExponentialHistograms.base(targetScale);

        int n = stores.size();
        int[] cursor = new int[n];
        int lo = Integer.MAX_VALUE;
        int hi = Integer.MIN_VALUE;
        for (int j = 0; j < n; j++) {
            Bins bins = stores.get(j).bins();
            int shift = shift(j);
            lo = Math.min(lo, bins.offset() >> shift);
            hi = Math.max(hi, (bins.offset() + bins.span() - 1) >> shift);
        }
        long cumulative = 0;
        for (int t = lo; t <= hi; t++) {
            for (int j = 0; j < n; j++) {
                Bins bins = stores.get(j).bins();
                int shift = shift(j);
                while (cursor[j] < bins.span() && (bins.offset() + cursor[j]) >> shift == t) {
                    cumulative += bins.countAt(cursor[j]++);
                }
            }
            if (cumulative >= rank) {
                return Math.pow(base, t + 0.5);
            }
        }
        return max;
    }

    // How much finer a store's scale is than the target: its bins merge 2^shift at a time.
    private int shift(int store) {
        return stores.get(store).scale() - targetScale;
    }
}
