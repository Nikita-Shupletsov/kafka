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

import org.apache.kafka.common.metrics.stats.Histogram.BinScheme;

import java.util.ArrayList;
import java.util.List;

/**
 * The {@link QuantileReader} for fixed-bin {@link Histogram} stores. Every store shares one {@link BinScheme},
 * so bin {@code b} means the same range in all of them: the read adds bin {@code b} across the stores, bin by bin,
 * and returns the lower edge of the bin where the quantile is crossed. The arithmetic is single-precision, as
 * {@link Percentiles} has always done it, so results are unchanged at exact quantile boundaries.
 */
final class FixedQuantileReader implements QuantileReader {

    private final BinScheme binScheme;
    private final List<float[]> counts = new ArrayList<>();
    private float total;

    FixedQuantileReader(BinScheme binScheme) {
        this.binScheme = binScheme;
    }

    @Override
    public void add(RecordingStore store) {
        Histogram histogram = (Histogram) store;
        counts.add(histogram.counts());
        total += (float) histogram.count();
    }

    @Override
    public double valueAt(double quantile) {
        if (total == 0.0f) {
            return Double.NaN;
        }
        float cumulative = 0.0f;
        float quant = (float) quantile;
        for (int b = 0; b < binScheme.bins(); b++) {
            for (float[] c : counts) {
                cumulative += c[b];
            }
            if (cumulative / total > quant) {
                return binScheme.fromBin(b);
            }
        }
        return Double.POSITIVE_INFINITY;
    }
}
