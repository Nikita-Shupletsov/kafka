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

import org.junit.jupiter.api.Test;

import java.util.Arrays;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class ExponentialHistogramTest {

    @Test
    public void testIndexIsFloorLog2ScaledByScale() {
        // index(v, scale) = floor(log2(v) * 2^scale); values chosen off bin boundaries to avoid
        // floating-point edge effects.
        assertEquals(0, ExponentialHistogram.index(1.0, 0));
        assertEquals(1, ExponentialHistogram.index(2.0, 0));
        assertEquals(2, ExponentialHistogram.index(4.0, 0));
        assertEquals(1, ExponentialHistogram.index(3.0, 0));   // log2(3) ≈ 1.58
        assertEquals(-1, ExponentialHistogram.index(0.5, 0));
        assertEquals(4, ExponentialHistogram.index(2.0, 2));   // log2(2) * 4
    }

    @Test
    public void testPositiveValues() {
        ExponentialHistogram h = new ExponentialHistogram(160, 8);
        h.record(10.0);
        h.record(100.0);
        h.record(1000.0);

        AdaptiveHistogramWindow s = h.snapshot();
        assertEquals(3L, s.count());
        assertEquals(1110.0, s.sum(), 0.0);
        assertEquals(10.0, s.min(), 0.0);
        assertEquals(1000.0, s.max(), 0.0);
        assertEquals(3L, Arrays.stream(s.binCounts()).sum());
    }

    @Test
    public void testValuesBelowOneAreCountedInTheBinOfOne() {
        ExponentialHistogram h = new ExponentialHistogram(160, 8);
        h.record(0.0);
        h.record(0.5);
        h.record(-5.0);
        h.record(1.0);
        h.record(7.0);

        AdaptiveHistogramWindow s = h.snapshot();
        // count, sum, min and max stay exact.
        assertEquals(5L, s.count());
        assertEquals(3.5, s.sum(), 0.0);   // 0 + 0.5 + (-5) + 1 + 7
        assertEquals(-5.0, s.min(), 0.0);
        assertEquals(7.0, s.max(), 0.0);
        // The four values at or below 1 share the bin of 1, index 0.
        assertEquals(0, s.offset());
        assertEquals(4L, s.binCounts()[0]);
        assertEquals(5L, Arrays.stream(s.binCounts()).sum());
    }

    @Test
    public void testDownscaleFitsBudgetAndPreservesCounts() {
        int maxBins = 16;
        ExponentialHistogram h = new ExponentialHistogram(maxBins, 8);
        double[] values = {1.0, 4.0, 16.0, 64.0, 256.0, 1024.0, 4096.0, 100000.0};
        for (double v : values) {
            h.record(v);
        }

        assertEquals(values.length, h.count());
        assertTrue(h.occupiedSpan() <= maxBins, "occupied span must stay within the budget");
        assertTrue(h.scale() < 8, "scale must downshift to fit the wide range");

        // Downscaling merges bins losslessly: every observation is still counted.
        AdaptiveHistogramWindow s = h.snapshot();
        assertEquals(values.length, Arrays.stream(s.binCounts()).sum());
    }

    @Test
    public void testClearEmptiesTheWindow() {
        ExponentialHistogram h = new ExponentialHistogram(160, 8);
        h.record(5.0);
        h.record(10.0);

        AdaptiveHistogramWindow first = h.snapshot();
        assertEquals(2L, first.count());
        assertEquals(15.0, first.sum(), 0.0);

        h.clear();
        AdaptiveHistogramWindow afterClear = h.snapshot();
        assertEquals(0L, afterClear.count());
        assertEquals(0.0, afterClear.sum(), 0.0);
        assertEquals(Double.NaN, afterClear.min());
        assertEquals(Double.NaN, afterClear.max());
        assertEquals(0, afterClear.binCounts().length);

        // An empty window merges away; merging only empty windows stays NaN.
        AdaptiveHistogramWindow merged = afterClear.merge(first);
        assertEquals(5.0, merged.min(), 0.0);
        assertEquals(10.0, merged.max(), 0.0);
        assertEquals(Double.NaN, afterClear.merge(afterClear).min());
    }

    @Test
    public void testNonFiniteValuesAreIgnored() {
        ExponentialHistogram h = new ExponentialHistogram(160, 8);
        h.record(Double.NaN);
        h.record(Double.POSITIVE_INFINITY);
        h.record(Double.NEGATIVE_INFINITY);
        h.record(5.0);

        assertEquals(1L, h.count());
        assertEquals(5.0, h.snapshot().sum(), 0.0);
    }
}
