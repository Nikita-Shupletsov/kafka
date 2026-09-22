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
import org.apache.kafka.common.metrics.stats.Histogram.ConstantBinScheme;

import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class QuantileReaderTest {

    private static final double[] QUANTILES = {0.0, 0.01, 0.25, 0.5, 0.9, 0.95, 0.99, 0.999, 1.0};

    @Test
    public void testFixedReaderWithoutObservationsIsNaN() {
        FixedQuantileReader reader = new FixedQuantileReader(new ConstantBinScheme(10, 0.0, 100.0));
        reader.add(new Histogram(new ConstantBinScheme(10, 0.0, 100.0)));
        assertTrue(Double.isNaN(reader.valueAt(0.5)));
    }

    @Test
    public void testFixedReaderAddsBinsAcrossStores() {
        BinScheme scheme = new ConstantBinScheme(10, 0.0, 100.0);
        Histogram first = new Histogram(scheme);
        Histogram second = new Histogram(scheme);
        for (int i = 0; i < 40; i++) {
            first.record(15.0);    // bin 1
        }
        for (int i = 0; i < 60; i++) {
            second.record(75.0);   // bin 7
        }

        FixedQuantileReader reader = new FixedQuantileReader(scheme);
        reader.add(first);
        reader.add(second);
        assertEquals(10.0, reader.valueAt(0.3), 0.0);   // 40% sit in bin 1: its lower edge
        assertEquals(70.0, reader.valueAt(0.5), 0.0);   // the rest in bin 7
        // A quantile the cumulative share never exceeds is never crossed, as Percentiles has always reported.
        assertEquals(Double.POSITIVE_INFINITY, reader.valueAt(1.0), 0.0);
    }

    @Test
    public void testExponentialReaderWithoutObservationsIsNaN() {
        ExponentialQuantileReader reader = new ExponentialQuantileReader();
        reader.add(new ExponentialHistogram(40, 5));
        assertTrue(Double.isNaN(reader.valueAt(0.5)));
    }

    @Test
    public void testExponentialReaderMatchesMergeAcrossScales() {
        Random random = new Random(42);
        // Three stores that settle at different scales: a narrow one stays fine, a wide one downscales, and
        // one has many values below 1, counted in the bin of 1.
        ExponentialHistogram narrow = new ExponentialHistogram(64, 6);
        ExponentialHistogram wide = new ExponentialHistogram(64, 6);
        ExponentialHistogram low = new ExponentialHistogram(64, 6);
        for (int i = 0; i < 1000; i++) {
            narrow.record(10.0 + random.nextDouble() * 5.0);
            wide.record(Math.pow(10.0, random.nextDouble() * 6.0));
            low.record(random.nextDouble() * 3.0);
        }
        assertNotEquals(narrow.scale(), wide.scale(), "the stores must be at different scales for this test");

        List<ExponentialHistogram> stores = List.of(narrow, wide, low);
        ExponentialQuantileReader reader = new ExponentialQuantileReader();
        List<AdaptiveHistogramWindow> snapshots = new ArrayList<>();
        for (ExponentialHistogram store : stores) {
            reader.add(store);
            snapshots.add(store.snapshot());
        }
        AdaptiveHistogramWindow merged = ExponentialHistograms.merge(snapshots);
        for (double q : QUANTILES) {
            assertEquals(walk(merged, q), reader.valueAt(q), 0.0, "quantile " + q);
        }
    }

    // The reference read: walk a merged snapshot, lowest value first, to the bin holding the quantile and
    // return its geometric midpoint.
    private static double walk(AdaptiveHistogramWindow s, double q) {
        double target = q * s.count();
        double base = ExponentialHistograms.base(s.scale());
        long cumulative = 0;
        long[] counts = s.binCounts();
        for (int k = 0; k < counts.length; k++) {
            cumulative += counts[k];
            if (cumulative >= target) {
                return Math.pow(base, s.offset() + k + 0.5);
            }
        }
        return s.max();
    }
}
