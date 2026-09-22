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
import org.apache.kafka.common.metrics.stats.Histogram.ExponentialBinScheme;
import org.apache.kafka.common.metrics.stats.Histogram.LinearBinScheme;

import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Random;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class HistogramTest {

    private static final double EPS = 0.0000001d;

    @Test
    public void testHistogram() {
        BinScheme scheme = new ConstantBinScheme(10, -5, 5);
        Histogram hist = new Histogram(scheme);
        for (int i = -5; i < 5; i++)
            hist.record(i);
        for (int i = 0; i < 10; i++)
            assertEquals(scheme.fromBin(i), hist.value(i / 10.0 + EPS), EPS);
    }

    @Test
    public void testConstantBinScheme() {
        ConstantBinScheme scheme = new ConstantBinScheme(5, -5, 5);
        assertEquals(0, scheme.toBin(-5.01), "A value below the lower bound should map to the first bin");
        assertEquals(4, scheme.toBin(5.01), "A value above the upper bound should map to the last bin");
        assertEquals(0, scheme.toBin(-5.0001), "Check boundary of bucket 0");
        assertEquals(0, scheme.toBin(-5.0000), "Check boundary of bucket 0");
        assertEquals(0, scheme.toBin(-4.99999), "Check boundary of bucket 0");
        assertEquals(0, scheme.toBin(-3.00001), "Check boundary of bucket 0");
        assertEquals(1, scheme.toBin(-3), "Check boundary of bucket 1");
        assertEquals(1, scheme.toBin(-1.00001), "Check boundary of bucket 1");
        assertEquals(2, scheme.toBin(-1), "Check boundary of bucket 2");
        assertEquals(2, scheme.toBin(0.99999), "Check boundary of bucket 2");
        assertEquals(3, scheme.toBin(1), "Check boundary of bucket 3");
        assertEquals(3, scheme.toBin(2.99999), "Check boundary of bucket 3");
        assertEquals(4, scheme.toBin(3), "Check boundary of bucket 4");
        assertEquals(4, scheme.toBin(4.9999), "Check boundary of bucket 4");
        assertEquals(4, scheme.toBin(5.000), "Check boundary of bucket 4");
        assertEquals(4, scheme.toBin(5.001), "Check boundary of bucket 4");
        assertEquals(Float.NEGATIVE_INFINITY, scheme.fromBin(-1), 0.001d);
        assertEquals(Float.POSITIVE_INFINITY, scheme.fromBin(5), 0.001d);
        assertEquals(-5.0, scheme.fromBin(0), 0.001d);
        assertEquals(-3.0, scheme.fromBin(1), 0.001d);
        assertEquals(-1.0, scheme.fromBin(2), 0.001d);
        assertEquals(1.0, scheme.fromBin(3), 0.001d);
        assertEquals(3.0, scheme.fromBin(4), 0.001d);
        checkBinningConsistency(scheme);
    }

    @Test
    public void testConstantBinSchemeWithPositiveRange() {
        ConstantBinScheme scheme = new ConstantBinScheme(5, 0, 5);
        assertEquals(0, scheme.toBin(-1.0), "A value below the lower bound should map to the first bin");
        assertEquals(4, scheme.toBin(5.01), "A value above the upper bound should map to the last bin");
        assertEquals(0, scheme.toBin(-0.0001), "Check boundary of bucket 0");
        assertEquals(0, scheme.toBin(0.0000), "Check boundary of bucket 0");
        assertEquals(0, scheme.toBin(0.0001), "Check boundary of bucket 0");
        assertEquals(0, scheme.toBin(0.9999), "Check boundary of bucket 0");
        assertEquals(1, scheme.toBin(1.0000), "Check boundary of bucket 1");
        assertEquals(1, scheme.toBin(1.0001), "Check boundary of bucket 1");
        assertEquals(1, scheme.toBin(1.9999), "Check boundary of bucket 1");
        assertEquals(2, scheme.toBin(2.0000), "Check boundary of bucket 2");
        assertEquals(2, scheme.toBin(2.0001), "Check boundary of bucket 2");
        assertEquals(2, scheme.toBin(2.9999), "Check boundary of bucket 2");
        assertEquals(3, scheme.toBin(3.0000), "Check boundary of bucket 3");
        assertEquals(3, scheme.toBin(3.0001), "Check boundary of bucket 3");
        assertEquals(3, scheme.toBin(3.9999), "Check boundary of bucket 3");
        assertEquals(4, scheme.toBin(4.0000), "Check boundary of bucket 4");
        assertEquals(4, scheme.toBin(4.9999), "Check boundary of bucket 4");
        assertEquals(4, scheme.toBin(5.0000), "Check boundary of bucket 4");
        assertEquals(4, scheme.toBin(5.0001), "Check boundary of bucket 4");
        assertEquals(Float.NEGATIVE_INFINITY, scheme.fromBin(-1), 0.001d);
        assertEquals(Float.POSITIVE_INFINITY, scheme.fromBin(5), 0.001d);
        assertEquals(0.0, scheme.fromBin(0), 0.001d);
        assertEquals(1.0, scheme.fromBin(1), 0.001d);
        assertEquals(2.0, scheme.fromBin(2), 0.001d);
        assertEquals(3.0, scheme.fromBin(3), 0.001d);
        assertEquals(4.0, scheme.fromBin(4), 0.001d);
        checkBinningConsistency(scheme);
    }

    @Test
    public void testLinearBinSchemeRejectsNegativeValues() {
        LinearBinScheme scheme = new LinearBinScheme(10, 10);
        assertThrows(IllegalArgumentException.class, () -> scheme.toBin(-1.0));
        assertEquals(9, scheme.toBin(11.0), "A value above max should map to the last bin");
    }

    @Test
    public void testLinearBinScheme() {
        LinearBinScheme scheme = new LinearBinScheme(10, 10);
        assertEquals(Float.NEGATIVE_INFINITY, scheme.fromBin(-1), 0.001d);
        assertEquals(Float.POSITIVE_INFINITY, scheme.fromBin(11), 0.001d);
        assertEquals(0.0, scheme.fromBin(0), 0.001d);
        assertEquals(0.2222, scheme.fromBin(1), 0.001d);
        assertEquals(0.6666, scheme.fromBin(2), 0.001d);
        assertEquals(1.3333, scheme.fromBin(3), 0.001d);
        assertEquals(2.2222, scheme.fromBin(4), 0.001d);
        assertEquals(3.3333, scheme.fromBin(5), 0.001d);
        assertEquals(4.6667, scheme.fromBin(6), 0.001d);
        assertEquals(6.2222, scheme.fromBin(7), 0.001d);
        assertEquals(8.0000, scheme.fromBin(8), 0.001d);
        assertEquals(10.000, scheme.fromBin(9), 0.001d);
        assertEquals(0, scheme.toBin(0.0000));
        assertEquals(0, scheme.toBin(0.2221));
        assertEquals(1, scheme.toBin(0.2223));
        assertEquals(2, scheme.toBin(0.6667));
        assertEquals(3, scheme.toBin(1.3334));
        assertEquals(4, scheme.toBin(2.2223));
        assertEquals(5, scheme.toBin(3.3334));
        assertEquals(6, scheme.toBin(4.6667));
        assertEquals(7, scheme.toBin(6.2223));
        assertEquals(8, scheme.toBin(8.0000));
        assertEquals(9, scheme.toBin(10.000));
        assertEquals(9, scheme.toBin(10.001));
        assertEquals(Float.POSITIVE_INFINITY, scheme.fromBin(10), 0.001d);
        checkBinningConsistency(scheme);
    }

    @Test
    public void testExponentialBinScheme() {
        // min=1, max=1024, 10 bins => ratio = (1024/1)^(1/10) = 2, so bin edges are powers of two.
        ExponentialBinScheme scheme = new ExponentialBinScheme(10, 1, 1024);
        assertEquals(Float.NEGATIVE_INFINITY, scheme.fromBin(-1), 0.001d);
        assertEquals(Float.POSITIVE_INFINITY, scheme.fromBin(10), 0.001d);
        assertEquals(1.0, scheme.fromBin(0), 0.001d);
        assertEquals(2.0, scheme.fromBin(1), 0.001d);
        assertEquals(4.0, scheme.fromBin(2), 0.001d);
        assertEquals(8.0, scheme.fromBin(3), 0.001d);
        assertEquals(16.0, scheme.fromBin(4), 0.001d);
        assertEquals(32.0, scheme.fromBin(5), 0.001d);
        assertEquals(64.0, scheme.fromBin(6), 0.001d);
        assertEquals(128.0, scheme.fromBin(7), 0.001d);
        assertEquals(256.0, scheme.fromBin(8), 0.001d);
        assertEquals(512.0, scheme.fromBin(9), 0.001d);
        // Values at or below min land in bin 0; at or above max land in the last bin.
        assertEquals(0, scheme.toBin(0.5));
        assertEquals(0, scheme.toBin(1.0));
        assertEquals(0, scheme.toBin(1.5));
        assertEquals(1, scheme.toBin(2.0));
        assertEquals(1, scheme.toBin(3.0));
        assertEquals(2, scheme.toBin(4.0));
        assertEquals(5, scheme.toBin(63.0));
        assertEquals(6, scheme.toBin(64.0));
        assertEquals(9, scheme.toBin(512.0));
        assertEquals(9, scheme.toBin(1024.0));
        assertEquals(9, scheme.toBin(5000.0));
        checkBinningConsistency(scheme);
    }

    @Test
    public void testExponentialBinSchemeValidation() {
        assertThrows(IllegalArgumentException.class, () -> new ExponentialBinScheme(1, 1, 1024));
        assertThrows(IllegalArgumentException.class, () -> new ExponentialBinScheme(10, 0, 1024));
        assertThrows(IllegalArgumentException.class, () -> new ExponentialBinScheme(10, -1, 1024));
        assertThrows(IllegalArgumentException.class, () -> new ExponentialBinScheme(10, 10, 10));
        assertThrows(IllegalArgumentException.class, () -> new ExponentialBinScheme(10, 10, 5));
    }

    @Test
    public void testExponentialHistogramGivesAccuratePercentilesOverWideRange() {
        // 1000 log-spaced bins over [1, 1e6] give ~1.4% relative width per bin. Recording the
        // integers 1..1000 (median ~500, p99 ~990) should reproduce those quantiles within a few
        // percent — the whole point of exponential bins for wide-range, non-negative quantities.
        ExponentialBinScheme scheme = new ExponentialBinScheme(1000, 1, 1_000_000);
        Histogram hist = new Histogram(scheme);
        for (int i = 1; i <= 1000; i++)
            hist.record(i);
        assertEquals(500.0, hist.value(0.5), 500.0 * 0.05);
        assertEquals(990.0, hist.value(0.99), 990.0 * 0.05);
        assertEquals(250.0, hist.value(0.25), 250.0 * 0.05);
    }

    @Test
    public void testEmptyWindowHasNaNMinMaxAndMergesAway() {
        BinScheme scheme = new ConstantBinScheme(10, 0, 10);
        double[] bounds = Histogram.bounds(scheme);
        Histogram hist = new Histogram(scheme);

        FixedHistogramWindow empty = hist.snapshot(bounds);
        assertEquals(0L, empty.count());
        assertEquals(Double.NaN, empty.min());
        assertEquals(Double.NaN, empty.max());

        hist.record(3.0);
        hist.record(7.0);
        FixedHistogramWindow full = hist.snapshot(bounds);
        for (FixedHistogramWindow merged : new FixedHistogramWindow[] {empty.merge(full), full.merge(empty)}) {
            assertEquals(2L, merged.count());
            assertEquals(3.0, merged.min(), 0.0);
            assertEquals(7.0, merged.max(), 0.0);
        }
        assertEquals(Double.NaN, empty.merge(empty).min());
    }

    private void checkBinningConsistency(BinScheme scheme) {
        for (int bin = 0; bin < scheme.bins(); bin++) {
            double fromBin = scheme.fromBin(bin);
            int binAgain = scheme.toBin(fromBin + EPS);
            assertEquals(bin, binAgain, "unbinning and rebinning the bin " + bin
                         + " gave a different result ("
                         + fromBin
                         + " was placed in bin "
                         + binAgain
                         + " )");
        }
    }

    public static void main(String[] args) {
        Random random = new Random();
        System.out.println("[-100, 100]:");
        for (BinScheme scheme : Arrays.asList(new ConstantBinScheme(1000, -100, 100),
                                              new ConstantBinScheme(100, -100, 100),
                                              new ConstantBinScheme(10, -100, 100))) {
            Histogram h = new Histogram(scheme);
            for (int i = 0; i < 10000; i++)
                h.record(200.0 * random.nextDouble() - 100.0);
            for (double quantile = 0.0; quantile < 1.0; quantile += 0.05)
                System.out.printf("%5.2f: %.1f, ", quantile, h.value(quantile));
            System.out.println();
        }

        System.out.println("[0, 1000]");
        for (BinScheme scheme : Arrays.asList(new LinearBinScheme(1000, 1000),
                                              new LinearBinScheme(100, 1000),
                                              new LinearBinScheme(10, 1000))) {
            Histogram h = new Histogram(scheme);
            for (int i = 0; i < 10000; i++)
                h.record(1000.0 * random.nextDouble());
            for (double quantile = 0.0; quantile < 1.0; quantile += 0.05)
                System.out.printf("%5.2f: %.1f, ", quantile, h.value(quantile));
            System.out.println();
        }
    }

}
