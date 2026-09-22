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

@InterfaceAudience.Public
public class Histogram implements RecordingStore {

    private final BinScheme binScheme;
    private final float[] hist;
    private double count;
    // Exact, unbinned aggregates of the recorded values, for snapshot().
    private double sum;
    private double min = Double.POSITIVE_INFINITY;
    private double max = Double.NEGATIVE_INFINITY;

    public Histogram(BinScheme binScheme) {
        this.hist = new float[binScheme.bins()];
        this.count = 0.0f;
        this.binScheme = binScheme;
    }

    public void record(double value) {
        record(value, value);
    }

    // Bins binnedValue but aggregates value, so a caller that pins out-of-range values into its declared range
    // for binning (as Percentiles does) still reports their exact sum, min and max.
    void record(double value, double binnedValue) {
        this.hist[binScheme.toBin(binnedValue)] += 1.0f;
        this.count += 1.0d;
        this.sum += value;
        this.min = Math.min(this.min, value);
        this.max = Math.max(this.max, value);
    }

    // The interior bin boundaries of a scheme, in the form FixedHistogramWindow takes. Computed once by
    // the owner and shared by every snapshot, rather than per sample.
    static double[] bounds(BinScheme binScheme) {
        double[] bounds = new double[binScheme.bins() - 1];
        for (int i = 0; i < bounds.length; i++) {
            bounds[i] = binScheme.fromBin(i + 1);
        }
        return bounds;
    }

    // An immutable copy of this histogram. bounds must be bounds(binScheme). The float bin counts are exact
    // up to 2^24 per bin, far beyond what one sample window receives.
    FixedHistogramWindow snapshot(double[] bounds) {
        long[] counts = new long[this.hist.length];
        for (int i = 0; i < counts.length; i++) {
            counts[i] = (long) this.hist[i];
        }
        double snapMin = count == 0 ? Double.NaN : min;
        double snapMax = count == 0 ? Double.NaN : max;
        return new FixedHistogramWindow(bounds, counts, (long) count, sum, snapMin, snapMax);
    }

    public double value(double quantile) {
        if (count == 0.0d)
            return Double.NaN;
        if (quantile > 1.00d)
            return Float.POSITIVE_INFINITY;
        if (quantile < 0.00d)
            return Float.NEGATIVE_INFINITY;
        float sum = 0.0f;
        float quant = (float) quantile;
        for (int i = 0; i < this.hist.length - 1; i++) {
            sum += this.hist[i];
            if (sum / count > quant)
                return binScheme.fromBin(i);
        }
        return binScheme.fromBin(this.hist.length - 1);
    }

    public float[] counts() {
        return this.hist;
    }

    // The number of values recorded, for FixedQuantileReader.
    double count() {
        return this.count;
    }

    public void clear() {
        Arrays.fill(this.hist, 0.0f);
        this.count = 0;
        this.sum = 0.0;
        this.min = Double.POSITIVE_INFINITY;
        this.max = Double.NEGATIVE_INFINITY;
    }

    @Override
    public String toString() {
        StringBuilder b = new StringBuilder("{");
        for (int i = 0; i < this.hist.length - 1; i++) {
            b.append(String.format("%.10f", binScheme.fromBin(i)));
            b.append(':');
            b.append(String.format("%.0f", this.hist[i]));
            b.append(',');
        }
        b.append(Float.POSITIVE_INFINITY);
        b.append(':');
        b.append(String.format("%.0f", this.hist[this.hist.length - 1]));
        b.append('}');
        return b.toString();
    }

    /**
     * An algorithm for determining the bin in which a value is to be placed as well as calculating the upper end
     * of each bin.
     */
    public interface BinScheme {

        /**
         * Get the number of bins.
         *
         * @return the number of bins
         */
        int bins();

        /**
         * Determine the 0-based bin number in which the supplied value should be placed.
         *
         * @param value the value
         * @return the 0-based index of the bin
         */
        int toBin(double value);

        /**
         * Determine the value at the lower end of the specified bin, which is the upper end of the previous one.
         *
         * @param bin the 0-based bin number
         * @return the value at the lower end of the bin; or {@link Float#NEGATIVE_INFINITY negative infinity}
         * if the bin number is negative or {@link Float#POSITIVE_INFINITY positive infinity} if the 0-based
         * bin number is greater than or equal to the {@link #bins() number of bins}.
         */
        double fromBin(int bin);
    }

    /**
     * A scheme for calculating the bins where the width of each bin is a constant determined by the range of values
     * and the number of bins.
     */
    public static class ConstantBinScheme implements BinScheme {
        private static final int MIN_BIN_NUMBER = 0;
        private final double min;
        private final int bins;
        private final double bucketWidth;
        private final int maxBinNumber;

        /**
         * Create a bin scheme with the specified number of bins that all have the same width.
         *
         * @param bins the number of bins; must be at least 2
         * @param min the minimum value to be counted in the bins
         * @param max the maximum value to be counted in the bins
         */
        public ConstantBinScheme(int bins, double min, double max) {
            if (bins < 2)
                throw new IllegalArgumentException("Must have at least 2 bins.");
            this.min = min;
            this.bins = bins;
            this.bucketWidth = (max - min) / bins;
            this.maxBinNumber = bins - 1;
        }

        public int bins() {
            return this.bins;
        }

        public double fromBin(int b) {
            if (b < MIN_BIN_NUMBER) {
                return Float.NEGATIVE_INFINITY;
            }
            if (b > maxBinNumber) {
                return Float.POSITIVE_INFINITY;
            }
            return min + b * bucketWidth;
        }

        public int toBin(double x) {
            int binNumber = (int) ((x - min) / bucketWidth);
            if (binNumber < MIN_BIN_NUMBER) {
                return MIN_BIN_NUMBER;
            }
            return Math.min(binNumber, maxBinNumber);
        }
    }

    /**
     * A scheme for calculating the bins where the width of each bin is one more than the previous bin, and therefore
     * the bin widths are increasing at a linear rate. However, the bin widths are scaled such that the specified range
     * of values will all fit within the bins (e.g., the upper range of the last bin is equal to the maximum value).
     */
    public static class LinearBinScheme implements BinScheme {
        private final int bins;
        private final double max;
        private final double scale;

        /**
         * Create a linear bin scheme with the specified number of bins and the maximum value to be counted in the bins.
         *
         * @param numBins the number of bins; must be at least 2
         * @param max the maximum value to be counted in the bins
         */
        public LinearBinScheme(int numBins, double max) {
            if (numBins < 2)
                throw new IllegalArgumentException("Must have at least 2 bins.");
            this.bins = numBins;
            this.max = max;
            double denom = numBins * (numBins - 1.0) / 2.0;
            this.scale = max / denom;
        }

        public int bins() {
            return this.bins;
        }

        public double fromBin(int b) {
            if (b > this.bins - 1) {
                return Float.POSITIVE_INFINITY;
            } else if (b < 0.0000d) {
                return Float.NEGATIVE_INFINITY;
            } else {
                return this.scale * (b * (b + 1.0)) / 2.0;
            }
        }

        public int toBin(double x) {
            if (x < 0.0d) {
                throw new IllegalArgumentException("Values less than 0.0 not accepted.");
            } else if (x > this.max) {
                return this.bins - 1;
            } else {
                return (int) (-0.5 + 0.5 * Math.sqrt(1.0 + 8.0 * x / this.scale));
            }
        }
    }

    /**
     * A scheme where the bins are spaced geometrically (log-scale): each bin covers a constant
     * <em>ratio</em> of values rather than a constant width, giving uniform relative precision across
     * the whole range. This suits wide-range, non-negative quantities such as latencies and sizes,
     * where the values of interest span orders of magnitude. Bin {@code i} covers
     * {@code [min*ratio^i, min*ratio^(i+1))} with {@code ratio = (max/min)^(1/bins)}; values at or
     * below {@code min} fall in bin 0 and values at or above {@code max} in the last bin. Because the
     * mapping is logarithmic, {@code min} must be strictly positive.
     */
    public static class ExponentialBinScheme implements BinScheme {
        private final int bins;
        private final double min;
        private final double max;
        private final double logMin;
        private final double logRatio;

        /**
         * @param numBins the number of bins; must be at least 2
         * @param min the minimum trackable value; must be greater than 0
         * @param max the maximum trackable value; must be greater than {@code min}
         */
        public ExponentialBinScheme(int numBins, double min, double max) {
            if (numBins < 2)
                throw new IllegalArgumentException("Must have at least 2 bins.");
            if (min <= 0.0)
                throw new IllegalArgumentException("Exponential bin scheme requires min > 0.0.");
            if (max <= min)
                throw new IllegalArgumentException("Exponential bin scheme requires max > min.");
            this.bins = numBins;
            this.min = min;
            this.max = max;
            this.logMin = Math.log(min);
            this.logRatio = (Math.log(max) - logMin) / numBins;
        }

        public int bins() {
            return this.bins;
        }

        public double fromBin(int b) {
            if (b < 0) {
                return Float.NEGATIVE_INFINITY;
            } else if (b > this.bins - 1) {
                return Float.POSITIVE_INFINITY;
            } else {
                return Math.exp(logMin + b * logRatio);
            }
        }

        public int toBin(double x) {
            if (x <= this.min) {
                return 0;
            } else if (x >= this.max) {
                return this.bins - 1;
            } else {
                int bin = (int) ((Math.log(x) - logMin) / logRatio);
                return Math.min(bin, this.bins - 1);
            }
        }
    }
}