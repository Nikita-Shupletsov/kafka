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
package org.apache.kafka.common.metrics.internals;

import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.MetricNameTemplate;
import org.apache.kafka.common.metrics.MetricConfig;
import org.apache.kafka.common.metrics.Sensor;
import org.apache.kafka.common.metrics.stats.Percentile;
import org.apache.kafka.common.metrics.stats.Percentiles;
import org.apache.kafka.common.metrics.stats.RawHistogram;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.function.Function;

/**
 * Builds the fleet-standard latency distribution for KIP-714 client telemetry: numeric
 * {@code -p50}/{@code -p95}/{@code -p99} percentiles (ordinary JMX-visible metrics) plus the raw
 * distribution published under the {@code -histogram} sibling name, which the telemetry collector ships
 * to the broker for central re-aggregation. Both come from a single adaptive {@link Percentiles}, added
 * with {@link #addTo(Sensor, Names, MetricConfig)}. A component that declares its metrics as templates gets the
 * names from {@link #templates}; one that does not derives them from a base name with
 * {@link #addTo(Sensor, MetricName, MetricConfig)}.
 *
 * <p>The point of a shared factory is that every latency metric across the client agrees on the
 * percentile set and bin budget — the fleet cannot compare distributions across metrics otherwise.
 * The window length and sample count are the client's own {@code metrics.sample.window.ms} and
 * {@code metrics.num.samples}, as for every other client stat.
 *
 * <p><b>Naming — the {@code -histogram} suffix.</b> Telemetry selects metrics by name <em>prefix</em>.
 * The raw distribution is published under a sibling name ({@code base + "-histogram"}) rather than the
 * scalar base so it is independently selectable: {@code ...request.latency.p99} picks a percentile,
 * {@code ...request.latency.histogram} picks the distribution, and the shared prefix picks both.
 *
 * <p>Both views come from one adaptive log-scale store on the {@link
 * org.apache.kafka.common.metrics.stats.SampledStat} ring. The percentiles are read over
 * {@code metrics.num.samples × metrics.sample.window.ms}, the span every other client stat reports over;
 * the raw distribution is the ring's closed windows, which the collector merges into disjoint per-push
 * deltas via a cursor. Once a telemetry subscription is known, the reporter may have the ring retain more than
 * {@code metrics.num.samples} windows so they survive until the next push; the percentiles still read over
 * {@code metrics.num.samples}.
 * The store spends its bin budget only on the occupied range and adapts its scale as latencies spread,
 * so a modest budget keeps good relative precision while bounding the telemetry payload (the raw bin
 * counts travel on the wire).
 */
public final class LatencyPercentiles {

    /**
     * Suffix distinguishing the raw telemetry histogram from the scalar/percentile variants of the
     * same measurement, so it can be selected independently by a name-prefix subscription.
     */
    public static final String HISTOGRAM_SUFFIX = "-histogram";

    /**
     * Bin budget for the adaptive store: holds 2.2% bins across any window within a 256x span, for
     * example 1 to 256 ms, and downscales beyond that.
     */
    private static final int HISTOGRAM_MAX_BINS = 256;

    /** Scale each window starts at: base-2 bins about 2.2% wide, before any downscaling. */
    private static final int HISTOGRAM_INITIAL_SCALE = 5;

    /** Percentiles exposed as numeric JMX metrics: name suffix, percentile, and how a description names it. */
    private static final String[] PERCENTILE_SUFFIXES = {"p50", "p95", "p99"};
    private static final double[] PERCENTILE_VALUES = {50.0, 95.0, 99.0};
    private static final String[] PERCENTILE_ORDINALS = {"50th", "95th", "99th"};

    private LatencyPercentiles() {
    }

    /**
     * The metric name templates of one latency distribution, for a component that declares its metrics as
     * templates (and documents them from those templates): one per percentile, in percentile order, and one for
     * the raw histogram. {@code quantity} completes each description, e.g. "the time in ms record batches spent
     * in the send buffer".
     */
    public static Templates templates(String baseName, String group, String quantity, Set<String> tagKeys) {
        List<MetricNameTemplate> percentiles = new ArrayList<>(PERCENTILE_SUFFIXES.length);
        for (int i = 0; i < PERCENTILE_SUFFIXES.length; i++) {
            percentiles.add(new MetricNameTemplate(baseName + "-" + PERCENTILE_SUFFIXES[i], group,
                "The " + PERCENTILE_ORDINALS[i] + " percentile of " + quantity + ".", tagKeys));
        }
        MetricNameTemplate histogram = new MetricNameTemplate(baseName + HISTOGRAM_SUFFIX, group,
            "The distribution of " + quantity + ", as raw bins. Sent only via client telemetry (KIP-714); "
                + "not exposed via JMX.", tagKeys);
        return new Templates(percentiles, histogram);
    }

    /**
     * Add the standard latency distribution named by {@code names} to {@code sensor}. {@code clientConfig} is the
     * client's metrics config; the percentiles report over its {@code samples × timeWindow}, as every other client
     * stat does.
     */
    public static void addTo(Sensor sensor, Names names, MetricConfig clientConfig) {
        Percentile[] percentiles = new Percentile[PERCENTILE_VALUES.length];
        for (int i = 0; i < percentiles.length; i++) {
            percentiles[i] = new Percentile(names.percentiles.get(i), PERCENTILE_VALUES[i]);
        }
        sensor.add(new Percentiles(HISTOGRAM_MAX_BINS, HISTOGRAM_INITIAL_SCALE,
            new RawHistogram(names.histogram), percentiles), clientConfig);
    }

    /**
     * Add the standard latency distribution for {@code scalarBase} to {@code sensor}, for a component that does
     * not declare its metrics as templates: the names are {@code scalarBase} with the percentile and
     * {@link #HISTOGRAM_SUFFIX} suffixes appended, in its group, with its description and tags.
     */
    public static void addTo(Sensor sensor, MetricName scalarBase, MetricConfig clientConfig) {
        List<MetricName> percentiles = new ArrayList<>(PERCENTILE_SUFFIXES.length);
        for (String suffix : PERCENTILE_SUFFIXES) {
            percentiles.add(suffixed(scalarBase, "-" + suffix));
        }
        addTo(sensor, new Names(percentiles, histogramMetricName(scalarBase)), clientConfig);
    }

    /**
     * The histogram metric name for a scalar measurement's base name — the base with
     * {@link #HISTOGRAM_SUFFIX} appended (same group, description and tags).
     */
    public static MetricName histogramMetricName(MetricName scalarBase) {
        return suffixed(scalarBase, HISTOGRAM_SUFFIX);
    }

    private static MetricName suffixed(MetricName base, String suffix) {
        return new MetricName(base.name() + suffix, base.group(), base.description(), base.tags());
    }

    /**
     * The templates of one latency distribution, from {@link #templates}.
     */
    public static final class Templates {
        private final List<MetricNameTemplate> percentiles;
        private final MetricNameTemplate histogram;

        private Templates(List<MetricNameTemplate> percentiles, MetricNameTemplate histogram) {
            this.percentiles = List.copyOf(percentiles);
            this.histogram = histogram;
        }

        /** Every template, percentiles first, for a component's list of all its templates. */
        public List<MetricNameTemplate> all() {
            List<MetricNameTemplate> all = new ArrayList<>(percentiles);
            all.add(histogram);
            return all;
        }

        /** The metric names, each instantiated from its template by {@code instance}. */
        public Names instantiate(Function<MetricNameTemplate, MetricName> instance) {
            List<MetricName> names = new ArrayList<>(percentiles.size());
            for (MetricNameTemplate template : percentiles) {
                names.add(instance.apply(template));
            }
            return new Names(names, instance.apply(histogram));
        }
    }

    /**
     * The metric names of one latency distribution: one per percentile, in percentile order, and the raw histogram.
     */
    public static final class Names {
        private final List<MetricName> percentiles;
        private final MetricName histogram;

        private Names(List<MetricName> percentiles, MetricName histogram) {
            this.percentiles = List.copyOf(percentiles);
            this.histogram = histogram;
        }
    }
}
