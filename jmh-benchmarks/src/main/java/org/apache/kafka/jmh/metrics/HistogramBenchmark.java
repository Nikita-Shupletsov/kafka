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
package org.apache.kafka.jmh.metrics;

import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.metrics.KafkaMetric;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.metrics.Sensor;
import org.apache.kafka.common.metrics.stats.Percentile;
import org.apache.kafka.common.metrics.stats.Percentiles;
import org.apache.kafka.common.metrics.stats.Percentiles.BucketSizing;
import org.apache.kafka.server.metrics.KafkaYammerMetrics;

import com.yammer.metrics.core.Histogram;

import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Group;
import org.openjdk.jmh.annotations.GroupThreads;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.annotations.Threads;
import org.openjdk.jmh.annotations.Warmup;

import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;

@State(Scope.Benchmark)
@Fork(value = 1)
@Threads(10)
@Warmup(iterations = 5)
@Measurement(iterations = 5)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
public class HistogramBenchmark {

    /*
     * This benchmark compares the performance of the most commonly used in the Kafka codebase
     * Yammer histogram and the Kafka Metrics framework histogram ({@link Percentiles}), which now
     * backs the coordinator latency histograms. It focuses on the write path in a multiple-writers,
     * multiple-readers scenario.
     *
     * For a fair comparison both sides are exercised as they are in production: the Yammer histogram
     * is thread-safe on its own, and the framework {@link Percentiles} is recorded and read through a
     * {@link Sensor}/{@link KafkaMetric}, which is how the framework serializes concurrent records and
     * reads (a bare Percentiles is NOT thread-safe). Bucket layout matches KafkaMetricHistogram:
     * geometric buckets over [1ms, MAX_VALUE] at 0.25% relative error.
     *
     * The benchmark relies on JMH Groups to distribute the worker threads across the benchmark methods.
     */

    private static final double MIN_VALUE = 1.0;
    private static final long MAX_VALUE = TimeUnit.MINUTES.toMillis(1L);
    private static final double RELATIVE_ERROR = 0.0025;
    // Number of geometric buckets to cover [MIN_VALUE, MAX_VALUE] at RELATIVE_ERROR per bucket — the same
    // count KafkaMetricHistogram.bucketsFor(...) produces (~4407).
    private static final int BUCKETS = (int) Math.ceil(Math.log(MAX_VALUE / MIN_VALUE) / Math.log(1 + RELATIVE_ERROR));

    private Metrics metrics;
    private Sensor frameworkSensor;
    private KafkaMetric frameworkP999;
    private Histogram yammerHistogram;

    @Setup(Level.Trial)
    public void setUp() {
        yammerHistogram = KafkaYammerMetrics.defaultRegistry().newHistogram(new com.yammer.metrics.core.MetricName("a", "", ""), true);

        metrics = new Metrics();
        MetricName p999 = metrics.metricName("latency-p999", "bench");
        frameworkSensor = metrics.sensor("latency");
        // Percentiles sizes its buckets as bytes/4; give it BUCKETS buckets to match KafkaMetricHistogram.
        frameworkSensor.add(new Percentiles(BUCKETS * 4, MIN_VALUE, MAX_VALUE, BucketSizing.EXPONENTIAL,
            new Percentile(p999, 99.9)));
        frameworkP999 = metrics.metric(p999);
    }

    @TearDown(Level.Trial)
    public void tearDown() {
        metrics.close();
    }

    /*
     * The write benchmark methods below are the core of the benchmark. They use ThreadLocalRandom
     * to generate values to record. This is much faster than the actual histogram recording, so
     * the benchmark results are representative of the histogram implementation.
     */

    @Benchmark
    @Group("runner")
    @GroupThreads(3)
    public void writeYammerHistogram() {
        yammerHistogram.update(ThreadLocalRandom.current().nextLong(MAX_VALUE));
    }

    @Benchmark
    @Group("runner")
    @GroupThreads(3)
    public void writeFrameworkHistogram() {
        frameworkSensor.record(ThreadLocalRandom.current().nextLong(MAX_VALUE));
    }

    /*
     * The read benchmark methods below are not real benchmark methods!
     * They are there only to simulate the concurrent exercise of the read and the write paths in
     * the histogram implementations (with the read path exercised significantly less often). The
     * measurements for these benchmark methods should be ignored as although not optimized away
     * (that's why the methods have a return value), they practically measure the cost of a
     * System.currentTimeMillis() call.
     */

    @Benchmark
    @Group("runner")
    @GroupThreads(2)
    public double readYammerHistogram() {
        long now = System.currentTimeMillis();
        if (now % 199 == 0) {
            return yammerHistogram.getSnapshot().get999thPercentile();
        }
        return now;
    }

    @Benchmark
    @Group("runner")
    @GroupThreads(2)
    public double readFrameworkHistogram() {
        long now = System.currentTimeMillis();
        if (now % 199 == 0) {
            return (Double) frameworkP999.metricValue();
        }
        return now;
    }
}
