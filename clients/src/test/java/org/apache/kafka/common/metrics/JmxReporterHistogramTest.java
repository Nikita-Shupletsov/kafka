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
package org.apache.kafka.common.metrics;

import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.metrics.stats.Avg;
import org.apache.kafka.common.metrics.stats.Percentile;
import org.apache.kafka.common.metrics.stats.Percentiles;
import org.apache.kafka.common.metrics.stats.RawHistogram;

import org.junit.jupiter.api.Test;

import java.lang.management.ManagementFactory;
import java.util.Arrays;
import java.util.Collections;

import javax.management.MBeanServer;
import javax.management.ObjectName;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class JmxReporterHistogramTest {

    @Test
    public void testRawHistogramIsNotExposedAsJmxAttribute() throws Exception {
        MBeanServer server = ManagementFactory.getPlatformMBeanServer();
        Metrics metrics = new Metrics();
        try (JmxReporter reporter = new JmxReporter()) {
            metrics.addReporter(reporter);

            MetricName base = metrics.metricName("request-latency", "group1", Collections.emptyMap());
            Sensor sensor = metrics.sensor("latency");
            // A raw adaptive histogram (no percentiles) → registers just the base windows metric.
            sensor.add(new Percentiles(40, 8, new RawHistogram(base)));
            // A companion numeric metric so the group's MBean exists.
            sensor.add(metrics.metricName("request-latency-avg", "group1", Collections.emptyMap()), new Avg());
            sensor.record(5.0);

            // JmxReporter's default prefix is empty, so the MBean domain is empty too.
            ObjectName mbean = new ObjectName(":type=group1");

            // The raw histogram windows (non-numeric value) must NOT be a JMX attribute.
            assertFalse(hasAttribute(server, mbean, "request-latency"),
                "the raw histogram windows must not be a JMX attribute");
            // Sanity: an ordinary numeric metric in the same group IS exposed.
            assertTrue(hasAttribute(server, mbean, "request-latency-avg"),
                "an ordinary numeric metric must be exposed as a JMX attribute");
        } finally {
            metrics.close();
        }
    }

    @Test
    public void testPercentilesAreExposedNextToRawHistogram() throws Exception {
        MBeanServer server = ManagementFactory.getPlatformMBeanServer();
        Metrics metrics = new Metrics();
        try (JmxReporter reporter = new JmxReporter()) {
            metrics.addReporter(reporter);

            MetricName base = metrics.metricName("fetch-latency", "group2", Collections.emptyMap());
            MetricName p99 = metrics.metricName("fetch-latency-p99", "group2", Collections.emptyMap());
            Sensor sensor = metrics.sensor("fetch");
            sensor.add(new Percentiles(40, 8, new RawHistogram(base), new Percentile(p99, 99)));
            sensor.record(5.0);

            ObjectName mbean = new ObjectName(":type=group2");
            assertTrue(hasAttribute(server, mbean, "fetch-latency-p99"),
                "a percentile of a raw histogram's stat must be exposed as a JMX attribute");
            assertFalse(hasAttribute(server, mbean, "fetch-latency"),
                "the raw histogram windows must not be a JMX attribute");
        } finally {
            metrics.close();
        }
    }

    private boolean hasAttribute(MBeanServer server, ObjectName mbean, String attribute)
        throws Exception {
        return Arrays.stream(server.getMBeanInfo(mbean).getAttributes())
            .anyMatch(a -> a.getName().equals(attribute));
    }
}
