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

import org.apache.kafka.common.annotation.InterfaceAudience;

/**
 * Super-interface for {@link Measurable} or {@link Gauge} that provides
 * metric values.
 */
@FunctionalInterface
@InterfaceAudience.Public
public interface MetricValueProvider<T> {

    /**
     * Returns the current value associated with this metric.
     *
     * @param config The configuration for this metric
     * @param now The POSIX time in milliseconds the measurement is being taken
     * @return the current metric value
     */
    T value(MetricConfig config, long now);

    /**
     * Whether reporters that expose metrics over JMX should expose this metric. Most values can be exposed; one
     * with no JMX representation, such as a raw histogram meant for client telemetry, cannot.
     *
     * @return true if the metric should be exposed over JMX
     */
    default boolean jmxExportable() {
        return true;
    }

    /**
     * Tells this provider how often a reader will read it. A provider whose value is a series of time windows
     * that the reader must each see once keeps enough windows to cover {@code readIntervalMs}, so none expires
     * between two reads. The default does nothing.
     * Called under the lock the metric is recorded and read under.
     *
     * @param readIntervalMs How often the reader reads the metric, in milliseconds
     * @param config The configuration for this metric
     */
    default void retainFor(long readIntervalMs, MetricConfig config) {
    }

}