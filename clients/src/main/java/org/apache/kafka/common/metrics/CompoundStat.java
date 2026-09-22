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
import org.apache.kafka.common.annotation.InterfaceAudience;

import java.util.ArrayList;
import java.util.List;

/**
 * A compound stat is a stat where a single measurement and associated data structure feeds many metrics. This is the
 * example for a histogram which has many associated percentiles.
 */
@InterfaceAudience.Public
public interface CompoundStat extends Stat {

    /**
     * The numeric ({@link Measurable}) metrics this stat feeds.
     */
    List<NamedMeasurable> stats();

    /**
     * The named metrics this stat feeds, each with its {@link MetricValueProvider} (numeric or not).
     * Registered by {@code Sensor.add(CompoundStat)}. Defaults to {@link #stats()}; override only to also
     * feed non-numeric metrics (e.g. a raw histogram distribution), otherwise the numeric {@link #stats()}
     * are exposed unchanged.
     */
    default List<NamedMetricValueProvider> providers() {
        return new ArrayList<>(stats());
    }

    /**
     * A named metric with its value provider, which may be numeric or not.
     */
    class NamedMetricValueProvider {

        private final MetricName name;
        private final MetricValueProvider<?> metricValueProvider;

        public NamedMetricValueProvider(MetricName name, MetricValueProvider<?> metricValueProvider) {
            this.name = name;
            this.metricValueProvider = metricValueProvider;
        }

        public MetricName name() {
            return name;
        }

        public MetricValueProvider<?> metricValueProvider() {
            return metricValueProvider;
        }
    }

    /**
     * A {@link NamedMetricValueProvider} whose value provider is a {@link Measurable} (numeric).
     */
    class NamedMeasurable extends NamedMetricValueProvider {

        public NamedMeasurable(MetricName name, Measurable stat) {
            super(name, stat);
        }

        public Measurable stat() {
            return (Measurable) metricValueProvider();
        }
    }

}
