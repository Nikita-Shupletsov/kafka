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

import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.annotation.InterfaceAudience;

import java.util.Objects;

/**
 * Names the raw distribution a {@link Percentiles} publishes alongside its percentiles: the ring's closed
 * windows, as a {@link HistogramWindows}, for the telemetry collector to ship for central re-aggregation.
 * Works with every bin layout; the window type follows the store. Mirrors {@link Percentile}, which
 * names a single quantile.
 *
 * <p>The window length and sample count come from the {@link org.apache.kafka.common.metrics.MetricConfig} the
 * stat is registered with, exactly like every other {@link SampledStat}, and the percentiles report over them as
 * usual. The windows' reader may need them retained for longer, which it asks for through
 * {@link org.apache.kafka.common.metrics.MetricValueProvider#retainFor}; that changes what the ring keeps, not
 * what the percentiles read.
 */
@InterfaceAudience.Public
public class RawHistogram {

    private final MetricName name;

    /**
     * @param name the metric name the raw distribution is published under
     */
    public RawHistogram(MetricName name) {
        this.name = Objects.requireNonNull(name, "name");
    }

    public MetricName name() {
        return this.name;
    }
}
