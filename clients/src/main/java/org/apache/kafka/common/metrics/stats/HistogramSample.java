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

/**
 * A {@link SampledStat} sample that records into a {@link RecordingStore} — the shared sample type for
 * the histogram-family compound stats ({@link Percentiles}, {@link Frequencies}). The ring recycles the
 * sample by clearing its store; reads reach past {@link #store} to the concrete store the stat created.
 */
class HistogramSample extends SampledStat.Sample {

    final RecordingStore store;

    HistogramSample(RecordingStore store, long now) {
        super(0.0, now);
        this.store = store;
    }

    @Override
    public void reset(long now) {
        super.reset(now);
        store.clear();
    }
}
