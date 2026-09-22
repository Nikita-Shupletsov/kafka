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
 * The per-sample store a {@link Percentiles} sample records into. The seam that lets one
 * {@link SampledStat} ring back either bin layout: a fixed-bin {@link Histogram} for the
 * {@code CONSTANT}/{@code LINEAR}/{@code EXPONENTIAL} {@code BucketSizing} values, or an adaptive
 * {@link ExponentialHistogram}. Reads are mode-specific and reach past this seam to the concrete
 * store; recording and window-recycling are all the ring needs.
 */
interface RecordingStore {

    /** Record one observation into the current sample. */
    void record(double value);

    /** Clear the store when the ring recycles this sample for a new window. */
    void clear();
}
