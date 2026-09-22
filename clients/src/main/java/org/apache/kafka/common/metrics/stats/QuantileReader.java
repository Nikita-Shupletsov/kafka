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
 * The read side of one kind of {@link RecordingStore}: built for a single percentile read, given the stores of
 * the samples that read covers, then asked for a quantile. It holds the stores by reference and walks their
 * bins in place, so a read allocates nothing in proportion to the number of bins. The stores must not
 * change while the reader is in use, which holds because a stat is read under the same lock it records under.
 */
interface QuantileReader {

    /** Adds the observations of one store to the read. */
    void add(RecordingStore store);

    /**
     * The value at {@code quantile}, in {@code [0, 1]}, across the stores added so far; {@code NaN} when they hold
     * no observations.
     */
    double valueAt(double quantile);
}
