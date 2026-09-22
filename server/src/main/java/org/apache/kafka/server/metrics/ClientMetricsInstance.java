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
package org.apache.kafka.server.metrics;

import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.server.util.timer.TimerTask;

import java.util.BitSet;
import java.util.List;
import java.util.Objects;

/**
 * Contains the metrics instance metadata and the state of the client instance.
 */
public class ClientMetricsInstance {

    private final Uuid clientInstanceId;
    private final ClientMetricsInstanceMetadata instanceMetadata;
    private final int subscriptionId;
    private final int subscriptionVersion;
    private final List<String> metrics;
    private final int pushIntervalMs;

    private long lastGetRequestTimestamp;
    private long lastPushRequestTimestamp;
    private long pushWindowStartTimestamp;
    private int pushesInWindow;
    private int requestsInWindow;
    // The open push: its declared part count (0 when none is open) and the indices admitted so far.
    private int openPushPartCount;
    private final BitSet openPushParts = new BitSet();
    private volatile boolean terminating;
    private volatile Errors lastKnownError;
    private TimerTask expirationTimerTask;

    public ClientMetricsInstance(Uuid clientInstanceId, ClientMetricsInstanceMetadata instanceMetadata,
        int subscriptionId, int subscriptionVersion, List<String> metrics, int pushIntervalMs) {
        this.clientInstanceId = Objects.requireNonNull(clientInstanceId);
        this.instanceMetadata = Objects.requireNonNull(instanceMetadata);
        this.subscriptionId = subscriptionId;
        this.subscriptionVersion = subscriptionVersion;
        this.metrics = metrics;
        this.terminating = false;
        this.pushIntervalMs = pushIntervalMs;
        this.lastKnownError = Errors.NONE;
    }

    public Uuid clientInstanceId() {
        return clientInstanceId;
    }

    public ClientMetricsInstanceMetadata instanceMetadata() {
        return instanceMetadata;
    }

    public int pushIntervalMs() {
        return pushIntervalMs;
    }

    public int subscriptionId() {
        return subscriptionId;
    }

    public int subscriptionVersion() {
        return subscriptionVersion;
    }

    public List<String> metrics() {
        return metrics;
    }

    public boolean terminating() {
        return terminating;
    }

    public synchronized void terminating(boolean terminating) {
        this.terminating = terminating;
    }

    public Errors lastKnownError() {
        return lastKnownError;
    }

    public synchronized void lastKnownError(Errors lastKnownError) {
        this.lastKnownError = lastKnownError;
    }

    // Visible for testing
    public synchronized TimerTask expirationTimerTask() {
        return expirationTimerTask;
    }

    public synchronized boolean maybeUpdateGetRequestTimestamp(long currentTime) {
        long lastRequestTimestamp = Math.max(lastGetRequestTimestamp, lastPushRequestTimestamp);
        long timeElapsedSinceLastMsg = currentTime - lastRequestTimestamp;
        if (timeElapsedSinceLastMsg >= pushIntervalMs) {
            lastGetRequestTimestamp = currentTime;
            return true;
        }
        return false;
    }

    // A single-part push request, as before this method gained part fields.
    public synchronized boolean maybeUpdatePushRequestTimestamp(long currentTime) {
        return maybeUpdatePushRequestTimestamp(currentTime, 0, 1, false);
    }

    /**
     * Counts one PushTelemetry request against the interval window's budget, before anything else about
     * the request is checked, so that a request rejected later still counts.
     *
     * <p>A new interval window opens when either the first push after a fresh get-subscriptions fetch
     * arrives (the client applies a jitter to the push interval, so a request may be sent between
     * 0.5 * pushIntervalMs and 1.5 * pushIntervalMs), or a full push interval has elapsed since the current
     * window started. A window admits one push of at most {@code maxParts} requests, or of one request for a
     * client that cannot split its pushes ({@code multiPart} false). Anything beyond that is throttled. A part
     * retried after a network failure fits unless the first attempt did reach the broker and the push already
     * uses every part; the retry is then throttled and the client drops the rest of that push.
     *
     * @param currentTime the current time in milliseconds
     * @param partIndex   the request's zero-based index within its push; index 0 opens a push
     * @param maxParts    the most requests a push may span
     * @param multiPart   whether the request is at a version that declares its part
     * @return whether the window has room for the request
     */
    public synchronized boolean maybeUpdatePushRequestTimestamp(long currentTime, int partIndex, int maxParts,
                                                                boolean multiPart) {
        boolean firstPushAfterGet = lastGetRequestTimestamp > lastPushRequestTimestamp;
        boolean windowElapsed = (currentTime - pushWindowStartTimestamp) >= pushIntervalMs;
        if (firstPushAfterGet || windowElapsed) {
            pushWindowStartTimestamp = currentTime;
            pushesInWindow = 0;
            requestsInWindow = 0;
        }

        int requestsPerWindow = multiPart ? maxParts : 1;
        if (requestsInWindow >= requestsPerWindow || (partIndex == 0 && pushesInWindow >= 1)) {
            return false;
        }
        if (partIndex == 0) {
            pushesInWindow++;
        }
        requestsInWindow++;
        lastPushRequestTimestamp = currentTime;
        return true;
    }

    /**
     * Records one PushTelemetry request as a part of a push, once nothing else can reject the request, so that
     * a part rejected for another reason and then retried does not read as a duplicate. Parts of one push
     * arrive in order on one connection, so a request with {@code partIndex == 0} opens a new push, closing
     * whatever push was open, complete or not; any other index continues the open push and is admitted once.
     *
     * <p>{@code partIndex} and {@code partCount} must already be validated against the broker's maximum.
     *
     * @param partIndex the request's zero-based index within its push
     * @param partCount the number of requests in the request's push
     * @return whether the request is accepted, or not a part of the open push
     */
    public synchronized PushAdmission admitPushPart(int partIndex, int partCount) {
        if (partIndex == 0) {
            openPushPartCount = partCount;
            openPushParts.clear();
        } else if (partCount != openPushPartCount) {
            return PushAdmission.STRAY;
        } else if (openPushParts.get(partIndex)) {
            return PushAdmission.DUPLICATE;
        }
        openPushParts.set(partIndex);
        return PushAdmission.ACCEPTED;
    }

    /**
     * The outcome of {@link #admitPushPart}.
     */
    public enum PushAdmission {
        /** The request is admitted. */
        ACCEPTED,
        /** The request continues no open push: none is open, or the open one has a different part count. */
        STRAY,
        /** The request repeats a part the open push has already admitted. */
        DUPLICATE
    }

    public synchronized void cancelExpirationTimerTask() {
        if (expirationTimerTask != null) {
            expirationTimerTask.cancel();
            expirationTimerTask = null;
        }
    }

    public synchronized void updateExpirationTimerTask(TimerTask timerTask) {
        cancelExpirationTimerTask();
        expirationTimerTask = timerTask;
    }
}
