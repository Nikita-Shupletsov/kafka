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
import org.apache.kafka.server.metrics.ClientMetricsInstance.PushAdmission;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.net.UnknownHostException;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class ClientMetricsInstanceTest {

    private ClientMetricsInstance clientInstance;

    @BeforeEach
    public void setUp() throws UnknownHostException {
        Uuid uuid = Uuid.randomUuid();
        ClientMetricsInstanceMetadata instanceMetadata = new ClientMetricsInstanceMetadata(uuid,
            ClientMetricsTestUtils.requestContext());
        clientInstance = new ClientMetricsInstance(uuid, instanceMetadata, 0, 0, null, ClientMetricsConfigs.INTERVAL_MS_DEFAULT);
    }

    @Test
    public void testMaybeUpdateRequestTimestampValid() {
        // First request should be accepted.
        assertTrue(clientInstance.maybeUpdateGetRequestTimestamp(System.currentTimeMillis()));
        assertTrue(clientInstance.maybeUpdatePushRequestTimestamp(System.currentTimeMillis()));
    }

    @Test
    public void testMaybeUpdateGetRequestAfterElapsedTimeValid() {
        assertTrue(clientInstance.maybeUpdateGetRequestTimestamp(System.currentTimeMillis() - ClientMetricsConfigs.INTERVAL_MS_DEFAULT));
        // Second request should be accepted as time since last request is greater than the push interval.
        assertTrue(clientInstance.maybeUpdateGetRequestTimestamp(System.currentTimeMillis()));
    }

    @Test
    public void testMaybeUpdateGetRequestWithImmediateRetryFail() {
        assertTrue(clientInstance.maybeUpdateGetRequestTimestamp(System.currentTimeMillis()));
        // Second request should be rejected as time since last request is less than the push interval.
        assertFalse(clientInstance.maybeUpdateGetRequestTimestamp(System.currentTimeMillis()));
    }

    @Test
    public void testMaybeUpdatePushRequestAfterElapsedTimeValid() {
        assertTrue(clientInstance.maybeUpdatePushRequestTimestamp(System.currentTimeMillis() - ClientMetricsConfigs.INTERVAL_MS_DEFAULT));
        // Second request should be accepted as time since last request is greater than the push interval.
        assertTrue(clientInstance.maybeUpdatePushRequestTimestamp(System.currentTimeMillis()));
    }

    @Test
    public void testMaybeUpdateGetRequestWithImmediateRetryAfterPushFail() {
        assertTrue(clientInstance.maybeUpdatePushRequestTimestamp(System.currentTimeMillis()));
        // Next request after push should be rejected as time since last request is less than the push interval.
        assertFalse(clientInstance.maybeUpdateGetRequestTimestamp(System.currentTimeMillis() + 1));
    }

    @Test
    public void testMaybeUpdatePushRequestWithImmediateRetryFail() {
        assertTrue(clientInstance.maybeUpdatePushRequestTimestamp(System.currentTimeMillis()));
        // Second request should be rejected as time since last request is less than the push interval.
        assertFalse(clientInstance.maybeUpdatePushRequestTimestamp(System.currentTimeMillis()));
    }

    @Test
    public void testMaybeUpdatePushRequestWithImmediateRetryAfterGetValid() {
        assertTrue(clientInstance.maybeUpdatePushRequestTimestamp(System.currentTimeMillis() - ClientMetricsConfigs.INTERVAL_MS_DEFAULT));
        assertTrue(clientInstance.maybeUpdateGetRequestTimestamp(System.currentTimeMillis()));
        // Next request after get should be accepted.
        assertTrue(clientInstance.maybeUpdatePushRequestTimestamp(System.currentTimeMillis() + 1));
    }

    @Test
    public void testAdmitsEachDeclaredPartOfAPushOnce() {
        assertEquals(PushAdmission.ACCEPTED, clientInstance.admitPushPart(0, 3));
        assertEquals(PushAdmission.ACCEPTED, clientInstance.admitPushPart(1, 3));
        // A repeated index is a duplicate, telling a retrying client that the part was delivered.
        assertEquals(PushAdmission.DUPLICATE, clientInstance.admitPushPart(1, 3));
        assertEquals(PushAdmission.ACCEPTED, clientInstance.admitPushPart(2, 3));
        // A part declaring another part count belongs to no open push.
        assertEquals(PushAdmission.STRAY, clientInstance.admitPushPart(1, 2));
    }

    @Test
    public void testPartWithoutOpenPushIsStray() {
        assertEquals(PushAdmission.STRAY, clientInstance.admitPushPart(1, 2));
    }

    @Test
    public void testIndexZeroClosesAnIncompletePush() {
        assertEquals(PushAdmission.ACCEPTED, clientInstance.admitPushPart(0, 3));
        // Part 1 never arrives; the next push's index 0 closes this one, and its parts are its own.
        assertEquals(PushAdmission.ACCEPTED, clientInstance.admitPushPart(0, 2));
        assertEquals(PushAdmission.ACCEPTED, clientInstance.admitPushPart(1, 2));
        assertEquals(PushAdmission.STRAY, clientInstance.admitPushPart(2, 3));
    }

    @Test
    public void testWindowAdmitsARetriedPartWithinMaxParts() {
        long now = 1_000_000L;
        assertTrue(clientInstance.maybeUpdateGetRequestTimestamp(now));
        // A two-part push under maxParts 3, with part 1 retried after a network failure.
        for (int part : new int[]{0, 1, 1}) {
            assertTrue(clientInstance.maybeUpdatePushRequestTimestamp(now, part, 3, true));
        }
        // The window has no room for a second push.
        assertFalse(clientInstance.maybeUpdatePushRequestTimestamp(now, 0, 3, true));

        // The next window admits the next push.
        long next = now + ClientMetricsConfigs.INTERVAL_MS_DEFAULT;
        assertTrue(clientInstance.maybeUpdatePushRequestTimestamp(next, 0, 3, true));
    }

    @Test
    public void testWindowAdmitsOnePushOfAtMostMaxPartsRequests() {
        long now = 1_000_000L;
        assertTrue(clientInstance.maybeUpdateGetRequestTimestamp(now));
        for (int part = 0; part < 3; part++) {
            assertTrue(clientInstance.maybeUpdatePushRequestTimestamp(now, part, 3, true));
        }
        // A fourth request, such as a retry of a part that had arrived, is throttled, and so is a second push.
        assertFalse(clientInstance.maybeUpdatePushRequestTimestamp(now, 1, 3, true));
        assertFalse(clientInstance.maybeUpdatePushRequestTimestamp(now, 0, 3, true));
    }

    @Test
    public void testSinglePartClientIsAdmittedOncePerWindow() {
        long now = 1_000_000L;
        assertTrue(clientInstance.maybeUpdateGetRequestTimestamp(now));
        // A request at the previous version is part 0 of 1, admitted once per interval whatever maxParts is.
        assertTrue(clientInstance.maybeUpdatePushRequestTimestamp(now, 0, 3, false));
        assertFalse(clientInstance.maybeUpdatePushRequestTimestamp(now + 1, 0, 3, false));
        // So is a newer client when the broker allows a single part.
        long next = now + ClientMetricsConfigs.INTERVAL_MS_DEFAULT;
        assertTrue(clientInstance.maybeUpdatePushRequestTimestamp(next, 0, 1, true));
        assertFalse(clientInstance.maybeUpdatePushRequestTimestamp(next + 1, 0, 1, true));
    }
}
