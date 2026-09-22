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

package org.apache.kafka.common.requests;

import org.apache.kafka.common.message.GetTelemetrySubscriptionsResponseData;
import org.apache.kafka.common.protocol.ByteBufferAccessor;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.protocol.ObjectSerializationCache;

import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;
import java.util.Collections;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class GetTelemetrySubscriptionsResponseTest {

    private GetTelemetrySubscriptionsResponseData roundTrip(GetTelemetrySubscriptionsResponseData data, short version) {
        ObjectSerializationCache cache = new ObjectSerializationCache();
        int size = data.size(cache, version);
        ByteBuffer buffer = ByteBuffer.allocate(size);
        data.write(new ByteBufferAccessor(buffer), cache, version);
        buffer.flip();
        return new GetTelemetrySubscriptionsResponseData(new ByteBufferAccessor(buffer), version);
    }

    @Test
    public void testTelemetryMaxPartsRoundTripV1() {
        GetTelemetrySubscriptionsResponseData data = new GetTelemetrySubscriptionsResponseData()
                .setTelemetryMaxParts((short) 5);
        GetTelemetrySubscriptionsResponseData decoded = roundTrip(data, (short) 1);
        assertEquals(5, decoded.telemetryMaxParts());
    }

    @Test
    public void testTelemetryMaxPartsDefaultsToOneAtV0() {
        // The field does not exist at v0; it defaults to 1 when decoded. Leaving it at its
        // default keeps the v0 write legal (writing a non-default v1+ field at v0 is rejected).
        GetTelemetrySubscriptionsResponseData data = new GetTelemetrySubscriptionsResponseData();
        GetTelemetrySubscriptionsResponseData decoded = roundTrip(data, (short) 0);
        assertEquals(1, decoded.telemetryMaxParts());
    }

    @Test
    public void testErrorCountsReturnsNoneWhenNoErrors() {
        GetTelemetrySubscriptionsResponseData data = new GetTelemetrySubscriptionsResponseData()
                .setErrorCode(Errors.NONE.code());
        GetTelemetrySubscriptionsResponse response = new GetTelemetrySubscriptionsResponse(data);
        assertEquals(Collections.singletonMap(Errors.NONE, 1), response.errorCounts());
    }

    @Test
    public void testErrorCountsReturnsOneError() {
        GetTelemetrySubscriptionsResponseData data = new GetTelemetrySubscriptionsResponseData()
                .setErrorCode(Errors.CLUSTER_AUTHORIZATION_FAILED.code());
        data.setErrorCode(Errors.INVALID_CONFIG.code());

        GetTelemetrySubscriptionsResponse response = new GetTelemetrySubscriptionsResponse(data);
        assertEquals(Collections.singletonMap(Errors.INVALID_CONFIG, 1), response.errorCounts());
    }
}
