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

import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigException;

import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class MetricConfigsTest {

    private AbstractConfig config(Map<String, Object> props) {
        return new AbstractConfig(MetricConfigs.CONFIG_DEF, props);
    }

    @Test
    public void testClientTelemetryMaxPartsDefault() {
        AbstractConfig config = config(new HashMap<>());
        assertEquals(MetricConfigs.CLIENT_TELEMETRY_MAX_PARTS_DEFAULT,
            config.getInt(MetricConfigs.CLIENT_TELEMETRY_MAX_PARTS_CONFIG));
        assertEquals(1, config.getInt(MetricConfigs.CLIENT_TELEMETRY_MAX_PARTS_CONFIG));
    }

    @Test
    public void testClientTelemetryMaxPartsCustomValue() {
        Map<String, Object> props = new HashMap<>();
        props.put(MetricConfigs.CLIENT_TELEMETRY_MAX_PARTS_CONFIG, "4");
        assertEquals(4, config(props).getInt(MetricConfigs.CLIENT_TELEMETRY_MAX_PARTS_CONFIG));
    }

    @Test
    public void testClientTelemetryMaxPartsRejectsBelowOne() {
        Map<String, Object> props = new HashMap<>();
        props.put(MetricConfigs.CLIENT_TELEMETRY_MAX_PARTS_CONFIG, "0");
        assertThrows(ConfigException.class, () -> config(props));
    }

    @Test
    public void testClientTelemetryMaxPartsAcceptsMaximum() {
        Map<String, Object> props = new HashMap<>();
        props.put(MetricConfigs.CLIENT_TELEMETRY_MAX_PARTS_CONFIG, "255");
        assertEquals(255, config(props).getInt(MetricConfigs.CLIENT_TELEMETRY_MAX_PARTS_CONFIG));
    }

    @Test
    public void testClientTelemetryMaxPartsRejectsAboveMaximum() {
        Map<String, Object> props = new HashMap<>();
        props.put(MetricConfigs.CLIENT_TELEMETRY_MAX_PARTS_CONFIG, "256");
        assertThrows(ConfigException.class, () -> config(props));
    }
}
