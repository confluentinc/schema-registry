/*
 * Copyright 2026 Confluent Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.confluent.kafka.schemaregistry.client.rest;

import io.confluent.kafka.schemaregistry.client.SchemaRegistryClientConfig;
import org.junit.Test;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

// Pins down the safer client defaults from issue #4588: during a Schema
// Registry outage the client should bound its connections, fail fast, and
// back off instead of flooding the service with new connections and retries.
public class RestServiceSaferDefaultsTest {

  @Test
  public void testTimeoutDefaultsAreShort() {
    Map<String, Object> configs = Collections.emptyMap();
    assertEquals(800,
        (int) SchemaRegistryClientConfig.getHttpConnectTimeoutMs(configs));
    assertEquals(6000,
        (int) SchemaRegistryClientConfig.getHttpReadTimeoutMs(configs));
    assertEquals(500,
        (int) SchemaRegistryClientConfig.getHttpConnectionRequestTimeoutMs(configs));
  }

  @Test
  public void testRetryBackoffDefaultsAreLonger() {
    Map<String, Object> configs = Collections.emptyMap();
    assertEquals(2000,
        (int) SchemaRegistryClientConfig.getRetriesWaitMs(configs));
    assertEquals(30000,
        (int) SchemaRegistryClientConfig.getRetriesMaxWaitMs(configs));
  }

  @Test
  public void testPoolDefaultsAreBounded() {
    Map<String, Object> configs = Collections.emptyMap();
    assertEquals(10,
        (int) SchemaRegistryClientConfig.getHttpClientPoolMaxTotalConnections(configs));
    assertEquals(10,
        (int) SchemaRegistryClientConfig.getHttpClientPoolMaxPerRouteConnections(configs));
  }

  @Test
  public void testPoolAndTimeoutConfigsCanBeOverridden() {
    Map<String, Object> configs = new HashMap<>();
    configs.put(SchemaRegistryClientConfig.HTTP_CLIENT_POOL_MAX_TOTAL_CONNECTIONS, "25");
    configs.put(SchemaRegistryClientConfig.HTTP_CLIENT_POOL_MAX_PER_ROUTE_CONNECTIONS, "7");
    configs.put(SchemaRegistryClientConfig.HTTP_CONNECTION_REQUEST_TIMEOUT_MS, "1500");
    assertEquals(25,
        (int) SchemaRegistryClientConfig.getHttpClientPoolMaxTotalConnections(configs));
    assertEquals(7,
        (int) SchemaRegistryClientConfig.getHttpClientPoolMaxPerRouteConnections(configs));
    assertEquals(1500,
        (int) SchemaRegistryClientConfig.getHttpConnectionRequestTimeoutMs(configs));
  }

  @Test
  public void testApacheHttpClientIsUsedByDefault() {
    assertTrue(SchemaRegistryClientConfig.useApacheHttpClient(Collections.emptyMap()));

    RestService restService = new RestService("http://localhost:8081");
    restService.configure(Collections.emptyMap());
    assertNotNull(restService.getApacheHttpClient());
  }

  @Test
  public void testApacheHttpClientCanBeDisabled() {
    Map<String, Object> configs = new HashMap<>();
    configs.put(SchemaRegistryClientConfig.USE_APACHE_HTTP_CLIENT, "false");

    RestService restService = new RestService("http://localhost:8081");
    restService.configure(configs);
    assertNull(restService.getApacheHttpClient());
  }
}
