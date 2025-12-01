/*
 * Copyright 2025 Confluent Inc.
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

import org.junit.Test;

import static org.junit.Assert.*;

public class RestServiceUserAgentTest {

  @Test
  public void testDefaultRequestPropertiesIncludesUserAgent() {
    // Verify all expected headers are present
    assertEquals(3, RestService.DEFAULT_REQUEST_PROPERTIES.size());
    assertTrue(RestService.DEFAULT_REQUEST_PROPERTIES.containsKey("Content-Type"));
    assertTrue(RestService.DEFAULT_REQUEST_PROPERTIES.containsKey(RestService.ACCEPT_UNKNOWN_PROPERTIES));
    assertTrue(RestService.DEFAULT_REQUEST_PROPERTIES.containsKey(RestService.USER_AGENT));

    // Verify User-Agent format
    String userAgent = RestService.DEFAULT_REQUEST_PROPERTIES.get(RestService.USER_AGENT);
    assertNotNull(userAgent);
    assertTrue(userAgent.startsWith("schema-registry-client-java/"));
  }
}
