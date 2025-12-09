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

package io.confluent.kafka.schemaregistry.utils;

import org.junit.Test;

import static org.junit.Assert.*;

public class ClientAppInfoParserTest {

  @Test
  public void testGetVersion() {
    String version = ClientAppInfoParser.getVersion();
    assertNotNull("Version should not be null", version);
    assertFalse("Version should not be empty", version.isEmpty());
  }

  @Test
  public void testClientVersionFormat() {
    String clientVersion = ClientAppInfoParser.getClientVersion();

    // Verify format: java/{version}
    String[] parts = clientVersion.split("/", 2);
    assertEquals("Client version should have exactly two parts", 2, parts.length);
    assertEquals("First part should be 'java'", "java", parts[0]);
    assertFalse("Version part should not be empty", parts[1].isEmpty());
  }
}
