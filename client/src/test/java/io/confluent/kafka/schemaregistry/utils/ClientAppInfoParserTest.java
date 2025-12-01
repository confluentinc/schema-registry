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
  public void testGetUserAgent() {
    String userAgent = ClientAppInfoParser.getUserAgent();
    assertNotNull("User-Agent should not be null", userAgent);
    assertFalse("User-Agent should not be empty", userAgent.isEmpty());

    // Verify format: schema-registry-client-java/{version} (Java/{java.version}; {os.name})
    assertTrue("User-Agent should start with 'schema-registry-client-java/'",
               userAgent.startsWith("schema-registry-client-java/"));
    assertTrue("User-Agent should contain Java version",
               userAgent.contains("Java/"));
    assertTrue("User-Agent should contain parentheses",
               userAgent.contains("(") && userAgent.contains(")"));
  }

  @Test
  public void testUserAgentFormat() {
    String userAgent = ClientAppInfoParser.getUserAgent();

    // Split to verify format
    String[] parts = userAgent.split(" ", 2);
    assertEquals("User-Agent should have two main parts", 2, parts.length);

    // First part: schema-registry-client-java/{version}
    String clientPart = parts[0];
    assertTrue("First part should contain version",
               clientPart.matches("schema-registry-client-java/.*"));

    // Second part: (Java/{version})
    String environmentPart = parts[1];
    assertTrue("Second part should be in parentheses",
               environmentPart.startsWith("(") && environmentPart.endsWith(")"));
    assertTrue("Second part should contain Java version",
               environmentPart.contains("Java/"));
  }
}
