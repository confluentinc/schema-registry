/**
 * Copyright 2016 Confluent Inc.
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
package io.confluent.kafka.schemaregistry.storage;

import org.junit.Test;

import java.util.AbstractMap;
import java.util.LinkedList;
import java.util.List;

import static org.junit.Assert.assertEquals;

public class KafkaSchemaRegistryTest {
  @Test
  public void testGetPortForIdentityPrecedence() {
    List<String> listeners = new LinkedList<String>();
    listeners.add("http://localhost:456");

    AbstractMap.SimpleEntry<String, Integer>  schemeAndPort = KafkaSchemaRegistry
        .getSchemeAndPortForIdentity(123, listeners);
    assertEquals("Expected listeners to take precedence over port.", Integer.valueOf(456),
        schemeAndPort.getValue());
    assertEquals("Expected Scheme match", "http", schemeAndPort.getKey());
  }

  @Test
  public void testGetPortForIdentityNoListeners() {
    List<String> listeners = new LinkedList<String>();
    AbstractMap.SimpleEntry<String, Integer>  schemeAndPort = KafkaSchemaRegistry.getSchemeAndPortForIdentity(123, listeners);
    assertEquals("Expected port to take the configured port value", Integer.valueOf(123),
        schemeAndPort.getValue());
    assertEquals("Expected Scheme match", "http", schemeAndPort.getKey());
  }

  @Test
  public void testGetPortForIdentityMultipleListenersWithHttps() {
    List<String> listeners = new LinkedList<String>();
    listeners.add("http://localhost:123");
    listeners.add("https://localhost:456");

    AbstractMap.SimpleEntry<String, Integer>  schemeAndPort = KafkaSchemaRegistry.getSchemeAndPortForIdentity(-1, listeners);
    assertEquals("Expected HTTPS listener's port to be returned", Integer.valueOf(456),
        schemeAndPort.getValue());
    assertEquals("Expected Scheme match", "https", schemeAndPort.getKey());
  }

  @Test
  public void testGetPortForIdentityMultipleListeners() {
    List<String> listeners = new LinkedList<String>();
    listeners.add("http://localhost:123");
    listeners.add("http://localhost:456");

    AbstractMap.SimpleEntry<String, Integer>  schemeAndPort = KafkaSchemaRegistry.getSchemeAndPortForIdentity(-1, listeners);
    assertEquals("Expected first listener's port to be returned", Integer.valueOf(123),
        schemeAndPort.getValue());
    assertEquals("Expected Scheme match", "http", schemeAndPort.getKey());
  }
}
