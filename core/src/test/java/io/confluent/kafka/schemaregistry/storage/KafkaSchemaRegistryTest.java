/*
 * Copyright 2016-2025 Confluent Inc.
 *
 * Licensed under the Confluent Community License (the "License"); you may not use
 * this file except in compliance with the License.  You may obtain a copy of the
 * License at
 *
 * http://www.confluent.io/confluent-community-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OF ANY KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations under the License.
 */

package io.confluent.kafka.schemaregistry.storage;

import org.junit.Test;

import java.util.LinkedList;
import java.util.List;

import io.confluent.kafka.schemaregistry.exceptions.SchemaRegistryException;
import io.confluent.kafka.schemaregistry.rest.SchemaRegistryConfig;

import static org.junit.Assert.assertEquals;

public class KafkaSchemaRegistryTest {

  @Test
  public void testGetPortForIdentityPrecedence() throws SchemaRegistryException {
    List<String> listeners = new LinkedList<String>();
    listeners.add("http://localhost:456");

    KafkaSchemaRegistry.SchemeAndPort schemeAndPort =
        KafkaSchemaRegistry.getSchemeAndPortForIdentity(123, listeners, SchemaRegistryConfig.HTTP);
    assertEquals("Expected listeners to take precedence over port.", 456, schemeAndPort.port);
    assertEquals("Expected Scheme match", SchemaRegistryConfig.HTTP, schemeAndPort.scheme);
  }

  @Test
  public void testGetPortForIdentityNoListeners() throws SchemaRegistryException {
    List<String> listeners = new LinkedList<String>();
    KafkaSchemaRegistry.SchemeAndPort schemeAndPort =
        KafkaSchemaRegistry.getSchemeAndPortForIdentity(123, listeners, SchemaRegistryConfig.HTTP);
    assertEquals("Expected port to take the configured port value", 123, schemeAndPort.port);
    assertEquals("Expected Scheme match", SchemaRegistryConfig.HTTP, schemeAndPort.scheme);
  }

  @Test
  public void testGetPortForIdentityMultipleListenersWithHttps() throws SchemaRegistryException {
    List<String> listeners = new LinkedList<String>();
    listeners.add("http://localhost:123");
    listeners.add("https://localhost:456");

    KafkaSchemaRegistry.SchemeAndPort schemeAndPort =
        KafkaSchemaRegistry.getSchemeAndPortForIdentity(-1, listeners, SchemaRegistryConfig.HTTPS);
    assertEquals("Expected HTTPS listener's port to be returned", 456, schemeAndPort.port);
    assertEquals("Expected Scheme match", SchemaRegistryConfig.HTTPS, schemeAndPort.scheme);
  }

  @Test
  public void testGetPortForIdentityMultipleListeners() throws SchemaRegistryException {
    List<String> listeners = new LinkedList<String>();
    listeners.add("http://localhost:123");
    listeners.add("http://localhost:456");

    KafkaSchemaRegistry.SchemeAndPort schemeAndPort =
        KafkaSchemaRegistry.getSchemeAndPortForIdentity(-1, listeners, SchemaRegistryConfig.HTTP);
    assertEquals("Expected first listener's port to be returned", 123, schemeAndPort.port);
    assertEquals("Expected Scheme match", SchemaRegistryConfig.HTTP, schemeAndPort.scheme);
  }
}
