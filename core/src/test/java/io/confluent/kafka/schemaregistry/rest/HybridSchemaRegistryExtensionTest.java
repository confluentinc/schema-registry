/*
 * Copyright 2025 Confluent Inc.
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

package io.confluent.kafka.schemaregistry.rest;

import io.confluent.kafka.schemaregistry.ClusterTestHarness;
import io.confluent.kafka.schemaregistry.CompatibilityLevel;
import io.confluent.kafka.schemaregistry.rest.extensions.HybridSchemaRegistryExtension;
import io.confluent.kafka.schemaregistry.rest.handler.HybridSRRequestForwardHandler;
import io.confluent.kafka.schemaregistry.rest.handler.SchemaRegistryHandler;
import io.confluent.kafka.schemaregistry.storage.KafkaSchemaRegistry;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Properties;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class HybridSchemaRegistryExtensionTest extends ClusterTestHarness {

  public HybridSchemaRegistryExtensionTest() {
    super(1, false, CompatibilityLevel.BACKWARD.name);
  }

  Properties getDefaultProperty() {
    Properties props = new Properties();
    schemaRegistryPort = choosePort();
    props.put(SchemaRegistryConfig.LISTENERS_CONFIG, getSchemaRegistryProtocol() +
            "://0.0.0.0:"
            + schemaRegistryPort);
    props.put(SchemaRegistryConfig.MODE_MUTABILITY, true);
    return props;
  }

  @Test
  public void testHybridHandlerAdded() throws Exception {
    Properties props = getDefaultProperty();
    props.put(
            SchemaRegistryConfig.RESOURCE_EXTENSION_CONFIG,
            HybridSchemaRegistryExtension.class.getName()
    );
    setupRestApp(props);
    // Verify that the Hybrid handler is added to the schema registry's custom handlers
    KafkaSchemaRegistry schemaRegistry = (KafkaSchemaRegistry) restApp.schemaRegistry();
    List<SchemaRegistryHandler> customHandlers = schemaRegistry.getCustomHandler();
    assertTrue(customHandlers.stream()
                    .anyMatch(handler -> handler instanceof HybridSRRequestForwardHandler),
            "HybridSRRequestForwardHandler should be in custom handlers");
  }


  @Test
  public void testHybridHandlerNotAdded() throws Exception {
    setupRestApp(getDefaultProperty());
    // Verify that the Hybrid handler is not added to the schema registry's custom handlers
    KafkaSchemaRegistry schemaRegistry = (KafkaSchemaRegistry) restApp.schemaRegistry();
    List<SchemaRegistryHandler> customHandlers = schemaRegistry.getCustomHandler();
    assertFalse(customHandlers.stream()
                    .anyMatch(handler -> handler instanceof HybridSRRequestForwardHandler),
            "HybridSRRequestForwardHandler should not be in custom handlers");
  }
} 