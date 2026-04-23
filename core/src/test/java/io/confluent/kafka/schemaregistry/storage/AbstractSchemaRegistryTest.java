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

package io.confluent.kafka.schemaregistry.storage;

import io.confluent.kafka.schemaregistry.ClusterTestHarness;
import io.confluent.kafka.schemaregistry.ParsedSchema;
import io.confluent.kafka.schemaregistry.avro.AvroSchema;
import io.confluent.kafka.schemaregistry.client.rest.entities.Config;
import io.confluent.kafka.schemaregistry.client.rest.entities.Schema;
import io.confluent.kafka.schemaregistry.exceptions.InvalidSchemaException;
import io.confluent.kafka.schemaregistry.exceptions.SchemaRegistryException;
import io.confluent.kafka.schemaregistry.rest.SchemaRegistryConfig;
import io.confluent.kafka.schemaregistry.storage.serialization.SchemaRegistrySerializer;
import io.confluent.rest.RestConfig;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class AbstractSchemaRegistryTest extends ClusterTestHarness {

  private SchemaRegistryConfig config;

  @Override
  public void setUp() throws Exception {
    super.setUp();
    Properties props = new Properties();
    props.setProperty(RestConfig.PORT_CONFIG, "123");
    props.setProperty(RestConfig.LISTENERS_CONFIG, "http://localhost:123");
    props.put(SchemaRegistryConfig.KAFKASTORE_BOOTSTRAP_SERVERS_CONFIG, brokerList);
    props.put(SchemaRegistryConfig.KAFKASTORE_TOPIC_CONFIG, ClusterTestHarness.KAFKASTORE_TOPIC);
    config = new SchemaRegistryConfig(props);
  }

  @Test
  public void testValidateSchemaHookIsInvokedOnRegister() throws SchemaRegistryException {
    AtomicInteger invocations = new AtomicInteger();
    KafkaSchemaRegistry registry = new KafkaSchemaRegistry(config, new SchemaRegistrySerializer()) {
      @Override
      protected void validateSchema(ParsedSchema parsedSchema, Schema schema, Config cfg)
              throws SchemaRegistryException {
        invocations.incrementAndGet();
        super.validateSchema(parsedSchema, schema, cfg);
      }
    };
    registry.init();

    Schema input = new Schema(
            "subject1", -1, -1, AvroSchema.TYPE,
            Collections.emptyList(), StoreUtils.avroSchemaString(1));
    registry.register("subject1", input);

    assertTrue(invocations.get() > 0,
            "Expected validateSchema hook to be invoked during registration");
  }

  @Test
  public void testValidateSchemaOverrideControlsRegistration() throws SchemaRegistryException {
    KafkaSchemaRegistry registry = new KafkaSchemaRegistry(config, new SchemaRegistrySerializer()) {
      @Override
      protected void validateSchema(ParsedSchema parsedSchema, Schema schema, Config cfg) {
        throw new IllegalStateException("sentinel-from-override");
      }
    };
    registry.init();

    Schema input = new Schema(
            "subject1", -1, -1, AvroSchema.TYPE,
            Collections.emptyList(), StoreUtils.avroSchemaString(1));

    InvalidSchemaException ex = assertThrows(InvalidSchemaException.class,
            () -> registry.register("subject1", input));
    assertTrue(ex.getMessage().contains("sentinel-from-override"),
            "Expected InvalidSchemaException to wrap the override's cause, got: " + ex.getMessage());
  }
}
