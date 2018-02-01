/**
 * Copyright 2014 Confluent Inc.
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

package io.confluent.kafka.schemaregistry.rest;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;
import java.util.Properties;

import javax.ws.rs.core.Configurable;

import io.confluent.kafka.schemaregistry.exceptions.SchemaRegistryException;
import io.confluent.kafka.schemaregistry.rest.extensions.SchemaRegistryResourceExtension;
import io.confluent.kafka.schemaregistry.rest.resources.CompatibilityResource;
import io.confluent.kafka.schemaregistry.rest.resources.ConfigResource;
import io.confluent.kafka.schemaregistry.rest.resources.RootResource;
import io.confluent.kafka.schemaregistry.rest.resources.SchemasResource;
import io.confluent.kafka.schemaregistry.rest.resources.SubjectVersionsResource;
import io.confluent.kafka.schemaregistry.rest.resources.SubjectsResource;
import io.confluent.kafka.schemaregistry.storage.KafkaSchemaRegistry;
import io.confluent.kafka.schemaregistry.storage.serialization.SchemaRegistrySerializer;
import io.confluent.rest.Application;
import io.confluent.rest.RestConfigException;

public class SchemaRegistryRestApplication extends Application<SchemaRegistryConfig> {

  private static final Logger log = LoggerFactory.getLogger(SchemaRegistryRestApplication.class);
  private KafkaSchemaRegistry schemaRegistry = null;
  private List<SchemaRegistryResourceExtension> schemaRegistryResourceExtensions = null;

  public SchemaRegistryRestApplication(Properties props) throws RestConfigException {
    this(new SchemaRegistryConfig(props));
  }

  public SchemaRegistryRestApplication(SchemaRegistryConfig config) {
    super(config);

  }

  @Override
  public void setupResources(Configurable<?> config, SchemaRegistryConfig schemaRegistryConfig) {
    try {
      schemaRegistry = new KafkaSchemaRegistry(
          schemaRegistryConfig,
          new SchemaRegistrySerializer()
      );
      schemaRegistry.init();
    } catch (SchemaRegistryException e) {
      log.error("Error starting the schema registry", e);
      onShutdown();
      System.exit(1);
    }

    schemaRegistryResourceExtensions =
        schemaRegistryConfig.getConfiguredInstances(
            SchemaRegistryConfig.SCHEMAREGISTRY_RESOURCE_EXTENSION_CONFIG,
            SchemaRegistryResourceExtension.class);

    config.register(RootResource.class);
    config.register(new ConfigResource(schemaRegistry));
    config.register(new SubjectsResource(schemaRegistry));
    config.register(new SchemasResource(schemaRegistry));
    config.register(new SubjectVersionsResource(schemaRegistry));
    config.register(new CompatibilityResource(schemaRegistry));

    if (schemaRegistryResourceExtensions != null) {
      try {
        for (SchemaRegistryResourceExtension
            schemaRegistryResourceExtension : schemaRegistryResourceExtensions) {
          schemaRegistryResourceExtension.register(config, schemaRegistryConfig, schemaRegistry);
        }
      } catch (SchemaRegistryException e) {
        log.error("Error starting the schema registry", e);
        System.exit(1);
      }
    }
  }

  @Override
  public void onShutdown() {

    if (schemaRegistry != null) {
      schemaRegistry.close();
    }

    if (schemaRegistryResourceExtensions != null) {
      for (SchemaRegistryResourceExtension
          schemaRegistryResourceExtension : schemaRegistryResourceExtensions) {
        try {
          schemaRegistryResourceExtension.close();
        } catch (IOException e) {
          log.error("Error closing the extension resource", e);
        }
      }
    }
  }

  // for testing purpose only
  public KafkaSchemaRegistry schemaRegistry() {
    return schemaRegistry;
  }
}
