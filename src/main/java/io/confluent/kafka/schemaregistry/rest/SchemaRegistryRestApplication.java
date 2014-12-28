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

import java.util.Properties;

import javax.ws.rs.core.Configurable;

import io.confluent.kafka.schemaregistry.rest.resources.RootResource;
import io.confluent.kafka.schemaregistry.rest.resources.SchemasResource;
import io.confluent.kafka.schemaregistry.rest.resources.TopicsResource;
import io.confluent.kafka.schemaregistry.storage.KafkaSchemaRegistry;
import io.confluent.kafka.schemaregistry.storage.SchemaRegistry;
import io.confluent.kafka.schemaregistry.storage.SchemaRegistryConfig;
import io.confluent.kafka.schemaregistry.storage.exceptions.SchemaRegistryException;
import io.confluent.kafka.schemaregistry.storage.serialization.SchemaSerializer;
import io.confluent.rest.Application;
import io.confluent.rest.ConfigurationException;

public class SchemaRegistryRestApplication extends Application<SchemaRegistryRestConfiguration> {

  private static final Logger log = LoggerFactory.getLogger(SchemaRegistryRestApplication.class);
  private SchemaRegistry schemaRegistry = null;

  public SchemaRegistryRestApplication() throws ConfigurationException {
    this(new Properties());
  }

  public SchemaRegistryRestApplication(Properties props) throws ConfigurationException {
    this(new SchemaRegistryRestConfiguration(props));
  }

  public SchemaRegistryRestApplication(SchemaRegistryRestConfiguration config) {
    this.config = config;
  }

  @Override
  public void setupResources(Configurable<?> config, SchemaRegistryRestConfiguration appConfig) {
    SchemaRegistryConfig schemaRegistryConfig = appConfig.getSchemaRegistryConfig();

    try {
      schemaRegistry = new KafkaSchemaRegistry(schemaRegistryConfig, new SchemaSerializer());
      schemaRegistry.init();
    } catch (SchemaRegistryException e) {
      log.error("Error starting the schema registry", e);
      System.exit(1);
    }
    config.register(RootResource.class);
    config.register(new TopicsResource(schemaRegistry));
    config.register(SchemasResource.class);
  }

  @Override
  public SchemaRegistryRestConfiguration configure() throws ConfigurationException {
    return config;
  }

  // for testing purpose only
  public SchemaRegistry schemaRegistry() {
    return schemaRegistry;
  }
}
