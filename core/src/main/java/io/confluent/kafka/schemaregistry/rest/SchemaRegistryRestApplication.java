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

import java.net.URI;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;

import javax.ws.rs.core.Configurable;

import io.confluent.kafka.schemaregistry.rest.resources.CompatibilityResource;
import io.confluent.kafka.schemaregistry.rest.resources.ConfigResource;
import io.confluent.kafka.schemaregistry.rest.resources.RootResource;
import io.confluent.kafka.schemaregistry.rest.resources.SchemasResource;
import io.confluent.kafka.schemaregistry.rest.resources.SubjectVersionsResource;
import io.confluent.kafka.schemaregistry.rest.resources.SubjectsResource;
import io.confluent.kafka.schemaregistry.storage.KafkaSchemaRegistry;
import io.confluent.kafka.schemaregistry.exceptions.SchemaRegistryException;
import io.confluent.kafka.schemaregistry.storage.serialization.SchemaRegistrySerializer;
import io.confluent.rest.Application;
import io.confluent.rest.RestConfig;
import io.confluent.rest.RestConfigException;

public class SchemaRegistryRestApplication extends Application<SchemaRegistryConfig> {

  private static final Logger log = LoggerFactory.getLogger(SchemaRegistryRestApplication.class);
  private KafkaSchemaRegistry schemaRegistry = null;

  public SchemaRegistryRestApplication(Properties props) throws RestConfigException {
    this(new SchemaRegistryConfig(props));
  }

  public SchemaRegistryRestApplication(SchemaRegistryConfig config) {
    super(config);
  }

  @Override
  public void setupResources(Configurable<?> config, SchemaRegistryConfig schemaRegistryConfig) {
    try {
      schemaRegistry = new KafkaSchemaRegistry(this,
                                               new SchemaRegistrySerializer());
      // This is a bit of a hack to deal with startup ordering issues when port == 0. See note in
      // onStarted.
      if (listeners().get(0).getPort() > 0) {
        schemaRegistry.init();
      }
    } catch (SchemaRegistryException e) {
      log.error("Error starting the schema registry", e);
      System.exit(1);
    }
    config.register(RootResource.class);
    config.register(new ConfigResource(schemaRegistry));
    config.register(new SubjectsResource(schemaRegistry));
    config.register(new SchemasResource(schemaRegistry));
    config.register(new SubjectVersionsResource(schemaRegistry));
    config.register(new CompatibilityResource(schemaRegistry));
  }

  @Override
  public void onStarted() {
    // We override this only for the case of unit/integration tests that use port == 0 to avoid
    // issues with requiring specific ports/parallel tests/ability to determine truly free ports.
    // We have to reorder startup a bit in this case because schema registry identity information
    // includes the port, but we cannot know this until Jetty completes startup. For the relevant
    // tests, it is safe to reorder since the relevant steps are blocking, so by the time we finally
    // return from the Jetty's Server.start() call, everything will be initialized. In real
    // deployments the port would always be fixed and we would want to run this initialization
    // before Server.start() (in setupResources) to ensure we have a connection and can handle
    // requests when we start Jetty listening.
    if (listeners().get(0).getPort() == 0) {
      try {
        schemaRegistry.init();
      } catch (SchemaRegistryException e) {
        log.error("Error starting the schema registry", e);
        System.exit(1);
      }
    }
  }

  @Override
  public void onShutdown() {
    schemaRegistry.close();
  }

  // for testing purpose only
  public KafkaSchemaRegistry schemaRegistry() {
    return schemaRegistry;
  }

  private List<URI> listeners() {
    return parseListeners(
        config.getList(RestConfig.LISTENERS_CONFIG),
        config.getInt(RestConfig.PORT_CONFIG),
        Arrays.asList("http", "https"),
        "http"
    );
  }
}
