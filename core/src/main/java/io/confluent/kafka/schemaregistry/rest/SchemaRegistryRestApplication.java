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

import org.eclipse.jetty.util.StringUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Properties;

import javax.ws.rs.core.Configurable;

import io.confluent.kafka.schemaregistry.exceptions.SchemaRegistryException;
import io.confluent.kafka.schemaregistry.rest.exceptions.RestSchemaRegistryException;
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
  private SchemaRegistryResourceExtension schemaRegistryResourceExtension = null;

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
      System.exit(1);
    }

    String extensionClassName =
        schemaRegistryConfig.getString(SchemaRegistryConfig
            .SCHEMAREGISTRY_RESOURCE_EXTENSION_CONFIG);

    if (StringUtil.isNotBlank(extensionClassName)) {
      try {
        Class<SchemaRegistryResourceExtension>
            restResourceExtensionClass =
            (Class<SchemaRegistryResourceExtension>) Class.forName(extensionClassName);

        schemaRegistryResourceExtension = restResourceExtensionClass.newInstance();
      } catch (ClassNotFoundException | IllegalAccessException | InstantiationException e) {
        throw new RestSchemaRegistryException(
            "Unable to load resource extension class " + extensionClassName
            + ". Check your classpath and that the configured class implements "
            + "the SchemaRegistryResourceExtension interface.");
      }
    }

    config.register(RootResource.class);
    config.register(new ConfigResource(schemaRegistry));
    config.register(new SubjectsResource(schemaRegistry));
    config.register(new SchemasResource(schemaRegistry));
    config.register(new SubjectVersionsResource(schemaRegistry));
    config.register(new CompatibilityResource(schemaRegistry));

    if (schemaRegistryResourceExtension != null) {
      schemaRegistryResourceExtension.register(config, schemaRegistryConfig, schemaRegistry);
    }
  }

  @Override
  public void onShutdown() {
    schemaRegistry.close();
    if (schemaRegistryResourceExtension != null) {
      try {
        schemaRegistryResourceExtension.close();
      } catch (IOException e) {
        log.error("Error closing the extension resource", e);
      }
    }
  }

  // for testing purpose only
  public KafkaSchemaRegistry schemaRegistry() {
    return schemaRegistry;
  }
}
