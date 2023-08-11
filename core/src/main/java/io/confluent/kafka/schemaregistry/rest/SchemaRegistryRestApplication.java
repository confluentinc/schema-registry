/*
 * Copyright 2018 Confluent Inc.
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

import io.confluent.kafka.schemaregistry.exceptions.SchemaRegistryException;
import io.confluent.kafka.schemaregistry.rest.extensions.SchemaRegistryResourceExtension;
import io.confluent.kafka.schemaregistry.rest.filters.ContextFilter;
import io.confluent.kafka.schemaregistry.rest.filters.RestCallMetricFilter;
import io.confluent.kafka.schemaregistry.rest.resources.CompatibilityResource;
import io.confluent.kafka.schemaregistry.rest.resources.ConfigResource;
import io.confluent.kafka.schemaregistry.rest.resources.ContextsResource;
import io.confluent.kafka.schemaregistry.rest.resources.ModeResource;
import io.confluent.kafka.schemaregistry.rest.resources.RootResource;
import io.confluent.kafka.schemaregistry.rest.resources.SchemasResource;
import io.confluent.kafka.schemaregistry.rest.resources.ServerMetadataResource;
import io.confluent.kafka.schemaregistry.rest.resources.SubjectVersionsResource;
import io.confluent.kafka.schemaregistry.rest.resources.SubjectsResource;
import io.confluent.kafka.schemaregistry.storage.KafkaSchemaRegistry;
import io.confluent.kafka.schemaregistry.storage.serialization.SchemaRegistrySerializer;
import io.confluent.rest.Application;
import io.confluent.rest.RestConfigException;
import org.eclipse.jetty.servlet.ServletContextHandler;
import org.eclipse.jetty.util.resource.Resource;
import org.eclipse.jetty.util.resource.ResourceCollection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.ws.rs.core.Configurable;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Properties;

public class SchemaRegistryRestApplication extends Application<SchemaRegistryConfig> {

  private static final Logger log = LoggerFactory.getLogger(SchemaRegistryRestApplication.class);
  private KafkaSchemaRegistry schemaRegistry = null;
  private List<SchemaRegistryResourceExtension> schemaRegistryResourceExtensions = null;

  public SchemaRegistryRestApplication(Properties props) throws RestConfigException {
    this(new SchemaRegistryConfig(props));
  }

  @Override
  protected void configurePreResourceHandling(ServletContextHandler context) {
    super.configurePreResourceHandling(context);
    context.setErrorHandler(new JsonErrorHandler());
  }

  public SchemaRegistryRestApplication(SchemaRegistryConfig config) {
    super(config);
  }


  protected KafkaSchemaRegistry initSchemaRegistry(SchemaRegistryConfig config) {
    KafkaSchemaRegistry kafkaSchemaRegistry = null;
    try {
      kafkaSchemaRegistry = new KafkaSchemaRegistry(
          config,
          new SchemaRegistrySerializer()
      );
      kafkaSchemaRegistry.init();
    } catch (SchemaRegistryException e) {
      log.error("Error starting the schema registry", e);
      onShutdown();
      System.exit(1);
    }
    return kafkaSchemaRegistry;
  }

  @Override
  public void configureBaseApplication(
      final Configurable<?> config,
      final Map<String, String> metricTags) {
    super.configureBaseApplication(config, metricTags);

    schemaRegistry = initSchemaRegistry(getConfiguration());
  }

  @Override
  public void setupResources(Configurable<?> config, SchemaRegistryConfig schemaRegistryConfig) {
    schemaRegistryResourceExtensions =
        schemaRegistryConfig.getConfiguredInstances(
            schemaRegistryConfig.definedResourceExtensionConfigName(),
            SchemaRegistryResourceExtension.class);

    config.register(RootResource.class);
    config.register(new ConfigResource(schemaRegistry));
    config.register(new ContextsResource(schemaRegistry));
    config.register(new SubjectsResource(schemaRegistry));
    config.register(new SchemasResource(schemaRegistry));
    config.register(new SubjectVersionsResource(schemaRegistry));
    config.register(new CompatibilityResource(schemaRegistry));
    config.register(new ModeResource(schemaRegistry));
    config.register(new ServerMetadataResource(schemaRegistry));
    config.register(new ContextFilter());
    config.register(new RestCallMetricFilter(
            schemaRegistry.getMetricsContainer().getApiCallsSuccess(),
            schemaRegistry.getMetricsContainer().getApiCallsFailure()));

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
  protected ResourceCollection getStaticResources() {
    List<String> locations = config.getStaticLocations();
    if (locations != null && !locations.isEmpty()) {
      Resource[] resources = locations.stream()
          .map(Resource::newClassPathResource)
          .toArray(Resource[]::new);
      return new ResourceCollection(resources);
    } else {
      return super.getStaticResources();
    }
  }

  @Override
  public void onShutdown() {

    if (schemaRegistry != null) {
      try {
        schemaRegistry.close();
      } catch (IOException e) {
        log.error("Error closing schema registry", e);
      }
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
