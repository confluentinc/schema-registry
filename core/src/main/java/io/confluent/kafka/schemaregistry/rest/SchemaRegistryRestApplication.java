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

import com.fasterxml.jackson.databind.ObjectMapper;
import io.confluent.kafka.schemaregistry.exceptions.SchemaRegistryException;
import io.confluent.kafka.schemaregistry.rest.exceptions.JettyEofExceptionMapper;
import io.confluent.kafka.schemaregistry.rest.exceptions.JettyEofExceptionWriterInterceptor;
import io.confluent.kafka.schemaregistry.rest.extensions.SchemaRegistryResourceExtension;
import io.confluent.kafka.schemaregistry.rest.filters.AliasFilter;
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
import io.confluent.kafka.schemaregistry.utils.JacksonMapper;
import io.confluent.rest.Application;
import io.confluent.rest.RestConfigException;
import java.util.Arrays;
import java.util.Collection;
import org.eclipse.jetty.ee10.servlet.ServletContextHandler;
import org.eclipse.jetty.server.Handler;
import org.eclipse.jetty.util.resource.Resource;
import org.eclipse.jetty.util.resource.ResourceFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import jakarta.ws.rs.core.Configurable;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Properties;

public class SchemaRegistryRestApplication extends Application<SchemaRegistryConfig> {

  private static final Logger log = LoggerFactory.getLogger(SchemaRegistryRestApplication.class);
  private KafkaSchemaRegistry schemaRegistry = null;

  public SchemaRegistryRestApplication(Properties props) throws RestConfigException {
    this(new SchemaRegistryConfig(props));
  }

  @Override
  protected ObjectMapper getJsonMapper() {
    return JacksonMapper.INSTANCE;
  }

  @Override
  protected void configurePreResourceHandling(ServletContextHandler context) {
    super.configurePreResourceHandling(context);
    context.setErrorHandler(new JsonErrorHandler());
    // This handler runs before first Session, Security or ServletHandler
    context.insertHandler(new RequestHeaderHandler());
    List<Handler.Singleton> schemaRegistryCustomHandlers =
            schemaRegistry.getCustomHandler();
    if (schemaRegistryCustomHandlers != null) {
      for (Handler.Singleton
              schemaRegistryCustomHandler : schemaRegistryCustomHandlers) {
        context.insertHandler(schemaRegistryCustomHandler);
      }
    }
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

  public void postServerStart() {
    try {
      schemaRegistry.postInit();
    } catch (SchemaRegistryException e) {
      log.error("Error starting the schema registry", e);
      System.exit(1);
    }
  }

  @Override
  public void configureBaseApplication(
      final Configurable<?> config,
      final Map<String, String> metricTags) {
    super.configureBaseApplication(config, metricTags);

    SchemaRegistryConfig schemaRegistryConfig = getConfiguration();
    registerInitResourceExtensions(config, schemaRegistryConfig);
    schemaRegistry = initSchemaRegistry(schemaRegistryConfig);
  }

  @Override
  public void setupResources(Configurable<?> config, SchemaRegistryConfig schemaRegistryConfig) {
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
    config.register(new AliasFilter(schemaRegistry));
    config.register(new RestCallMetricFilter(
            schemaRegistry.getMetricsContainer().getApiCallsSuccess(),
            schemaRegistry.getMetricsContainer().getApiCallsFailure()));
    config.register(new JettyEofExceptionMapper());
    config.register(new JettyEofExceptionWriterInterceptor());

    List<SchemaRegistryResourceExtension> schemaRegistryResourceExtensions =
        schemaRegistry.getResourceExtensions();
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

  public void registerInitResourceExtensions(
      Configurable<?> config,
      SchemaRegistryConfig schemaRegistryConfig) {
    List<SchemaRegistryResourceExtension> preInitResourceExtensions =
        schemaRegistryConfig.getConfiguredInstances(
          SchemaRegistryConfig.INIT_RESOURCE_EXTENSION_CONFIG,
          SchemaRegistryResourceExtension.class);
    boolean fipsExtensionProvided = false;
    final String fipsResourceExtensionClassName =
        "io.confluent.kafka.schemaregistry.security.SchemaRegistryFipsResourceExtension";
    if (preInitResourceExtensions != null) {
      try {
        for (SchemaRegistryResourceExtension
            schemaRegistryResourceExtension : preInitResourceExtensions) {
          schemaRegistryResourceExtension.register(config, schemaRegistryConfig, schemaRegistry);
          if (fipsResourceExtensionClassName.equals(
              schemaRegistryResourceExtension.getClass().getCanonicalName())) {
            fipsExtensionProvided = true;
          }
        }
      } catch (SchemaRegistryException e) {
        log.error("Error starting the schema registry resource extension", e);
        System.exit(1);
      }
    }
    if (schemaRegistryConfig.getBoolean(SchemaRegistryConfig.ENABLE_FIPS_CONFIG)
        && !fipsExtensionProvided) {
      log.error("Error enabling the FIPS resource extension: `enable.fips` is set to true but the "
          + "`init.resource.extension.class` config is either not configured or does not contain \""
          + fipsResourceExtensionClassName + "\"");
      System.exit(1);
    }
  }

  @Override
  protected Collection<Resource> getStaticResources() {
    ResourceFactory.LifeCycle resourceFactory = ResourceFactory.lifecycle();
    List<String> locations = config.getStaticLocations();
    if (locations != null && !locations.isEmpty()) {
      Resource[] resources = locations.stream()
          .map(resource -> resourceFactory.newClassLoaderResource(resource))
          .toArray(Resource[]::new);
      return Arrays.asList(resources);
    } else {
      return super.getStaticResources();
    }
  }

  @Override
  public void onShutdown() {
    if (schemaRegistry == null) {
      return;
    }

    try {
      schemaRegistry.close();
    } catch (IOException e) {
      log.error("Error closing schema registry", e);
    }

    List<SchemaRegistryResourceExtension> schemaRegistryResourceExtensions =
        schemaRegistry.getResourceExtensions();
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
