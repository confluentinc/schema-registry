/*
 * Copyright 2023 Confluent Inc.
 */

package io.confluent.dekregistry;

import com.netflix.governator.InjectorBuilder;
import com.netflix.governator.LifecycleInjector;
import com.netflix.governator.LifecycleInjectorCreator;
import io.confluent.kafka.schemaregistry.exceptions.SchemaRegistryException;
import io.confluent.kafka.schemaregistry.rest.SchemaRegistryConfig;
import io.confluent.kafka.schemaregistry.rest.extensions.SchemaRegistryResourceExtension;
import io.confluent.kafka.schemaregistry.storage.SchemaRegistry;
import io.confluent.dekregistry.web.rest.resources.DekRegistryResource;
import java.io.IOException;
import javax.ws.rs.core.Configurable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DekRegistryResourceExtension implements SchemaRegistryResourceExtension {
  private static final Logger LOG = LoggerFactory.getLogger(DekRegistryResourceExtension.class);

  private LifecycleInjector injector;

  @Override
  public void register(
      Configurable<?> configurable,
      SchemaRegistryConfig schemaRegistryConfig,
      SchemaRegistry schemaRegistry
  ) throws SchemaRegistryException {

    // Use the Governator LifecycleInjector to invoke @PostConstruct/@PreDestroy methods.
    // Alternative would be to use Guice.createInjector(new DataCatalogModule())
    // and call lifecycycle methods explicitly.
    LOG.debug("registering injector");
    injector = InjectorBuilder.fromModules(new DekRegistryModule(schemaRegistry))
        .createInjector(new LifecycleInjectorCreator());
    LOG.debug("done registering injector");

    LOG.debug("registering rest classes");
    configurable.register(injector.getInstance(DekRegistryResource.class));
    LOG.debug("done registering rest classes");
  }

  @Override
  public void close() throws IOException {
  }
}
