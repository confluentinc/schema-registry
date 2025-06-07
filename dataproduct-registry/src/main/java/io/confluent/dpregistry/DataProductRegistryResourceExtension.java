/*
 * Copyright 2023 Confluent Inc.
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

package io.confluent.dpregistry;

import com.netflix.governator.InjectorBuilder;
import com.netflix.governator.LifecycleInjector;
import com.netflix.governator.LifecycleInjectorCreator;
import io.confluent.dpregistry.storage.DataProductRegistry;
import io.confluent.dpregistry.web.rest.resources.DataProductRegistryResource;
import io.confluent.kafka.schemaregistry.exceptions.SchemaRegistryException;
import io.confluent.kafka.schemaregistry.rest.SchemaRegistryConfig;
import io.confluent.kafka.schemaregistry.rest.extensions.SchemaRegistryResourceExtension;
import io.confluent.kafka.schemaregistry.storage.SchemaRegistry;
import jakarta.ws.rs.core.Configurable;
import java.io.IOException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DataProductRegistryResourceExtension implements SchemaRegistryResourceExtension {
  private static final Logger LOG =
      LoggerFactory.getLogger(DataProductRegistryResourceExtension.class);

  private LifecycleInjector injector;
  private DataProductRegistry dataProductRegistry;

  @Override
  public void register(
      Configurable<?> configurable,
      SchemaRegistryConfig schemaRegistryConfig,
      SchemaRegistry schemaRegistry
  ) throws SchemaRegistryException {

    // Use the Governator LifecycleInjector to invoke @PostConstruct/@PreDestroy methods.
    // Alternative would be to use Guice.createInjector(new DekRegistryModule(schemaRegistry))
    // and call lifecycycle methods explicitly.
    LOG.debug("registering injector");
    injector = InjectorBuilder.fromModules(new DataProductRegistryModule(schemaRegistry))
        .createInjector(new LifecycleInjectorCreator());
    LOG.debug("done registering injector");

    LOG.debug("registering rest classes");
    configurable.register(injector.getInstance(DataProductRegistryResource.class));
    LOG.debug("done registering rest classes");

    dataProductRegistry = injector.getInstance(DataProductRegistry.class);
  }

  @Override
  public boolean initialized() {
    return dataProductRegistry != null && dataProductRegistry.initialized();
  }

  @Override
  public void close() throws IOException {
    if (injector != null) {
      injector.close();
      injector = null;
    }
  }
}
