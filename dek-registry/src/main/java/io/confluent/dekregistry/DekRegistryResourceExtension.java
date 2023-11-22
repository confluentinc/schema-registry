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

package io.confluent.dekregistry;

import com.netflix.governator.InjectorBuilder;
import com.netflix.governator.LifecycleInjector;
import com.netflix.governator.LifecycleInjectorCreator;
import io.confluent.dekregistry.storage.DekRegistry;
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
  private DekRegistry dekRegistry;

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
    injector = InjectorBuilder.fromModules(new DekRegistryModule(schemaRegistry))
        .createInjector(new LifecycleInjectorCreator());
    LOG.debug("done registering injector");

    LOG.debug("registering rest classes");
    configurable.register(injector.getInstance(DekRegistryResource.class));
    LOG.debug("done registering rest classes");

    dekRegistry = injector.getInstance(DekRegistry.class);
  }

  @Override
  public boolean initialized() {
    return dekRegistry != null && dekRegistry.initialized();
  }

  @Override
  public void close() throws IOException {
    if (injector != null) {
      injector.close();
      injector = null;
    }
  }
}
