/*
 * Copyright 2026 Confluent Inc.
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

package io.confluent.eventfeed;

import com.netflix.governator.InjectorBuilder;
import com.netflix.governator.LifecycleInjector;
import com.netflix.governator.LifecycleInjectorCreator;
import io.cloudevents.http.restful.ws.CloudEventsProvider;
import io.confluent.eventfeed.web.rest.exceptionmappers.CloudEventDeserializationExceptionMapper;
import io.confluent.eventfeed.web.rest.resources.EventFeedResource;
import io.confluent.kafka.schemaregistry.exceptions.SchemaRegistryException;
import io.confluent.kafka.schemaregistry.rest.SchemaRegistryConfig;
import io.confluent.kafka.schemaregistry.rest.extensions.SchemaRegistryResourceExtension;
import io.confluent.kafka.schemaregistry.storage.SchemaRegistry;
import jakarta.ws.rs.core.Configurable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

public class EventFeedResourceExtension implements SchemaRegistryResourceExtension {
  private static final Logger LOG = LoggerFactory.getLogger(EventFeedResourceExtension.class);

  private LifecycleInjector injector;

  protected LifecycleInjector createInjector(SchemaRegistry schemaRegistry) {
    return InjectorBuilder.fromModules(new EventFeedModule(schemaRegistry))
            .createInjector(new LifecycleInjectorCreator());
  }

  @Override
  public void register(
          Configurable<?> config,
          SchemaRegistryConfig schemaRegistryConfig,
          SchemaRegistry schemaRegistry) throws SchemaRegistryException {
    config.register(new CloudEventsProvider());
    config.register(new CloudEventDeserializationExceptionMapper());

    // Use the Governator LifecycleInjector to invoke @PostConstruct/@PreDestroy methods.
    // Alternative would be to use Guice.createInjector(new DataCatalogModule())
    // and call lifecycycle methods explicitly.
    LOG.debug("registering injector");
    injector = createInjector(schemaRegistry);
    LOG.debug("done registering injector");

    LOG.debug("registering rest classes");
    config.register(injector.getInstance(EventFeedResource.class));
    LOG.debug("done registering rest classes");
  }

  @Override
  public void close() throws IOException {
    if (injector != null) {
      injector.close();
      injector = null;
    }
  }
}
