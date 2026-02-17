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

import com.google.inject.AbstractModule;
import io.confluent.kafka.schemaregistry.storage.SchemaRegistry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class EventFeedModule extends AbstractModule {
  private static final Logger log = LoggerFactory.getLogger(EventFeedModule.class);

  private final SchemaRegistry schemaRegistry;

  public EventFeedModule(SchemaRegistry schemaRegistry) {
    this.schemaRegistry = schemaRegistry;
  }

  @Override
  protected void configure() {
    log.info("Configuring event feed module.");
    bind(SchemaRegistry.class).toInstance(schemaRegistry);
    log.info("Done configuring event feed module.");
  }
}
