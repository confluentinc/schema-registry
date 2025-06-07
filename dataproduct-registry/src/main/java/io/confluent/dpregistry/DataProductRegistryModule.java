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

import com.google.inject.AbstractModule;
import io.confluent.kafka.schemaregistry.storage.SchemaRegistry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DataProductRegistryModule extends AbstractModule {
  private static final Logger LOG = LoggerFactory.getLogger(DataProductRegistryModule.class);

  private final SchemaRegistry schemaRegistry;

  public DataProductRegistryModule(SchemaRegistry schemaRegistry) {
    this.schemaRegistry = schemaRegistry;
  }

  @Override
  protected void configure() {
    LOG.info("Configuring data product registry module");

    bind(SchemaRegistry.class).toInstance(schemaRegistry);

    LOG.info("Done configuring data product registry module");
  }
}
