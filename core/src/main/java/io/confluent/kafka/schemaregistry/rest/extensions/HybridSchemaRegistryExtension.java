/*
 * Copyright 2025 Confluent Inc.
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

package io.confluent.kafka.schemaregistry.rest.extensions;

import io.confluent.kafka.schemaregistry.exceptions.SchemaRegistryException;
import io.confluent.kafka.schemaregistry.rest.SchemaRegistryConfig;
import io.confluent.kafka.schemaregistry.rest.handler.HybridSRRequestForwardHandler;
import io.confluent.kafka.schemaregistry.storage.KafkaSchemaRegistry;
import io.confluent.kafka.schemaregistry.storage.SchemaRegistry;
import jakarta.ws.rs.core.Configurable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;

public class HybridSchemaRegistryExtension implements SchemaRegistryResourceExtension {
  private static final Logger LOG = LoggerFactory.getLogger(HybridSchemaRegistryExtension.class);

  KafkaSchemaRegistry schemaRegistry;
  @Override
  public void register(Configurable<?> config, SchemaRegistryConfig schemaRegistryConfig, SchemaRegistry schemaRegistry) throws SchemaRegistryException {
    LOG.debug("adding HybridSRRequestForwardHandler");
    this.schemaRegistry = (KafkaSchemaRegistry) schemaRegistry;
    this.schemaRegistry.addCustomHandler(List.of(new HybridSRRequestForwardHandler()));
  }

  @Override
  public boolean initialized() {
    return SchemaRegistryResourceExtension.super.initialized();
  }

  @Override
  public boolean healthy() {
    return SchemaRegistryResourceExtension.super.healthy();
  }

  @Override
  public void close() throws IOException {

  }
}
