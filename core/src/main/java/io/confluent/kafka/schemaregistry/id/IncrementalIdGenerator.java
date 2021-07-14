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

package io.confluent.kafka.schemaregistry.id;

import io.confluent.kafka.schemaregistry.exceptions.IdGenerationException;
import io.confluent.kafka.schemaregistry.rest.SchemaRegistryConfig;
import io.confluent.kafka.schemaregistry.storage.SchemaKey;
import io.confluent.kafka.schemaregistry.storage.SchemaRegistry;
import io.confluent.kafka.schemaregistry.storage.SchemaValue;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import io.confluent.kafka.schemaregistry.utils.QualifiedSubject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class IncrementalIdGenerator implements IdGenerator {

  Logger log = LoggerFactory.getLogger(IncrementalIdGenerator.class);

  private final SchemaRegistry schemaRegistry;
  private final Map<String, Integer> maxIds = new ConcurrentHashMap<>();

  public IncrementalIdGenerator(SchemaRegistry schemaRegistry) {
    this.schemaRegistry = schemaRegistry;
  }

  @Override
  public int id(SchemaValue schema) throws IdGenerationException {
    String context = QualifiedSubject.contextFor(schemaRegistry.tenant(), schema.getSubject());
    return maxIds.compute(context, (k, v) -> v != null ? v + 1 : 1);
  }

  @Override
  public int getMaxId(SchemaValue schema) {
    String context = QualifiedSubject.contextFor(schemaRegistry.tenant(), schema.getSubject());
    return maxIds.computeIfAbsent(context, k -> 1);
  }

  @Override
  public void configure(SchemaRegistryConfig config) {

  }

  @Override
  public void init() throws IdGenerationException {

  }

  @Override
  public void schemaRegistered(SchemaKey schemaKey, SchemaValue schemaValue) {
    String context = QualifiedSubject.contextFor(schemaRegistry.tenant(), schemaKey.getSubject());
    maxIds.compute(context, (k, v) -> {
      int id = v != null ? v : 1;
      return Math.max(schemaValue.getId(), id);
    });
  }
}
