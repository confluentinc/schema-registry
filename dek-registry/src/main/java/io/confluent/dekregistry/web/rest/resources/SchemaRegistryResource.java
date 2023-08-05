/*
 * Copyright 2023 Confluent Inc.
 */

package io.confluent.dekregistry.web.rest.resources;

import io.confluent.kafka.schemaregistry.storage.SchemaRegistry;

public abstract class SchemaRegistryResource {

  private final SchemaRegistry schemaRegistry;

  public SchemaRegistryResource(SchemaRegistry schemaRegistry) {
    this.schemaRegistry = schemaRegistry;
  }

  public SchemaRegistry getSchemaRegistry() {
    return schemaRegistry;
  }
}
