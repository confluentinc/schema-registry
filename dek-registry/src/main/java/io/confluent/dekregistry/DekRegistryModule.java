/*
 * Copyright 2023 Confluent Inc.
 */

package io.confluent.dekregistry;

import com.google.inject.AbstractModule;
import io.confluent.kafka.schemaregistry.storage.SchemaRegistry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DekRegistryModule extends AbstractModule {
  private static final Logger LOG = LoggerFactory.getLogger(DekRegistryModule.class);

  private final SchemaRegistry schemaRegistry;

  public DekRegistryModule(SchemaRegistry schemaRegistry) {
    this.schemaRegistry = schemaRegistry;
  }

  @Override
  protected void configure() {
    LOG.info("Configuring dek registry module");

    bind(SchemaRegistry.class).toInstance(schemaRegistry);

    LOG.info("Done configuring dek registry module");
  }
}
