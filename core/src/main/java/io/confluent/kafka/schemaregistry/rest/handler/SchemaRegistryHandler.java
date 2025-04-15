package io.confluent.kafka.schemaregistry.rest.handler;

import io.confluent.kafka.schemaregistry.exceptions.SchemaRegistryException;
import io.confluent.kafka.schemaregistry.rest.SchemaRegistryConfig;
import io.confluent.kafka.schemaregistry.storage.SchemaRegistry;
import org.eclipse.jetty.server.Handler;

public abstract class SchemaRegistryHandler extends Handler.Wrapper {

  public abstract void init(SchemaRegistryConfig schemaRegistryConfig,
                            SchemaRegistry schemaRegistry);

  boolean initialized() {
    return true;
  }
}
