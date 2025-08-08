package io.confluent.kafka.schemaregistry.utils;

import io.confluent.kafka.schemaregistry.entities.SchemaRegistryType;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Props {
  public static final String PROPERTY_SCHEMA_REGISTRY_TYPE = "schema.registry.metadata.type";
  private static final Logger log = LoggerFactory.getLogger(Props.class);

  public static SchemaRegistryType GetSchemaRegistryType (Map<String, Object> props) {
    Object srType = props.getOrDefault(PROPERTY_SCHEMA_REGISTRY_TYPE, new SchemaRegistryType());
    if (srType instanceof SchemaRegistryType) {
      return (SchemaRegistryType) srType;
    } else {
      throw new IllegalArgumentException("Invalid schema registry type: " + srType);
    }
  }
}
