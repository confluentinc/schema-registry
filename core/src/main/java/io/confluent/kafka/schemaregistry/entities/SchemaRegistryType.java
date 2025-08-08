package io.confluent.kafka.schemaregistry.entities;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonInclude.Include;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.confluent.kafka.schemaregistry.storage.SchemaRegistry;

@JsonInclude(Include.NON_NULL)
@JsonIgnoreProperties(ignoreUnknown = true)
public class SchemaRegistryType {
  private static final String DEFAULT_TYPE = "opensource";

  private final String type;
  private final String subtype;

  public SchemaRegistryType() {
    this.type = DEFAULT_TYPE;
    this.subtype = null;
  }

  public SchemaRegistryType(String type, String subtype) {
    this.type = type;
    this.subtype = subtype;
  }

  @JsonProperty("type")
  public String getType() {
    return type;
  }

  @JsonProperty("subType")
  public String getSubtype() {
    return subtype;
  }
}
