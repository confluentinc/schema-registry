package io.confluent.kafka.schemaregistry.rest.entities;

import com.fasterxml.jackson.annotation.JsonProperty;

import org.hibernate.validator.constraints.NotEmpty;

import javax.validation.constraints.Min;

public class Schema {

  @NotEmpty
  private final String topic;
  @NotEmpty
  private final SchemaSubType schemaSubType;
  @Min(1)
  private final Integer version;
  @NotEmpty
  private final String schema;
  private boolean deprecated = false;

  public Schema(@JsonProperty("topic") String topic,
                SchemaSubType schemaSubType,
                @JsonProperty("version") Integer version,
                @JsonProperty("schema") String schema,
                @JsonProperty("deprecated") boolean deprecated) {
    this.topic = topic;
    this.schemaSubType = schemaSubType;
    this.version = version;
    this.schema = schema;
    this.deprecated = deprecated;
  }

  @JsonProperty("name")
  public String getName() {
    return this.getTopic() + this.getSchemaSubtypeString();
  }

  @JsonProperty("topic")
  public String getTopic() {
    return topic;
  }

  public SchemaSubType getSchemaSubType() {
    return this.schemaSubType;
  }

  @JsonProperty("subtype")
  public String getSchemaSubtypeString() {
    return this.getSchemaSubType().toString().toLowerCase();
  }

  @JsonProperty("schema")
  public String getSchema() {
    return this.schema;
  }

  @JsonProperty("version")
  public Integer getVersion() {
    return this.version;
  }

  @JsonProperty("deprecated")
  public boolean getDeprecated() {
    return this.deprecated;
  }

  @JsonProperty("deprecated")
  public void setDeprecated(boolean deprecated) {
    this.deprecated = deprecated;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    Schema that = (Schema) o;

    if (!this.getName().equals(that.getName())) {
      return false;
    }
    if (!this.getTopic().equals(that.getTopic())) {
      return false;
    }
    if (!this.schemaSubType.equals(that.schemaSubType)) {
      return false;
    }
    if (!schema.equals(that.schema)) {
      return false;
    }
    if (this.version != that.getVersion()) {
      return false;
    }
    if (this.deprecated && !that.deprecated) {
      return false;
    }

    return true;
  }

  @Override
  public int hashCode() {
    int result = this.getName().hashCode();
    result = 31 * result + topic.hashCode();
    result = 31 * result + schemaSubType.hashCode();
    result = 31 * result + schema.hashCode();
    result = 31 * result + version;
    result = 31 * result + new Boolean(deprecated).hashCode();
    return result;
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append("{name=" + this.getName() + ",");
    sb.append("{topic=" + this.topic + ",");
    sb.append("{schemaSubType=" + this.getSchemaSubtypeString() + ",");
    sb.append("schema=" + this.schema + ",");
    sb.append("version=" + this.version + ",");
    sb.append("deprecated=" + this.deprecated + ",");
    return sb.toString();
  }

  public static String name(String topic, SchemaSubType schemaSubType) {
    return topic + schemaSubType.toString().toLowerCase();
  }
}
