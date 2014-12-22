package io.confluent.kafka.schemaregistry.rest.entities;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;

import org.hibernate.validator.constraints.NotEmpty;

import javax.validation.constraints.Min;

public class Schema {

  @NotEmpty
  private final String topic;
  @NotEmpty
  private final String schemaSubType;
  @Min(1)
  private final Integer version;
  @NotEmpty
  private final String schema;
  private boolean deprecated = false;

  public Schema(@JsonProperty("topic") String topic,
                @JsonProperty("subtype") String schemaSubType,
                @JsonProperty("version") Integer version,
                @JsonProperty("schema") String schema,
                @JsonProperty("deprecated") boolean deprecated) {
    this.topic = topic;
    this.schemaSubType = schemaSubType;
    this.version = version;
    this.schema = schema;
    this.deprecated = deprecated;
  }

  // TODO - should this really be a json property, or is it something we just want to use internally?
  @JsonIgnore
  public String getName() {
    return Schema.name(this.getTopic(), this.getSchemaSubType());
  }

  @JsonProperty("topic")
  public String getTopic() {
    return topic;
  }

  @JsonProperty("subtype")
  public String getSchemaSubType() {
    return this.schemaSubType;
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
    sb.append("{schemaSubType=" + this.schemaSubType + ",");
    sb.append("schema=" + this.schema + ",");
    sb.append("version=" + this.version + ",");
    sb.append("deprecated=" + this.deprecated + ",");
    return sb.toString();
  }

  /**
   * An identifier for the collection of schemas having different versions but
   * the same topic and schemaSubType.
   */
  public static String name(String topic, String schemaSubType) {
    return topic + "/" + schemaSubType;
  }
}
