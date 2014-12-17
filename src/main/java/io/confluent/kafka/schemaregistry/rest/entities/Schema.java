package io.confluent.kafka.schemaregistry.rest.entities;

import com.fasterxml.jackson.annotation.JsonProperty;

import org.hibernate.validator.constraints.NotEmpty;

import javax.validation.constraints.Min;

public class Schema {

  @NotEmpty
  private String name;
  @Min(1)
  private Integer version;
  @NotEmpty
  private String schema;
  private boolean compatible = true;
  private boolean deprecated = false;
  private boolean latest = true;

  public Schema(@JsonProperty("name") String name,
                @JsonProperty("version") Integer version,
                @JsonProperty("schema") String schema,
                @JsonProperty("compatible") boolean compatible,
                @JsonProperty("deprecated") boolean deprecated,
                @JsonProperty("latest") boolean latest) {
    this.name = name;
    this.version = version;
    this.schema = schema;
    this.compatible = compatible;
    this.deprecated = deprecated;
    this.latest = latest;
  }

  @JsonProperty("name")
  public String getName() {
    return name;
  }

  @JsonProperty("name")
  public void setName(String name) {
    this.name = name;
  }

  @JsonProperty("schema")
  public String getSchema() {
    return this.schema;
  }

  @JsonProperty("schema")
  public void setSchema(String schema) {
    this.schema = schema;
  }

  @JsonProperty("version")
  public Integer getVersion() {
    return this.version;
  }

  @JsonProperty("version")
  public void setVersion(Integer version) {
    this.version = version;
  }

  @JsonProperty("compatible")
  public boolean getCompatible() {
    return this.compatible;
  }

  @JsonProperty("compatible")
  public void setCompatible(boolean compatible) {
    this.compatible = compatible;
  }

  @JsonProperty("deprecated")
  public boolean getDeprecated() {
    return this.deprecated;
  }

  @JsonProperty("deprecated")
  public void setDeprecated(boolean deprecated) {
    this.deprecated = deprecated;
  }

  @JsonProperty("latest")
  public boolean getLatest() {
    return this.latest;
  }

  @JsonProperty("latest")
  public void setLatest(boolean latest) {
    this.latest = latest;
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

    if (!name.equals(that.getName())) {
      return false;
    }
    if (!schema.equals(that.schema)) {
      return false;
    }
    if (this.version != that.getVersion()) {
      return false;
    }
    if (this.compatible && !that.compatible) {
      return false;
    }
    if (this.deprecated && !that.deprecated) {
      return false;
    }
    if (this.latest && !that.latest) {
      return false;
    }

    return true;
  }

  @Override
  public int hashCode() {
    int result = name.hashCode();
    result = 31 * result + schema.hashCode();
    result = 31 * result + version;
    result = 31 * result + new Boolean(compatible).hashCode();
    result = 31 * result + new Boolean(deprecated).hashCode();
    result = 31 * result + new Boolean(latest).hashCode();
    return result;
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append("{name=" + this.name + ",");
    sb.append("schema=" + this.schema + ",");
    sb.append("version=" + this.version + ",");
    sb.append("compatible=" + this.compatible + ",");
    sb.append("deprecated=" + this.deprecated + ",");
    sb.append("latest=" + this.latest + "}");
    return sb.toString();
  }


}
