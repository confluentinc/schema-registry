package io.confluent.kafka.schemaregistry.rest.entities.requests;

import com.fasterxml.jackson.annotation.JsonProperty;

import org.hibernate.validator.constraints.NotEmpty;

public class RegisterSchemaRequest {

  @NotEmpty
  private String schema;

  @JsonProperty("schema")
  public String getSchema() {
    return this.schema;
  }

  @JsonProperty("schema")
  public void setSchema(String schema) {
    this.schema = schema;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    if (!super.equals(o)) {
      return false;
    }

    RegisterSchemaRequest that = (RegisterSchemaRequest) o;

    if (schema != null ? !schema.equals(that.schema) : that.schema != null) {
      return false;
    }

    return true;
  }

  @Override
  public int hashCode() {
    int result = super.hashCode();
    result = 31 * result + (schema != null ? schema.hashCode() : 0);
    return result;
  }
}
