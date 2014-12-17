package io.confluent.kafka.schemaregistry.rest.entities.requests;

import com.fasterxml.jackson.annotation.JsonProperty;

import org.hibernate.validator.constraints.NotEmpty;

public class RegisterSchemaResponse {

  @NotEmpty
  private int version;

  @JsonProperty("version")
  public int getVersion() {
    return version;
  }

  @JsonProperty("version")
  public void setVersion(int version) {
    this.version = version;
  }

}
