package io.confluent.kafka.schemaregistry.zookeeper;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.IOException;

/**
 *  The identity of a schema registry instance. Used for Zookeeper registration.
 */
public class SchemaRegistryIdentity {
  public static int CURRENT_VERSION = 1;

  private Integer version;
  private String host;
  private Integer port;

  public SchemaRegistryIdentity(@JsonProperty("version") Integer version,
                                @JsonProperty("host") String host,
                                @JsonProperty("port") Integer port) {
    this.version = version;
    this.host = host;
    this.port = port;
  }

  @JsonProperty("version")
  public Integer getVersion() {
    return this.version;
  }

  @JsonProperty("version")
  public void setVersion(Integer version) {
    this.version = version;
  }

  @JsonProperty("host")
  public String getHost() {
    return this.host;
  }

  @JsonProperty("host")
  public void setHost(String host) {
    this.host = host;
  }

  @JsonProperty("port")
  public Integer getPort() {
    return this.port;
  }

  @JsonProperty("port")
  public void setPort(Integer port) {
    this.port = port;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    SchemaRegistryIdentity that = (SchemaRegistryIdentity) o;

    if (!this.version.equals(that.version)) {
      return false;
    }
    if (!this.host.equals(that.host)) {
      return false;
    }
    if (!this.port.equals(that.port)) {
      return false;
    }

    return true;
  }

  @Override
  public int hashCode() {
    int result = port;
    result = 31 * result + host.hashCode();
    result = 31 * result + version;
    return result;
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append("version=" + this.version + ",");
    sb.append("host=" + this.host + ",");
    sb.append("port=" + this.port);
    return sb.toString();
  }

  public String toJson() throws IOException {
    return new ObjectMapper().writeValueAsString(this);
  }

  public static SchemaRegistryIdentity fromJson(String json) throws IOException {
    return new ObjectMapper().readValue(json, SchemaRegistryIdentity.class);
  }
}
