/**
 * Copyright 2014 Confluent Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.confluent.kafka.schemaregistry.zookeeper;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.IOException;

/**
 * The identity of a schema registry instance. The master will store the json representation of its
 * identity in Zookeeper.
 */
public class SchemaRegistryIdentity {

  public static int CURRENT_VERSION = 1;

  private Integer version;
  private String host;
  private Integer port;
  private Boolean masterEligibility;

  public SchemaRegistryIdentity(@JsonProperty("host") String host,
                                @JsonProperty("port") Integer port,
                                @JsonProperty("master_eligibility") Boolean masterEligibility) {
    this.version = CURRENT_VERSION;
    this.host = host;
    this.port = port;
    this.masterEligibility = masterEligibility;
  }

  public static SchemaRegistryIdentity fromJson(String json) throws IOException {
    return new ObjectMapper().readValue(json, SchemaRegistryIdentity.class);
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

  @JsonProperty("master_eligibility")
  public boolean getMasterEligibility() {
    return this.masterEligibility;
  }

  @JsonProperty("master_eligibility")
  public void setMasterEligibility(Boolean eligibility) {
    this.masterEligibility = eligibility;
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
    if (!this.masterEligibility.equals(that.masterEligibility)) {
      return false;
    }

    return true;
  }

  @Override
  public int hashCode() {
    int result = port;
    result = 31 * result + host.hashCode();
    result = 31 * result + version;
    result = 31 * result + masterEligibility.hashCode();
    return result;
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append("version=" + this.version + ",");
    sb.append("host=" + this.host + ",");
    sb.append("port=" + this.port + ",");
    sb.append("masterEligibility=" + this.masterEligibility);
    return sb.toString();
  }

  public String toJson() throws IOException {
    return new ObjectMapper().writeValueAsString(this);
  }
}
