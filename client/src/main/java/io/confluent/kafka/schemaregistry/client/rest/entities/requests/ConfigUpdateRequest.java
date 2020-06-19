/*
 * Copyright 2018 Confluent Inc.
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

package io.confluent.kafka.schemaregistry.client.rest.entities.requests;

import com.fasterxml.jackson.annotation.JsonProperty;

import io.swagger.annotations.ApiModelProperty;

import java.io.IOException;

import io.confluent.kafka.schemaregistry.utils.JacksonMapper;
import java.util.Objects;

public class ConfigUpdateRequest {

  private String compatibilityLevel;

  public static ConfigUpdateRequest fromJson(String json) throws IOException {
    return JacksonMapper.INSTANCE.readValue(json, ConfigUpdateRequest.class);
  }

  @ApiModelProperty(value = "Compatability Level",
      allowableValues = "BACKWARD, BACKWARD_TRANSITIVE, FORWARD, FORWARD_TRANSITIVE, FULL, "
          + "FULL_TRANSITIVE, NONE")
  @JsonProperty("compatibility")
  public String getCompatibilityLevel() {
    return this.compatibilityLevel;
  }

  @JsonProperty("compatibility")
  public void setCompatibilityLevel(String compatibilityLevel) {
    this.compatibilityLevel = compatibilityLevel;
  }

  public String toJson() throws IOException {
    return JacksonMapper.INSTANCE.writeValueAsString(this);
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    ConfigUpdateRequest that = (ConfigUpdateRequest) o;
    return Objects.equals(compatibilityLevel, that.compatibilityLevel);
  }

  @Override
  public int hashCode() {
    return Objects.hash(compatibilityLevel);
  }
}
