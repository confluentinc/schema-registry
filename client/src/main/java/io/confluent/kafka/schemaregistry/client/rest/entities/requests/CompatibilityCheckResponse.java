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

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.io.IOException;

import io.confluent.kafka.schemaregistry.utils.JacksonMapper;
import io.swagger.v3.oas.annotations.media.Schema;

import java.util.List;
import java.util.Objects;

@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonIgnoreProperties(ignoreUnknown = true)
@Schema(description = "Compatibility check response")
public class CompatibilityCheckResponse {

  private boolean isCompatible;
  private List<String> messages = null;

  public static CompatibilityCheckResponse fromJson(String json) throws IOException {
    return JacksonMapper.INSTANCE.readValue(json, CompatibilityCheckResponse.class);
  }

  @Schema(description = "Whether the compared schemas are compatible")
  @JsonProperty("is_compatible")
  public boolean getIsCompatible() {
    return isCompatible;
  }

  @JsonProperty("is_compatible")
  public void setIsCompatible(boolean isCompatible) {
    this.isCompatible = isCompatible;
  }

  public String toJson() throws IOException {
    return JacksonMapper.INSTANCE.writeValueAsString(this);
  }

  @Schema(description = "Error messages", example = "[]")
  @JsonProperty("messages")
  public List<String> getMessages() {
    return messages;
  }

  @JsonProperty("messages")
  public void setMessages(List<String> messages) {
    this.messages = messages;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    CompatibilityCheckResponse that = (CompatibilityCheckResponse) o;
    return isCompatible == that.isCompatible;
  }

  @Override
  public int hashCode() {
    return Objects.hash(isCompatible);
  }
}
