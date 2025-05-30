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

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonInclude.Include;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.io.IOException;

import io.confluent.kafka.schemaregistry.client.rest.entities.Mode;
import io.confluent.kafka.schemaregistry.utils.JacksonMapper;
import io.swagger.v3.oas.annotations.media.Schema;

import java.util.Objects;
import java.util.Optional;

@JsonInclude(Include.NON_NULL)
@JsonIgnoreProperties(ignoreUnknown = true)
@Schema(description = "Mode update request")
public class ModeUpdateRequest {

  private Optional<String> mode;

  public static ModeUpdateRequest fromJson(String json) throws IOException {
    return JacksonMapper.INSTANCE.readValue(json, ModeUpdateRequest.class);
  }

  public ModeUpdateRequest() {
  }

  public ModeUpdateRequest(Optional<String> mode) {
    setMode(mode);
  }

  public ModeUpdateRequest(String mode) {
    setMode(mode);
  }

  @Schema(description = Mode.MODE_DESC,
          allowableValues = {"READWRITE", "READONLY", "READONLY_OVERRIDE", "IMPORT"},
          example = "READWRITE")
  @JsonProperty("mode")
  public Optional<String> getOptionalMode() {
    return this.mode;
  }

  @JsonIgnore
  public String getMode() {
    return mode != null ? mode.orElse(null) : null;
  }

  @JsonProperty("mode")
  public void setMode(Optional<String> mode) {
    this.mode = mode;
  }

  @JsonIgnore
  public void setMode(String mode) {
    this.mode = mode != null ? Optional.of(mode) : null;
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
    ModeUpdateRequest that = (ModeUpdateRequest) o;
    return Objects.equals(mode, that.mode);
  }

  @Override
  public int hashCode() {
    return Objects.hash(mode);
  }
}
