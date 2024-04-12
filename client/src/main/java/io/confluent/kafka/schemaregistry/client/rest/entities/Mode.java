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

package io.confluent.kafka.schemaregistry.client.rest.entities;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.io.IOException;

import io.confluent.kafka.schemaregistry.utils.JacksonMapper;
import io.swagger.v3.oas.annotations.media.Schema;

import java.util.Objects;

@JsonInclude(JsonInclude.Include.NON_EMPTY)
@JsonIgnoreProperties(ignoreUnknown = true)
@Schema(description = Mode.MODE_DESC)
public class Mode {

  public static final String MODE_DESC = "Schema Registry operating mode";

  @Schema(description = MODE_DESC,
          allowableValues = "READWRITE, READONLY, READONLY_OVERRIDE, IMPORT")
  private String mode;

  public static Mode fromJson(String json) throws IOException {
    return JacksonMapper.INSTANCE.readValue(json, Mode.class);
  }

  public Mode(@JsonProperty("mode") String mode) {
    this.mode = mode;
  }

  @JsonProperty("mode")
  public String getMode() {
    return this.mode;
  }

  @JsonProperty("mode")
  public void setMode(String mode) {
    this.mode = mode;
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
    Mode that = (Mode) o;
    return Objects.equals(mode, that.mode);
  }

  @Override
  public int hashCode() {
    return Objects.hash(mode);
  }
}
