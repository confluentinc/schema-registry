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
import io.swagger.v3.oas.annotations.media.Schema;

@JsonInclude(JsonInclude.Include.NON_EMPTY)
@JsonIgnoreProperties(ignoreUnknown = true)
@Schema(description = "Config")
public class Config {

  private String compatibilityLevel;

  public Config(@JsonProperty("compatibilityLevel") String compatibilityLevel) {
    this.compatibilityLevel = compatibilityLevel;
  }

  public Config() {
    compatibilityLevel = null;
  }

  @Schema(description = "Compatibility Level",
      allowableValues = "BACKWARD, BACKWARD_TRANSITIVE, FORWARD, FORWARD_TRANSITIVE, FULL, "
          + "FULL_TRANSITIVE, NONE")
  @JsonProperty("compatibilityLevel")
  public String getCompatibilityLevel() {
    return compatibilityLevel;
  }

  @JsonProperty("compatibilityLevel")
  public void setCompatibilityLevel(String compatibilityLevel) {
    this.compatibilityLevel = compatibilityLevel;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    Config that = (Config) o;

    return this.compatibilityLevel.equals(that.compatibilityLevel);
  }

  @Override
  public int hashCode() {
    return 31 * compatibilityLevel.hashCode();
  }

  @Override
  public String toString() {
    return "{compatibilityLevel=" + this.compatibilityLevel + "}";
  }
}
