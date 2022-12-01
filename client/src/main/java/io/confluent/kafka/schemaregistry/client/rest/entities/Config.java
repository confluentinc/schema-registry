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
import java.util.Objects;

@JsonInclude(JsonInclude.Include.NON_EMPTY)
@JsonIgnoreProperties(ignoreUnknown = true)
@Schema(description = "Config")
public class Config {

  private String compatibilityLevel;
  private String compatibilityGroup;
  private Metadata metadataOverride;
  private RuleSet ruleSetOverride;

  public Config(@JsonProperty("compatibilityLevel") String compatibilityLevel) {
    this.compatibilityLevel = compatibilityLevel;
  }

  public Config() {
    compatibilityLevel = null;
  }

  @Schema(description = "Compatibility Level",
      allowableValues = {"BACKWARD", "BACKWARD_TRANSITIVE", "FORWARD", "FORWARD_TRANSITIVE",
        "FULL", "FULL_TRANSITIVE", "NONE"},
      example = "FULL_TRANSITIVE")
  @JsonProperty("compatibilityLevel")
  public String getCompatibilityLevel() {
    return compatibilityLevel;
  }

  @JsonProperty("compatibilityLevel")
  public void setCompatibilityLevel(String compatibilityLevel) {
    this.compatibilityLevel = compatibilityLevel;
  }

  @JsonProperty("compatibilityGroup")
  public String getCompatibilityGroup() {
    return this.compatibilityGroup;
  }

  @JsonProperty("compatibilityGroup")
  public void setCompatibilityGroup(String compatibilityGroup) {
    this.compatibilityGroup = compatibilityGroup;
  }

  @JsonProperty("metadataOverride")
  public Metadata getMetadataOverride() {
    return this.metadataOverride;
  }

  @JsonProperty("metadataOverride")
  public void setMetadataOverride(Metadata metadataOverride) {
    this.metadataOverride = metadataOverride;
  }

  @JsonProperty("ruleSetOverride")
  public RuleSet getRuleSetOverride() {
    return this.ruleSetOverride;
  }

  @JsonProperty("ruleSetOverride")
  public void setRuleSetOverride(RuleSet ruleSetOverride) {
    this.ruleSetOverride = ruleSetOverride;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    Config config = (Config) o;
    return Objects.equals(compatibilityLevel, config.compatibilityLevel)
        && Objects.equals(compatibilityGroup, config.compatibilityGroup)
        && Objects.equals(metadataOverride, config.metadataOverride)
        && Objects.equals(ruleSetOverride, config.ruleSetOverride);
  }

  @Override
  public int hashCode() {
    return Objects.hash(compatibilityLevel, compatibilityGroup, metadataOverride, ruleSetOverride);
  }

  @Override
  public String toString() {
    return "Config{" +
        "compatibilityLevel='" + compatibilityLevel + '\'' +
        ", compatibilityGroup='" + compatibilityGroup + '\'' +
        ", metadataOverride=" + metadataOverride +
        ", ruleSetOverride=" + ruleSetOverride +
        '}';
  }
}
