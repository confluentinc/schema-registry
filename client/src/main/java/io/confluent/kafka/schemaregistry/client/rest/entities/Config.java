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

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.confluent.kafka.schemaregistry.client.rest.entities.requests.ConfigUpdateRequest;
import io.swagger.v3.oas.annotations.media.Schema;
import java.util.Objects;

@JsonInclude(JsonInclude.Include.NON_EMPTY)
@JsonIgnoreProperties(ignoreUnknown = true)
@Schema(description = "Config")
public class Config {

  private String compatibilityLevel;
  private String compatibilityGroup;
  private Metadata initialMetadata;
  private Metadata finalMetadata;
  private RuleSet initialRuleSet;
  private RuleSet finalRuleSet;

  @JsonCreator
  public Config(@JsonProperty("compatibilityLevel") String compatibilityLevel,
                @JsonProperty("compatibilityGroup") String compatibilityGroup,
                @JsonProperty("initialMetadata") Metadata initialMetadata,
                @JsonProperty("finalMetadata") Metadata finalMetadata,
                @JsonProperty("initialRuleSet") RuleSet initialRuleSet,
                @JsonProperty("finalRuleSet") RuleSet finalRuleSet) {
    this.compatibilityLevel = compatibilityLevel;
    this.compatibilityGroup = compatibilityGroup;
    this.initialMetadata = initialMetadata;
    this.finalMetadata = finalMetadata;
    this.initialRuleSet = initialRuleSet;
    this.finalRuleSet = finalRuleSet;
  }

  public Config(@JsonProperty("compatibilityLevel") String compatibilityLevel) {
    this.compatibilityLevel = compatibilityLevel;
  }

  public Config() {
  }

  public Config(ConfigUpdateRequest request) {
    this.compatibilityLevel = request.getCompatibilityLevel();
    this.compatibilityGroup = request.getCompatibilityGroup();
    this.initialMetadata = request.getInitialMetadata();
    this.finalMetadata = request.getInitialMetadata();
    this.initialRuleSet = request.getInitialRuleSet();
    this.finalRuleSet = request.getFinalRuleSet();
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

  @JsonProperty("initialMetadata")
  public Metadata getInitialMetadata() {
    return this.initialMetadata;
  }

  @JsonProperty("initialMetadata")
  public void setInitialMetadata(Metadata initialMetadata) {
    this.initialMetadata = initialMetadata;
  }

  @JsonProperty("finalMetadata")
  public Metadata getFinalMetadata() {
    return this.finalMetadata;
  }

  @JsonProperty("finalMetadata")
  public void setFinalMetadata(Metadata finalMetadata) {
    this.finalMetadata = finalMetadata;
  }

  @JsonProperty("initialRuleSet")
  public RuleSet getInitialRuleSet() {
    return this.initialRuleSet;
  }

  @JsonProperty("initialRuleSet")
  public void setInitialRuleSet(RuleSet initialRuleSet) {
    this.initialRuleSet = initialRuleSet;
  }

  @JsonProperty("finalRuleSet")
  public RuleSet getFinalRuleSet() {
    return this.finalRuleSet;
  }

  @JsonProperty("finalRuleSet")
  public void setFinalRuleSet(RuleSet finalRuleSet) {
    this.finalRuleSet = finalRuleSet;
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
        && Objects.equals(initialMetadata, config.initialMetadata)
        && Objects.equals(finalMetadata, config.finalMetadata)
        && Objects.equals(initialRuleSet, config.initialRuleSet)
        && Objects.equals(finalRuleSet, config.finalRuleSet);
  }

  @Override
  public int hashCode() {
    return Objects.hash(compatibilityLevel, compatibilityGroup,
        initialMetadata, finalMetadata, initialRuleSet, finalRuleSet);
  }

  @Override
  public String toString() {
    return "Config{"
        + "compatibilityLevel='" + compatibilityLevel + '\''
        + ", compatibilityGroup='" + compatibilityGroup + '\''
        + ", initialMetadata=" + initialMetadata
        + ", finalMetadata=" + finalMetadata
        + ", initialRuleSet=" + initialRuleSet
        + ", finalRuleSet=" + finalRuleSet
        + '}';
  }
}
