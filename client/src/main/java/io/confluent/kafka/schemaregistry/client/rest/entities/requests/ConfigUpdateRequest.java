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
import io.confluent.kafka.schemaregistry.client.rest.entities.Config;
import io.confluent.kafka.schemaregistry.client.rest.entities.Metadata;
import io.confluent.kafka.schemaregistry.client.rest.entities.RuleSet;
import io.confluent.kafka.schemaregistry.utils.JacksonMapper;
import io.swagger.v3.oas.annotations.media.Schema;

import java.io.IOException;
import java.util.Objects;

@JsonInclude(JsonInclude.Include.NON_EMPTY)
@JsonIgnoreProperties(ignoreUnknown = true)
@Schema(description = "Config update request")
public class ConfigUpdateRequest {

  private String compatibilityLevel;
  private String compatibilityGroup;
  private Metadata initialMetadata;
  private Metadata finalMetadata;
  private RuleSet initialRuleSet;
  private RuleSet finalRuleSet;

  public ConfigUpdateRequest() {
  }

  public ConfigUpdateRequest(Config config) {
    this.compatibilityLevel = config.getCompatibilityLevel();
    this.compatibilityGroup = config.getCompatibilityGroup();
    this.initialMetadata = config.getInitialMetadata();
    this.finalMetadata = config.getFinalMetadata();
    this.initialRuleSet = config.getInitialRuleSet();
    this.finalRuleSet = config.getFinalRuleSet();
  }

  public static ConfigUpdateRequest fromJson(String json) throws IOException {
    return JacksonMapper.INSTANCE.readValue(json, ConfigUpdateRequest.class);
  }

  @Schema(description = "Compatibility Level",
      allowableValues = {"BACKWARD", "BACKWARD_TRANSITIVE", "FORWARD", "FORWARD_TRANSITIVE", "FULL",
        "FULL_TRANSITIVE", "NONE"},
      example = "FULL_TRANSITIVE")
  @JsonProperty("compatibility")
  public String getCompatibilityLevel() {
    return this.compatibilityLevel;
  }

  @JsonProperty("compatibility")
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
    return Objects.equals(compatibilityLevel, that.compatibilityLevel)
        && Objects.equals(compatibilityGroup, that.compatibilityGroup)
        && Objects.equals(initialMetadata, that.initialMetadata)
        && Objects.equals(finalMetadata, that.finalMetadata)
        && Objects.equals(initialRuleSet, that.initialRuleSet)
        && Objects.equals(finalRuleSet, that.finalRuleSet);
  }

  @Override
  public int hashCode() {
    return Objects.hash(compatibilityLevel, compatibilityGroup,
        initialMetadata, finalMetadata, initialRuleSet, finalRuleSet);
  }
}
