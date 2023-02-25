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
  private Metadata defaultMetadata;
  private Metadata overrideMetadata;
  private RuleSet defaultRuleSet;
  private RuleSet overrideRuleSet;

  public ConfigUpdateRequest() {
  }

  public ConfigUpdateRequest(Config config) {
    this.compatibilityLevel = config.getCompatibilityLevel();
    this.compatibilityGroup = config.getCompatibilityGroup();
    this.defaultMetadata = config.getDefaultMetadata();
    this.overrideMetadata = config.getOverrideMetadata();
    this.defaultRuleSet = config.getDefaultRuleSet();
    this.overrideRuleSet = config.getOverrideRuleSet();
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

  @JsonProperty("defaultMetadata")
  public Metadata getDefaultMetadata() {
    return this.defaultMetadata;
  }

  @JsonProperty("defaultMetadata")
  public void setDefaultMetadata(Metadata defaultMetadata) {
    this.defaultMetadata = defaultMetadata;
  }

  @JsonProperty("overrideMetadata")
  public Metadata getOverrideMetadata() {
    return this.overrideMetadata;
  }

  @JsonProperty("overrideMetadata")
  public void setOverrideMetadata(Metadata overrideMetadata) {
    this.overrideMetadata = overrideMetadata;
  }

  @JsonProperty("defaultRuleSet")
  public RuleSet getDefaultRuleSet() {
    return this.defaultRuleSet;
  }

  @JsonProperty("defaultRuleSet")
  public void setDefaultRuleSet(RuleSet defaultRuleSet) {
    this.defaultRuleSet = defaultRuleSet;
  }

  @JsonProperty("overrideRuleSet")
  public RuleSet getOverrideRuleSet() {
    return this.overrideRuleSet;
  }

  @JsonProperty("overrideRuleSet")
  public void setOverrideRuleSet(RuleSet overrideRuleSet) {
    this.overrideRuleSet = overrideRuleSet;
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
        && Objects.equals(defaultMetadata, that.defaultMetadata)
        && Objects.equals(overrideMetadata, that.overrideMetadata)
        && Objects.equals(defaultRuleSet, that.defaultRuleSet)
        && Objects.equals(overrideRuleSet, that.overrideRuleSet);
  }

  @Override
  public int hashCode() {
    return Objects.hash(compatibilityLevel, compatibilityGroup,
        defaultMetadata, overrideMetadata, defaultRuleSet, overrideRuleSet);
  }
}
