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
import com.fasterxml.jackson.annotation.JsonIgnore;
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

  private String alias;
  private Boolean normalize;

  private Boolean validateFields;
  private String compatibilityLevel;
  private String compatibilityGroup;
  private Metadata defaultMetadata;
  private Metadata overrideMetadata;
  private RuleSet defaultRuleSet;
  private RuleSet overrideRuleSet;

  @JsonCreator
  public Config(@JsonProperty("alias") String alias,
                @JsonProperty("normalize") Boolean normalize,
                @JsonProperty("validateFields") Boolean validateFields,
                @JsonProperty("compatibilityLevel") String compatibilityLevel,
                @JsonProperty("compatibilityGroup") String compatibilityGroup,
                @JsonProperty("defaultMetadata") Metadata defaultMetadata,
                @JsonProperty("overrideMetadata") Metadata overrideMetadata,
                @JsonProperty("defaultRuleSet") RuleSet defaultRuleSet,
                @JsonProperty("overrideRuleSet") RuleSet overrideRuleSet) {
    this.alias = alias;
    this.normalize = normalize;
    this.validateFields = validateFields;
    this.compatibilityLevel = compatibilityLevel;
    this.compatibilityGroup = compatibilityGroup;
    this.defaultMetadata = defaultMetadata;
    this.overrideMetadata = overrideMetadata;
    this.defaultRuleSet = defaultRuleSet;
    this.overrideRuleSet = overrideRuleSet;
  }

  public Config(@JsonProperty("compatibilityLevel") String compatibilityLevel) {
    this.compatibilityLevel = compatibilityLevel;
  }

  public Config() {
  }

  public Config(ConfigUpdateRequest request) {
    this.alias = request.getAlias();
    this.normalize = request.isNormalize();
    this.validateFields = request.isValidateFields();
    this.compatibilityLevel = request.getCompatibilityLevel();
    this.compatibilityGroup = request.getCompatibilityGroup();
    this.defaultMetadata = request.getDefaultMetadata();
    this.overrideMetadata = request.getOverrideMetadata();
    this.defaultRuleSet = request.getDefaultRuleSet();
    this.overrideRuleSet = request.getOverrideRuleSet();
  }

  @JsonProperty("alias")
  public String getAlias() {
    return alias;
  }

  @JsonProperty("alias")
  public void setAlias(String alias) {
    this.alias = alias;
  }

  @JsonProperty("normalize")
  public Boolean isNormalize() {
    return normalize;
  }

  @JsonProperty("normalize")
  public void setNormalize(Boolean normalize) {
    this.normalize = normalize;
  }

  @JsonProperty("validateFields")
  public Boolean isValidateFields() {
    return validateFields;
  }

  @JsonProperty("validateFields")
  public void setValidateFields(Boolean validateFieldNames) {
    this.validateFields = validateFieldNames;
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

  @JsonIgnore
  public boolean hasDefaultsOrOverrides() {
    return defaultMetadata != null
        || overrideMetadata != null
        || defaultRuleSet != null
        || overrideRuleSet != null;
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
    return Objects.equals(alias, config.alias)
        && Objects.equals(normalize, config.normalize)
        && Objects.equals(validateFields, config.validateFields)
        && Objects.equals(compatibilityLevel, config.compatibilityLevel)
        && Objects.equals(compatibilityGroup, config.compatibilityGroup)
        && Objects.equals(defaultMetadata, config.defaultMetadata)
        && Objects.equals(overrideMetadata, config.overrideMetadata)
        && Objects.equals(defaultRuleSet, config.defaultRuleSet)
        && Objects.equals(overrideRuleSet, config.overrideRuleSet);
  }

  @Override
  public int hashCode() {
    return Objects.hash(alias, normalize, validateFields, compatibilityLevel, compatibilityGroup,
        defaultMetadata, overrideMetadata, defaultRuleSet, overrideRuleSet);
  }

  @Override
  public String toString() {
    return "Config{"
        + "alias='" + alias + '\''
        + ", normalize=" + normalize
        + ", validateFields=" + validateFields
        + ", compatibilityLevel='" + compatibilityLevel + '\''
        + ", compatibilityGroup='" + compatibilityGroup + '\''
        + ", defaultMetadata=" + defaultMetadata
        + ", overrideMetadata=" + overrideMetadata
        + ", defaultRuleSet=" + defaultRuleSet
        + ", overrideRuleSet=" + overrideRuleSet
        + '}';
  }
}
