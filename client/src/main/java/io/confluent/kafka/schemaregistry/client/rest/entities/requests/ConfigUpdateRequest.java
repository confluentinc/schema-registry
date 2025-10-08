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
import io.confluent.kafka.schemaregistry.client.rest.entities.Config;
import io.confluent.kafka.schemaregistry.client.rest.entities.Metadata;
import io.confluent.kafka.schemaregistry.client.rest.entities.RuleSet;
import io.confluent.kafka.schemaregistry.utils.JacksonMapper;
import io.swagger.v3.oas.annotations.media.Schema;

import java.io.IOException;
import java.util.Objects;
import java.util.Optional;

@JsonInclude(Include.NON_NULL)
@JsonIgnoreProperties(ignoreUnknown = true)
@Schema(description = "Config update request")
public class ConfigUpdateRequest {

  private Optional<String> alias;
  private Optional<Boolean> normalize;
  private Optional<Boolean> validateFields;
  private Optional<Boolean> validateRules;
  private Optional<String> compatibilityLevel;
  private Optional<String> compatibilityPolicy;
  private Optional<String> compatibilityGroup;
  private Optional<Metadata> defaultMetadata;
  private Optional<Metadata> overrideMetadata;
  private Optional<RuleSet> defaultRuleSet;
  private Optional<RuleSet> overrideRuleSet;

  public ConfigUpdateRequest() {
  }

  public ConfigUpdateRequest(Config config) {
    setAlias(config.getAlias());
    setNormalize(config.isNormalize());
    setValidateFields(config.isValidateFields());
    setValidateRules(config.isValidateRules());
    setCompatibilityLevel(config.getCompatibilityLevel());
    setCompatibilityPolicy(config.getCompatibilityPolicy());
    setCompatibilityGroup(config.getCompatibilityGroup());
    setDefaultMetadata(config.getDefaultMetadata());
    setOverrideMetadata(config.getOverrideMetadata());
    setDefaultRuleSet(config.getDefaultRuleSet());
    setOverrideRuleSet(config.getOverrideRuleSet());
  }

  public static ConfigUpdateRequest fromJson(String json) throws IOException {
    return JacksonMapper.INSTANCE.readValue(json, ConfigUpdateRequest.class);
  }

  @JsonProperty("alias")
  public Optional<String> getOptionalAlias() {
    return this.alias;
  }

  @JsonIgnore
  public String getAlias()  {
    return alias != null ? alias.orElse(null) : null;
  }

  @JsonProperty("alias")
  public void setAlias(Optional<String> alias) {
    this.alias = alias;
  }

  @JsonIgnore
  public void setAlias(String alias) {
    this.alias = alias != null ? Optional.of(alias) : null;
  }

  @JsonProperty("normalize")
  public Optional<Boolean> isOptionalNormalize() {
    return this.normalize;
  }

  @JsonIgnore
  public Boolean isNormalize() {
    return normalize != null ? normalize.orElse(null) : null;
  }

  @JsonProperty("normalize")
  public void setNormalize(Optional<Boolean> normalize) {
    this.normalize = normalize;
  }

  @JsonIgnore
  public void setNormalize(Boolean normalize) {
    this.normalize = normalize != null ? Optional.of(normalize) : null;
  }

  @JsonProperty("validateFields")
  public Optional<Boolean> isOptionalValidateFields() {
    return validateFields;
  }

  @JsonIgnore
  public Boolean isValidateFields() {
    return validateFields != null ? validateFields.orElse(null) : null;
  }

  @JsonProperty("validateFields")
  public void setValidateFields(Optional<Boolean> validateFields) {
    this.validateFields = validateFields;
  }

  @JsonIgnore
  public void setValidateFields(Boolean validateFields) {
    this.validateFields = validateFields != null ? Optional.of(validateFields) : null;
  }

  @JsonProperty("validateRules")
  public Optional<Boolean> isOptionalValidateRules() {
    return validateRules;
  }

  @JsonIgnore
  public Boolean isValidateRules() {
    return validateRules != null ? validateRules.orElse(null) : null;
  }

  @JsonProperty("validateRules")
  public void setValidateRules(Optional<Boolean> validateRules) {
    this.validateRules = validateRules;
  }

  @JsonIgnore
  public void setValidateRules(Boolean validateRules) {
    this.validateRules = validateRules != null ? Optional.of(validateRules) : null;
  }

  @Schema(description = "Compatibility Level",
      allowableValues = {"BACKWARD", "BACKWARD_TRANSITIVE", "FORWARD", "FORWARD_TRANSITIVE", "FULL",
        "FULL_TRANSITIVE", "NONE"},
      example = "FULL_TRANSITIVE")
  @JsonProperty("compatibility")
  public Optional<String> getOptionalCompatibilityLevel() {
    return this.compatibilityLevel;
  }

  @JsonIgnore
  public String getCompatibilityLevel() {
    return compatibilityLevel != null ? compatibilityLevel.orElse(null) : null;
  }

  @JsonProperty("compatibility")
  public void setCompatibilityLevel(Optional<String> compatibilityLevel) {
    this.compatibilityLevel = compatibilityLevel;
  }

  @JsonIgnore
  public void setCompatibilityLevel(String compatibilityLevel) {
    this.compatibilityLevel = compatibilityLevel != null ? Optional.of(compatibilityLevel) : null;
  }

  @JsonProperty("compatibilityPolicy")
  public Optional<String> getOptionalCompatibilityPolicy() {
    return this.compatibilityPolicy;
  }

  @JsonIgnore
  public String getCompatibilityPolicy() {
    return compatibilityPolicy != null ? compatibilityPolicy.orElse(null) : null;
  }

  @JsonProperty("compatibilityPolicy")
  public void setCompatibilityPolicy(Optional<String> compatibilityPolicy) {
    this.compatibilityPolicy = compatibilityPolicy;
  }

  @JsonIgnore
  public void setCompatibilityPolicy(String compatibilityPolicy) {
    this.compatibilityPolicy = compatibilityPolicy != null
        ? Optional.of(compatibilityPolicy)
        : null;
  }

  @JsonProperty("compatibilityGroup")
  public Optional<String> getOptionalCompatibilityGroup() {
    return this.compatibilityGroup;
  }

  @JsonIgnore
  public String getCompatibilityGroup() {
    return compatibilityGroup != null ? compatibilityGroup.orElse(null) : null;
  }

  @JsonProperty("compatibilityGroup")
  public void setCompatibilityGroup(Optional<String> compatibilityGroup) {
    this.compatibilityGroup = compatibilityGroup;
  }

  @JsonIgnore
  public void setCompatibilityGroup(String compatibilityGroup) {
    this.compatibilityGroup = compatibilityGroup != null ? Optional.of(compatibilityGroup) : null;
  }

  @JsonProperty("defaultMetadata")
  public Optional<Metadata> getOptionalDefaultMetadata() {
    return this.defaultMetadata;
  }

  @JsonIgnore
  public Metadata getDefaultMetadata() {
    return defaultMetadata != null ? defaultMetadata.orElse(null) : null;
  }

  @JsonProperty("defaultMetadata")
  public void setDefaultMetadata(Optional<Metadata> defaultMetadata) {
    this.defaultMetadata = defaultMetadata;
  }

  @JsonIgnore
  public void setDefaultMetadata(Metadata defaultMetadata) {
    this.defaultMetadata = defaultMetadata != null ? Optional.of(defaultMetadata) : null;
  }

  @JsonProperty("overrideMetadata")
  public Optional<Metadata> getOptionalOverrideMetadata() {
    return this.overrideMetadata;
  }

  @JsonIgnore
  public Metadata getOverrideMetadata() {
    return overrideMetadata != null ? overrideMetadata.orElse(null) : null;
  }

  @JsonProperty("overrideMetadata")
  public void setOverrideMetadata(Optional<Metadata> overrideMetadata) {
    this.overrideMetadata = overrideMetadata;
  }

  @JsonIgnore
  public void setOverrideMetadata(Metadata overrideMetadata) {
    this.overrideMetadata = overrideMetadata != null ? Optional.of(overrideMetadata) : null;
  }

  @JsonProperty("defaultRuleSet")
  public Optional<RuleSet> getOptionalDefaultRuleSet() {
    return this.defaultRuleSet;
  }

  @JsonIgnore
  public RuleSet getDefaultRuleSet() {
    return defaultRuleSet != null ? defaultRuleSet.orElse(null) : null;
  }

  @JsonProperty("defaultRuleSet")
  public void setDefaultRuleSet(Optional<RuleSet> defaultRuleSet) {
    this.defaultRuleSet = defaultRuleSet;
  }

  @JsonIgnore
  public void setDefaultRuleSet(RuleSet defaultRuleSet) {
    this.defaultRuleSet = defaultRuleSet != null ? Optional.of(defaultRuleSet) : null;
  }

  @JsonProperty("overrideRuleSet")
  public Optional<RuleSet> getOptionalOverrideRuleSet() {
    return this.overrideRuleSet;
  }

  @JsonIgnore
  public RuleSet getOverrideRuleSet() {
    return overrideRuleSet != null ? overrideRuleSet.orElse(null) : null;
  }

  @JsonProperty("overrideRuleSet")
  public void setOverrideRuleSet(Optional<RuleSet> overrideRuleSet) {
    this.overrideRuleSet = overrideRuleSet;
  }

  @JsonIgnore
  public void setOverrideRuleSet(RuleSet overrideRuleSet) {
    this.overrideRuleSet = overrideRuleSet != null ? Optional.of(overrideRuleSet) : null;
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
    return Objects.equals(alias, that.alias)
        && Objects.equals(normalize, that.normalize)
        && Objects.equals(validateFields, that.validateFields)
        && Objects.equals(validateRules, that.validateRules)
        && Objects.equals(compatibilityLevel, that.compatibilityLevel)
        && Objects.equals(compatibilityPolicy, that.compatibilityPolicy)
        && Objects.equals(compatibilityGroup, that.compatibilityGroup)
        && Objects.equals(defaultMetadata, that.defaultMetadata)
        && Objects.equals(overrideMetadata, that.overrideMetadata)
        && Objects.equals(defaultRuleSet, that.defaultRuleSet)
        && Objects.equals(overrideRuleSet, that.overrideRuleSet);
  }

  @Override
  public int hashCode() {
    return Objects.hash(alias, normalize, validateFields, validateRules,
        compatibilityLevel, compatibilityPolicy, compatibilityGroup,
        defaultMetadata, overrideMetadata, defaultRuleSet, overrideRuleSet);
  }
}
