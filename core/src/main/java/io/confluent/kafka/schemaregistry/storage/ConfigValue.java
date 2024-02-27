/*
 * Copyright 2018 Confluent Inc.
 *
 * Licensed under the Confluent Community License (the "License"); you may not use
 * this file except in compliance with the License.  You may obtain a copy of the
 * License at
 *
 * http://www.confluent.io/confluent-community-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OF ANY KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations under the License.
 */

package io.confluent.kafka.schemaregistry.storage;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonInclude.Include;
import com.fasterxml.jackson.annotation.JsonProperty;

import io.confluent.kafka.schemaregistry.CompatibilityLevel;
import io.confluent.kafka.schemaregistry.client.rest.entities.Config;
import java.util.Objects;

@JsonInclude(Include.NON_EMPTY)
@JsonIgnoreProperties(ignoreUnknown = true)
public class ConfigValue extends SubjectValue {

  private String alias;
  private Boolean normalize;
  private Boolean validateFields;
  private CompatibilityLevel compatibilityLevel;
  private String compatibilityGroup;
  private Metadata defaultMetadata;
  private Metadata overrideMetadata;
  private RuleSet defaultRuleSet;
  private RuleSet overrideRuleSet;

  public ConfigValue(@JsonProperty("subject") String subject,
                     @JsonProperty("alias") String alias,
                     @JsonProperty("normalize") Boolean normalize,
                     @JsonProperty("validateFields") Boolean validateFields,
                     @JsonProperty("compatibilityLevel") CompatibilityLevel compatibilityLevel,
                     @JsonProperty("compatibilityGroup") String compatibilityGroup,
                     @JsonProperty("defaultMetadata") Metadata defaultMetadata,
                     @JsonProperty("overrideMetadata") Metadata overrideMetadata,
                     @JsonProperty("defaultRuleSet") RuleSet defaultRuleSet,
                     @JsonProperty("overrideRuleSet") RuleSet overrideRuleSet) {
    super(subject);
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

  public ConfigValue(String subject, Config configEntity) {
    super(subject);
    this.alias = configEntity.getAlias();
    this.normalize = configEntity.isNormalize();
    this.validateFields = configEntity.isValidateFields();
    this.compatibilityLevel = CompatibilityLevel.forName(configEntity.getCompatibilityLevel());
    this.compatibilityGroup = configEntity.getCompatibilityGroup();
    io.confluent.kafka.schemaregistry.client.rest.entities.Metadata defaultMetadata =
        configEntity.getDefaultMetadata();
    this.defaultMetadata = defaultMetadata != null ? new Metadata(defaultMetadata) : null;
    io.confluent.kafka.schemaregistry.client.rest.entities.Metadata overrideMetadata =
        configEntity.getOverrideMetadata();
    this.overrideMetadata = overrideMetadata != null ? new Metadata(overrideMetadata) : null;
    io.confluent.kafka.schemaregistry.client.rest.entities.RuleSet defaultRuleSet =
        configEntity.getDefaultRuleSet();
    this.defaultRuleSet = defaultRuleSet != null ? new RuleSet(defaultRuleSet) : null;
    io.confluent.kafka.schemaregistry.client.rest.entities.RuleSet overrideRuleSet =
        configEntity.getOverrideRuleSet();
    this.overrideRuleSet = overrideRuleSet != null ? new RuleSet(overrideRuleSet) : null;
  }

  public ConfigValue(String subject, Config configEntity, RuleSetHandler ruleSetHandler) {
    super(subject);
    this.alias = configEntity.getAlias();
    this.normalize = configEntity.isNormalize();
    this.validateFields = configEntity.isValidateFields();
    this.compatibilityLevel = CompatibilityLevel.forName(configEntity.getCompatibilityLevel());
    this.compatibilityGroup = configEntity.getCompatibilityGroup();
    io.confluent.kafka.schemaregistry.client.rest.entities.Metadata defaultMetadata =
        configEntity.getDefaultMetadata();
    this.defaultMetadata = defaultMetadata != null ? new Metadata(defaultMetadata) : null;
    io.confluent.kafka.schemaregistry.client.rest.entities.Metadata overrideMetadata =
        configEntity.getOverrideMetadata();
    this.overrideMetadata = overrideMetadata != null ? new Metadata(overrideMetadata) : null;
    this.defaultRuleSet = ruleSetHandler.transform(configEntity.getDefaultRuleSet());
    this.overrideRuleSet = ruleSetHandler.transform(configEntity.getOverrideRuleSet());
  }

  public ConfigValue(String subject, CompatibilityLevel compatibilityLevel) {
    super(subject);
    this.compatibilityLevel = compatibilityLevel;
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
  public void setValidateFields(Boolean validateFields) {
    this.validateFields = validateFields;
  }

  @JsonProperty("compatibilityLevel")
  public CompatibilityLevel getCompatibilityLevel() {
    return compatibilityLevel;
  }

  @JsonProperty("compatibilityLevel")
  public void setCompatibilityLevel(CompatibilityLevel compatibilityLevel) {
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

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    if (!super.equals(o)) {
      return false;
    }
    ConfigValue that = (ConfigValue) o;
    return Objects.equals(alias, that.alias)
        && Objects.equals(normalize, that.normalize)
        && Objects.equals(validateFields, that.validateFields)
        && compatibilityLevel == that.compatibilityLevel
        && Objects.equals(compatibilityGroup, that.compatibilityGroup)
        && Objects.equals(defaultMetadata, that.defaultMetadata)
        && Objects.equals(overrideMetadata, that.overrideMetadata)
        && Objects.equals(defaultRuleSet, that.defaultRuleSet)
        && Objects.equals(overrideRuleSet, that.overrideRuleSet);
  }

  @Override
  public int hashCode() {
    return Objects.hash(super.hashCode(), alias, normalize, validateFields, compatibilityLevel,
            compatibilityGroup, defaultMetadata, overrideMetadata, defaultRuleSet,
            overrideRuleSet);
  }

  @Override
  public String toString() {
    return "ConfigValue{"
        + "alias='" + alias + '\''
        + ", normalize=" + normalize
        + ", validateFields=" + validateFields
        + ", compatibilityLevel=" + compatibilityLevel
        + ", compatibilityGroup='" + compatibilityGroup + '\''
        + ", defaultMetadata=" + defaultMetadata
        + ", overrideMetadata=" + overrideMetadata
        + ", defaultRuleSet=" + defaultRuleSet
        + ", overrideRuleSet=" + overrideRuleSet
        + '}';
  }

  @Override
  public ConfigKey toKey() {
    return new ConfigKey(getSubject());
  }

  public Config toConfigEntity() {
    return new Config(
        alias,
        normalize,
        validateFields,
        compatibilityLevel != null ? compatibilityLevel.name : null,
        compatibilityGroup,
        defaultMetadata != null ? defaultMetadata.toMetadataEntity() : null,
        overrideMetadata != null ? overrideMetadata.toMetadataEntity() : null,
        defaultRuleSet != null ? defaultRuleSet.toRuleSetEntity() : null,
        overrideRuleSet != null ? overrideRuleSet.toRuleSetEntity() : null
    );
  }

  public static ConfigValue update(ConfigValue oldConfig, ConfigValue newConfig) {
    if (oldConfig == null) {
      return newConfig;
    } else if (newConfig == null) {
      return oldConfig;
    } else {
      return new ConfigValue(
          newConfig.getSubject() != null
              ? newConfig.getSubject() : oldConfig.getSubject(),
          newConfig.getAlias() != null
              ? newConfig.getAlias() : oldConfig.getAlias(),
          newConfig.isNormalize() != null
              ? newConfig.isNormalize() : oldConfig.isNormalize(),
          newConfig.isValidateFields() != null
              ? newConfig.isValidateFields() : oldConfig.isValidateFields(),
          newConfig.getCompatibilityLevel() != null
              ? newConfig.getCompatibilityLevel() : oldConfig.getCompatibilityLevel(),
          newConfig.getCompatibilityGroup() != null
              ? newConfig.getCompatibilityGroup() : oldConfig.getCompatibilityGroup(),
          newConfig.getDefaultMetadata() != null
              ? newConfig.getDefaultMetadata() : oldConfig.getDefaultMetadata(),
          newConfig.getOverrideMetadata() != null
              ? newConfig.getOverrideMetadata() : oldConfig.getOverrideMetadata(),
          newConfig.getDefaultRuleSet() != null
              ? newConfig.getDefaultRuleSet() : oldConfig.getDefaultRuleSet(),
          newConfig.getOverrideRuleSet() != null
              ? newConfig.getOverrideRuleSet() : oldConfig.getOverrideRuleSet()
      );
    }
  }
}
