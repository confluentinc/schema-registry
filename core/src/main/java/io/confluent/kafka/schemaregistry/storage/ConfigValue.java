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

  private CompatibilityLevel compatibilityLevel;
  private String compatibilityGroup;
  private Metadata initialMetadata;
  private Metadata finalMetadata;
  private RuleSet initialRuleSet;
  private RuleSet finalRuleSet;

  public ConfigValue(@JsonProperty("subject") String subject,
                     @JsonProperty("compatibilityLevel") CompatibilityLevel compatibilityLevel,
                     @JsonProperty("compatibilityGroup") String compatibilityGroup,
                     @JsonProperty("initialMetadata") Metadata initialMetadata,
                     @JsonProperty("finalMetadata") Metadata finalMetadata,
                     @JsonProperty("initialRuleSet") RuleSet initialRuleSet,
                     @JsonProperty("finalRuleSet") RuleSet finalRuleSet) {
    super(subject);
    this.compatibilityLevel = compatibilityLevel;
    this.compatibilityGroup = compatibilityGroup;
    this.initialMetadata = initialMetadata;
    this.finalMetadata = finalMetadata;
    this.initialRuleSet = initialRuleSet;
    this.finalRuleSet = finalRuleSet;
  }

  public ConfigValue(String subject, Config configEntity) {
    super(subject);
    this.compatibilityLevel = CompatibilityLevel.forName(configEntity.getCompatibilityLevel());
    this.compatibilityGroup = configEntity.getCompatibilityGroup();
    io.confluent.kafka.schemaregistry.client.rest.entities.Metadata initialMetadata =
        configEntity.getInitialMetadata();
    this.initialMetadata = initialMetadata != null ? new Metadata(initialMetadata) : null;
    io.confluent.kafka.schemaregistry.client.rest.entities.Metadata finalMetadata =
        configEntity.getFinalMetadata();
    this.finalMetadata = finalMetadata != null ? new Metadata(finalMetadata) : null;
    io.confluent.kafka.schemaregistry.client.rest.entities.RuleSet initialRuleSet =
        configEntity.getInitialRuleSet();
    this.initialRuleSet = initialRuleSet != null ? new RuleSet(initialRuleSet) : null;
    io.confluent.kafka.schemaregistry.client.rest.entities.RuleSet finalRuleSet =
        configEntity.getFinalRuleSet();
    this.finalRuleSet = finalRuleSet != null ? new RuleSet(finalRuleSet) : null;
  }

  public ConfigValue(String subject, Config configEntity, RuleSetHandler ruleSetHandler) {
    super(subject);
    this.compatibilityLevel = CompatibilityLevel.forName(configEntity.getCompatibilityLevel());
    this.compatibilityGroup = configEntity.getCompatibilityGroup();
    io.confluent.kafka.schemaregistry.client.rest.entities.Metadata initialMetadata =
        configEntity.getInitialMetadata();
    this.initialMetadata = initialMetadata != null ? new Metadata(initialMetadata) : null;
    io.confluent.kafka.schemaregistry.client.rest.entities.Metadata finalMetadata =
        configEntity.getFinalMetadata();
    this.finalMetadata = finalMetadata != null ? new Metadata(finalMetadata) : null;
    this.initialRuleSet = ruleSetHandler.transform(configEntity.getInitialRuleSet());
    this.finalRuleSet = ruleSetHandler.transform(configEntity.getFinalRuleSet());
  }

  public ConfigValue(String subject, CompatibilityLevel compatibilityLevel) {
    super(subject);
    this.compatibilityLevel = compatibilityLevel;
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
  public RuleSet getfinalRuleSet() {
    return this.finalRuleSet;
  }

  @JsonProperty("finalRuleSet")
  public void setfinalRuleSet(RuleSet finalRuleSet) {
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
    if (!super.equals(o)) {
      return false;
    }
    ConfigValue that = (ConfigValue) o;
    return compatibilityLevel == that.compatibilityLevel
        && Objects.equals(compatibilityGroup, that.compatibilityGroup)
        && Objects.equals(initialMetadata, that.initialMetadata)
        && Objects.equals(finalMetadata, that.finalMetadata)
        && Objects.equals(initialRuleSet, that.initialRuleSet)
        && Objects.equals(finalRuleSet, that.finalRuleSet);
  }

  @Override
  public int hashCode() {
    return Objects.hash(super.hashCode(), compatibilityLevel, compatibilityGroup,
        initialMetadata, finalMetadata, initialRuleSet, finalRuleSet);
  }

  @Override
  public String toString() {
    return "ConfigValue{"
        + "compatibilityLevel=" + compatibilityLevel
        + ", compatibilityGroup='" + compatibilityGroup + '\''
        + ", initialMetadata=" + initialMetadata
        + ", finalMetadata=" + finalMetadata
        + ", initialRuleSet=" + initialRuleSet
        + ", finalRuleSet=" + finalRuleSet
        + '}';
  }

  @Override
  public ConfigKey toKey() {
    return new ConfigKey(getSubject());
  }

  public Config toConfigEntity() {
    return new Config(
        compatibilityLevel != null ? compatibilityLevel.name : null,
        compatibilityGroup,
        initialMetadata != null ? initialMetadata.toMetadataEntity() : null,
        finalMetadata != null ? finalMetadata.toMetadataEntity() : null,
        initialRuleSet != null ? initialRuleSet.toRuleSetEntity() : null,
        finalRuleSet != null ? finalRuleSet.toRuleSetEntity() : null
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
          newConfig.getCompatibilityLevel() != null
              ? newConfig.getCompatibilityLevel() : oldConfig.getCompatibilityLevel(),
          newConfig.getCompatibilityGroup() != null
              ? newConfig.getCompatibilityGroup() : oldConfig.getCompatibilityGroup(),
          newConfig.getInitialMetadata() != null
              ? newConfig.getInitialMetadata() : oldConfig.getInitialMetadata(),
          newConfig.getFinalMetadata() != null
              ? newConfig.getFinalMetadata() : oldConfig.getFinalMetadata(),
          newConfig.getInitialRuleSet() != null
              ? newConfig.getInitialRuleSet() : oldConfig.getInitialRuleSet(),
          newConfig.getfinalRuleSet() != null
              ? newConfig.getfinalRuleSet() : oldConfig.getfinalRuleSet()
      );
    }
  }
}
