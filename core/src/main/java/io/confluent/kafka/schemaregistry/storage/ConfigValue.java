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
  private Metadata metadataOverride;
  private RuleSet ruleSetOverride;

  public ConfigValue(@JsonProperty("subject") String subject,
                     @JsonProperty("compatibilityLevel") CompatibilityLevel compatibilityLevel,
                     @JsonProperty("compatibilityGroup") String compatibilityGroup,
                     @JsonProperty("metadataOverride") Metadata metadataOverride,
                     @JsonProperty("ruleSetOverride") RuleSet ruleSetOverride) {
    super(subject);
    this.compatibilityLevel = compatibilityLevel;
    this.compatibilityGroup = compatibilityGroup;
    this.metadataOverride = metadataOverride;
    this.ruleSetOverride = ruleSetOverride;
  }

  public ConfigValue(String subject, Config configEntity) {
    super(subject);
    this.compatibilityLevel = CompatibilityLevel.forName(configEntity.getCompatibilityLevel());
    this.compatibilityGroup = configEntity.getCompatibilityGroup();
    io.confluent.kafka.schemaregistry.client.rest.entities.Metadata metadata =
        configEntity.getMetadataOverride();
    this.metadataOverride = metadata != null ? new Metadata(metadata) : null;
    io.confluent.kafka.schemaregistry.client.rest.entities.RuleSet ruleSet =
        configEntity.getRuleSetOverride();
    this.ruleSetOverride = ruleSet != null ? new RuleSet(ruleSet) : null;
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
    if (!super.equals(o)) {
      return false;
    }
    ConfigValue that = (ConfigValue) o;
    return compatibilityLevel == that.compatibilityLevel
        && Objects.equals(compatibilityGroup, that.compatibilityGroup)
        && Objects.equals(metadataOverride, that.metadataOverride)
        && Objects.equals(ruleSetOverride, that.ruleSetOverride);
  }

  @Override
  public int hashCode() {
    return Objects.hash(super.hashCode(), compatibilityLevel, compatibilityGroup, metadataOverride,
        ruleSetOverride);
  }

  @Override
  public String toString() {
    return "ConfigValue{"
        + "compatibilityLevel=" + compatibilityLevel
        + ", compatibilityGroup='" + compatibilityGroup + '\''
        + ", metadataOverride=" + metadataOverride
        + ", ruleSetOverride=" + ruleSetOverride
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
        metadataOverride != null ? metadataOverride.toMetadataEntity() : null,
        ruleSetOverride != null ? ruleSetOverride.toRuleSetEntity() : null
    );
  }

  public static ConfigValue merge(ConfigValue oldConfig, ConfigValue newConfig) {
    if (oldConfig == null) {
      return newConfig;
    }
    else if (newConfig == null) {
      return oldConfig;
    }
    else {
      return new ConfigValue(
          newConfig.getSubject() != null
              ? newConfig.getSubject() : oldConfig.getSubject(),
          newConfig.getCompatibilityLevel() != null
              ? newConfig.getCompatibilityLevel() : oldConfig.getCompatibilityLevel(),
          newConfig.getCompatibilityGroup() != null
              ? newConfig.getCompatibilityGroup() : oldConfig.getCompatibilityGroup(),
          newConfig.getMetadataOverride() != null
              ? newConfig.getMetadataOverride() : oldConfig.getMetadataOverride(),
          newConfig.getRuleSetOverride() != null
              ? newConfig.getRuleSetOverride() : oldConfig.getRuleSetOverride()
      );
    }
  }
}
