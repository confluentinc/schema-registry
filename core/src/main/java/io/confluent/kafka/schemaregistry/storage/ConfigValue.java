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

@JsonInclude(Include.NON_EMPTY)
@JsonIgnoreProperties(ignoreUnknown = true)
public class ConfigValue extends SubjectValue {

  private CompatibilityLevel compatibilityLevel;

  public ConfigValue(@JsonProperty("subject") String subject,
                     @JsonProperty("compatibilityLevel") CompatibilityLevel compatibilityLevel) {
    super(subject);
    this.compatibilityLevel = compatibilityLevel;
  }

  public ConfigValue() {
    super(null);
    compatibilityLevel = null;
  }

  @JsonProperty("compatibilityLevel")
  public CompatibilityLevel getCompatibilityLevel() {
    return compatibilityLevel;
  }

  @JsonProperty("compatibilityLevel")
  public void setCompatibilityLevel(CompatibilityLevel compatibilityLevel) {
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
    if (!super.equals(o)) {
      return false;
    }

    ConfigValue that = (ConfigValue) o;

    if (!this.compatibilityLevel.equals(that.compatibilityLevel)) {
      return false;
    }
    return true;
  }

  @Override
  public int hashCode() {
    int result = super.hashCode();
    result = 31 * result + compatibilityLevel.hashCode();
    return result;
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append("{compatibilityLevel=" + this.compatibilityLevel + "}");
    return sb.toString();
  }

  @Override
  public ConfigKey toKey() {
    return new ConfigKey(getSubject());
  }
}
