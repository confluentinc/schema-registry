/**
 * Copyright 2014 Confluent Inc.
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
package io.confluent.kafka.schemaregistry.rest.entities;

import com.fasterxml.jackson.annotation.JsonProperty;

import org.hibernate.validator.constraints.NotEmpty;

public class Topic {

  @NotEmpty
  private String name;

  private String compatibility = "full";
  private String registration = "all";
  private String deprecation = "all";
  private String validators = null;

  public Topic(@JsonProperty("name") String name,
               @JsonProperty("compatibility") String compatibility,
               @JsonProperty("registration") String registration,
               @JsonProperty("deprecation") String deprecation,
               @JsonProperty("validators") String validators) {
    this.name = name;
    this.compatibility = compatibility;
    this.registration = registration;
    this.deprecation = deprecation;
    this.validators = validators;
  }

  public Topic(@JsonProperty("name") String name) {
    this.name = name;
  }

  @JsonProperty("name")
  public String getName() {
    return name;
  }

  @JsonProperty("name")
  public void setName(String name) {
    this.name = name;
  }

  @JsonProperty("compatibility")
  public String getCompatibility() {
    return this.compatibility;
  }

  @JsonProperty("compatibility")
  public void setCompatibility(String compatibility) {
    this.compatibility = compatibility;
  }

  @JsonProperty("registration")
  public String getRegistration() {
    return this.registration;
  }

  @JsonProperty("registration")
  public void setRegistration(String registration) {
    this.registration = registration;
  }

  @JsonProperty("deprecation")
  public String getDeprecation() {
    return this.deprecation;
  }

  @JsonProperty("deprecation")
  public void setDeprecation(String deprecation) {
    this.deprecation = deprecation;
  }

  @JsonProperty("validators")
  public String getValidators() {
    return this.validators;
  }

  @JsonProperty("validators")
  public void setValidators(String validators) {
    this.validators = validators;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    Topic topic = (Topic) o;

    if (!name.equals(topic.name)) {
      return false;
    }
    if (!this.compatibility.equals(topic.compatibility)) {
      return false;
    }
    if (!this.registration.equals(topic.registration)) {
      return false;
    }
    if (!this.deprecation.equals(topic.deprecation)) {
      return false;
    }
    if (!this.validators.equals(topic.validators)) {
      return false;
    }

    return true;
  }

  @Override
  public int hashCode() {
    int result = name.hashCode();
    result = 31 * result + this.registration.hashCode();
    result = 31 * result + this.deprecation.hashCode();
    result = 31 * result + this.validators.hashCode();
    return result;
  }

}
