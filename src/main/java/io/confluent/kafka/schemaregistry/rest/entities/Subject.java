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

public class Subject {

  @NotEmpty
  private String name;

  private String compatibility = "full";

  public Subject(@JsonProperty("name") String name,
                 @JsonProperty("compatibility") String compatibility)
  {
    this.name = name;
    this.compatibility = compatibility;
  }

  public Subject(@JsonProperty("name") String name) {
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

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    Subject subject = (Subject) o;

    if (!name.equals(subject.name)) {
      return false;
    }
    if (!this.compatibility.equals(subject.compatibility)) {
      return false;
    }

    return true;
  }

  @Override
  public int hashCode() {
    int result = name.hashCode();
    result = 31 * result + this.compatibility.hashCode();
    return result;
  }
}
