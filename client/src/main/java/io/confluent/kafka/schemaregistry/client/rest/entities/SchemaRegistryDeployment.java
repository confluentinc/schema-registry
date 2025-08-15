/*
 * Copyright 2025 Confluent Inc.
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
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonInclude.Include;
import com.fasterxml.jackson.annotation.JsonProperty;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

@JsonInclude(Include.NON_NULL)
@JsonIgnoreProperties(ignoreUnknown = true)
public class SchemaRegistryDeployment {
  public static final String DEFAULT_ATTRIBUTE = "opensource";

  private final List<String> attributes;

  @JsonCreator
  public SchemaRegistryDeployment() {
    this.attributes = new ArrayList<>();
    this.attributes.add(DEFAULT_ATTRIBUTE);
  }

  public SchemaRegistryDeployment(String attribute) {
    this.attributes = new ArrayList<>();
    this.attributes.add(attribute);
  }

  public SchemaRegistryDeployment(List<String> attributes) {
    this.attributes = new ArrayList<>(attributes);
  }

  @JsonProperty("attributes")
  public List<String> getAttributes() {
    return attributes;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    SchemaRegistryDeployment that = (SchemaRegistryDeployment) o;
    return Objects.equals(attributes, that.attributes);
  }

  @Override
  public int hashCode() {
    return Objects.hash(attributes);
  }
}
