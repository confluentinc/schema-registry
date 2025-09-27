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

package io.confluent.kafka.schemaregistry.client.rest.entities.requests;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.confluent.kafka.schemaregistry.client.rest.entities.LifecyclePolicy;
import io.confluent.kafka.schemaregistry.utils.JacksonMapper;
import java.io.IOException;
import java.util.Objects;
import java.util.Optional;

@JsonInclude(JsonInclude.Include.NON_EMPTY)
@JsonIgnoreProperties(ignoreUnknown = true)
public class AssociationUpdateInfo {

  private String associationType;
  private Optional<LifecyclePolicy> lifecycle;
  private Optional<Boolean> frozen;

  @JsonCreator
  public AssociationUpdateInfo(
      @JsonProperty("associationType") String associationType,
      @JsonProperty("lifecycle") Optional<LifecyclePolicy> lifecycle,
      @JsonProperty("frozen") Optional<Boolean> frozen) {
    this.associationType = associationType;
    this.lifecycle = lifecycle;
    this.frozen = frozen;
  }

  @JsonProperty("associationType")
  public String getAssociationType() {
    return associationType;
  }

  @JsonProperty("associationType")
  public void setAssociationType(String associationType) {
    this.associationType = associationType;
  }

  @JsonProperty("lifecycle")
  public Optional<LifecyclePolicy> getLifecycle() {
    return lifecycle;
  }

  @JsonProperty("lifecycle")
  public void setLifecycle(Optional<LifecyclePolicy> lifecycle) {
    this.lifecycle = lifecycle;
  }

  @JsonProperty("frozen")
  public Optional<Boolean> getFrozen() {
    return frozen;
  }

  @JsonProperty("frozen")
  public void setFrozen(Optional<Boolean> frozen) {
    this.frozen = frozen;
  }

  @Override
  public boolean equals(Object o) {
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    AssociationUpdateInfo that = (AssociationUpdateInfo) o;
    return Objects.equals(frozen, that.frozen)
        && Objects.equals(associationType, that.associationType)
        && Objects.equals(lifecycle, that.lifecycle);
  }

  @Override
  public int hashCode() {
    return Objects.hash(associationType, lifecycle, frozen);
  }

  public String toJson() throws IOException {
    return JacksonMapper.INSTANCE.writeValueAsString(this);
  }
}