/*
 * Copyright 2026 Confluent Inc.
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
import io.confluent.kafka.schemaregistry.client.rest.entities.OpType;
import io.confluent.kafka.schemaregistry.client.rest.exceptions.IllegalPropertyException;
import java.util.Objects;

@JsonInclude(JsonInclude.Include.NON_EMPTY)
@JsonIgnoreProperties(ignoreUnknown = true)
public class AssociationDeleteOp extends AssociationOp {

  private String associationType;

  @JsonCreator
  public AssociationDeleteOp(
      @JsonProperty("associationType") String associationType) {
    super(OpType.DELETE);
    this.associationType = associationType;
  }

  @JsonProperty("associationType")
  public String getAssociationType() {
    return associationType;
  }

  @JsonProperty("associationType")
  public void setAssociationType(String associationType) {
    this.associationType = associationType;
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
    AssociationDeleteOp that = (AssociationDeleteOp) o;
    return Objects.equals(associationType, that.associationType);
  }

  @Override
  public int hashCode() {
    return Objects.hash(super.hashCode(), associationType);
  }

  public void validate(boolean dryRun) {
    if (getAssociationType() != null && !getAssociationType().isEmpty()) {
      if (!getAssociationType().equals(KEY_ASSOCIATION_TYPE)
          && !getAssociationType().equals(VALUE_ASSOCIATION_TYPE)) {
        throw new IllegalPropertyException(
            "associationType",
            "must be either '" + KEY_ASSOCIATION_TYPE + "' or '" + VALUE_ASSOCIATION_TYPE + "'");
      }
    } else {
      setAssociationType(VALUE_ASSOCIATION_TYPE);
    }
  }
}