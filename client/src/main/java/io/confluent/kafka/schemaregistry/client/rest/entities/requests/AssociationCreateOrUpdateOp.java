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

import static io.confluent.kafka.schemaregistry.client.rest.utils.RestValidation.checkSubject;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.confluent.kafka.schemaregistry.client.rest.entities.LifecyclePolicy;
import io.confluent.kafka.schemaregistry.client.rest.entities.OpType;
import io.confluent.kafka.schemaregistry.client.rest.exceptions.IllegalPropertyException;
import java.util.Objects;

@JsonInclude(JsonInclude.Include.NON_EMPTY)
@JsonIgnoreProperties(ignoreUnknown = true)
public abstract class AssociationCreateOrUpdateOp extends AssociationOp {

  private String subject;
  private String associationType;
  private LifecyclePolicy lifecycle;
  private Boolean frozen;
  private RegisterSchemaRequest schema;
  private Boolean normalize;

  @JsonCreator
  public AssociationCreateOrUpdateOp(
      @JsonProperty("opType") OpType opType,
      @JsonProperty("subject") String subject,
      @JsonProperty("associationType") String associationType,
      @JsonProperty("lifecycle") LifecyclePolicy lifecycle,
      @JsonProperty("frozen") Boolean frozen,
      @JsonProperty("schema") RegisterSchemaRequest schema,
      @JsonProperty("normalize") Boolean normalize) {
    super(opType);
    this.subject = subject;
    this.associationType = associationType;
    this.lifecycle = lifecycle;
    this.frozen = frozen;
    this.schema = schema;
    this.normalize = normalize;
  }

  @JsonProperty("subject")
  public String getSubject() {
    return subject;
  }

  @JsonProperty("subject")
  public void setSubject(String subject) {
    this.subject = subject;
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
  public LifecyclePolicy getLifecycle() {
    return lifecycle;
  }

  @JsonProperty("lifecycle")
  public void setLifecycle(LifecyclePolicy lifecycle) {
    this.lifecycle = lifecycle;
  }

  @JsonProperty("frozen")
  public Boolean getFrozen() {
    return frozen;
  }

  @JsonProperty("frozen")
  public void setFrozen(Boolean frozen) {
    this.frozen = frozen;
  }

  @JsonProperty("schema")
  public RegisterSchemaRequest getSchema() {
    return schema;
  }

  @JsonProperty("schema")
  public void setSchema(RegisterSchemaRequest schema) {
    this.schema = schema;
  }

  @JsonProperty("normalize")
  public Boolean getNormalize() {
    return normalize;
  }

  @JsonProperty("normalize")
  public void setNormalize(Boolean normalize) {
    this.normalize = normalize;
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
    AssociationCreateOrUpdateOp that = (AssociationCreateOrUpdateOp) o;
    return Objects.equals(subject, that.subject)
        && Objects.equals(associationType, that.associationType)
        && Objects.equals(lifecycle, that.lifecycle)
        && Objects.equals(frozen, that.frozen)
        && Objects.equals(schema, that.schema)
        && Objects.equals(normalize, that.normalize);
  }

  @Override
  public int hashCode() {
    return Objects.hash(
        super.hashCode(), subject, associationType, lifecycle, frozen, schema, normalize);
  }

  /**
   * Applies CREATE defaults: schema implies frozen STRONG, frozen requires schema,
   * non-schema defaults to non-frozen, lifecycle defaults to WEAK.
   */
  public void applyCreateDefaults() {
    if (getSchema() != null) {
      if (getLifecycle() == LifecyclePolicy.WEAK) {
        throw new IllegalPropertyException(
            "lifecycle", "cannot be WEAK when schema is provided for create");
      }
      if (Boolean.FALSE.equals(getFrozen())) {
        throw new IllegalPropertyException(
            "frozen", "cannot be false when schema is provided for create");
      }
      setLifecycle(LifecyclePolicy.STRONG);
      setFrozen(true);
    } else if (Boolean.TRUE.equals(getFrozen())) {
      throw new IllegalPropertyException(
          "schema", "schema must be provided when creating a frozen association");
    } else {
      setFrozen(false);
    }
    if (getLifecycle() == null) {
      setLifecycle(LifecyclePolicy.WEAK);
    }
  }

  /**
   * Applies UPSERT defaults when no existing association exists:
   * schema implies STRONG (not frozen), lifecycle defaults to WEAK.
   */
  public void applyUpsertDefaults() {
    if (getSchema() != null) {
      if (getLifecycle() == LifecyclePolicy.WEAK) {
        throw new IllegalPropertyException(
            "lifecycle", "cannot be WEAK when schema is provided");
      }
      setLifecycle(LifecyclePolicy.STRONG);
      if (getFrozen() == null) {
        setFrozen(false);
      }
    } else {
      if (getFrozen() == null) {
        setFrozen(false);
      }
    }
    if (getLifecycle() == null) {
      setLifecycle(LifecyclePolicy.WEAK);
    }
  }

  // Base validation for the batch path (shared by CREATE and UPSERT ops).
  // Validates subject format, defaults associationType, and enforces WEAK
  // restrictions (no schema, no frozen). Lifecycle is NOT defaulted here —
  // CREATE calls applyCreateDefaults() first, while UPSERT leaves it null
  // (the server uses the existing association's lifecycle).
  public void validate(boolean dryRun) {
    if (getSubject() != null) {
      checkSubject(getSubject());
    }
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
    if (getLifecycle() == LifecyclePolicy.WEAK) {
      if (getSchema() != null) {
        throw new IllegalPropertyException(
            "lifecycle", "cannot be WEAK when schema is provided");
      }
      if (Boolean.TRUE.equals(getFrozen())) {
        throw new IllegalPropertyException(
            "frozen", "association with lifecycle of WEAK cannot be frozen");
      }
    }
  }
}