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

import static io.confluent.kafka.schemaregistry.client.rest.utils.RestValidation.checkName;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.confluent.kafka.schemaregistry.client.rest.entities.ErrorMessage;
import io.confluent.kafka.schemaregistry.client.rest.entities.LifecyclePolicy;
import io.confluent.kafka.schemaregistry.utils.QualifiedSubject;
import io.confluent.kafka.schemaregistry.client.rest.exceptions.IllegalPropertyException;
import io.confluent.kafka.schemaregistry.utils.JacksonMapper;
import java.io.IOException;
import java.util.List;
import java.util.Objects;

@JsonInclude(JsonInclude.Include.NON_EMPTY)
@JsonIgnoreProperties(ignoreUnknown = true)
public class AssociationOpRequest {

  private static final String TOPIC_RESOURCE_TYPE = "topic";

  private String resourceName;
  private String resourceNamespace;
  private String resourceId;
  private String resourceType;
  private List<? extends AssociationOp> associations;
  private ErrorMessage error;

  @JsonCreator
  public AssociationOpRequest(
      @JsonProperty("resourceName") String resourceName,
      @JsonProperty("resourceNamespace") String resourceNamespace,
      @JsonProperty("resourceId") String resourceId,
      @JsonProperty("resourceType") String resourceType,
      @JsonProperty("associations") List<? extends AssociationOp> associations) {
    this.resourceName = resourceName;
    this.resourceNamespace = resourceNamespace;
    this.resourceId = resourceId;
    this.resourceType = resourceType;
    this.associations = associations;
  }

  @JsonProperty("resourceName")
  public String getResourceName() {
    return resourceName;
  }

  @JsonProperty("resourceName")
  public void setResourceName(String resourceName) {
    this.resourceName = resourceName;
  }

  @JsonProperty("resourceNamespace")
  public String getResourceNamespace() {
    return resourceNamespace;
  }

  @JsonProperty("resourceNamespace")
  public void setResourceNamespace(String resourceNamespace) {
    this.resourceNamespace = resourceNamespace;
  }

  @JsonProperty("resourceId")
  public String getResourceId() {
    return resourceId;
  }

  @JsonProperty("resourceId")
  public void setResourceId(String resourceId) {
    this.resourceId = resourceId;
  }

  @JsonProperty("resourceType")
  public String getResourceType() {
    return resourceType;
  }

  @JsonProperty("resourceType")
  public void setResourceType(String resourceType) {
    this.resourceType = resourceType;
  }

  @JsonProperty("associations")
  public List<? extends AssociationOp> getAssociations() {
    return associations;
  }

  @JsonProperty("associations")
  public void setAssociations(List<? extends AssociationOp> associations) {
    this.associations = associations;
  }

  @JsonProperty("error")
  public ErrorMessage getError() {
    return error;
  }

  @JsonProperty("error")
  public void setError(ErrorMessage error) {
    this.error = error;
  }

  @Override
  public boolean equals(Object o) {
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    AssociationOpRequest that = (AssociationOpRequest) o;
    return Objects.equals(resourceName, that.resourceName)
        && Objects.equals(resourceNamespace, that.resourceNamespace)
        && Objects.equals(resourceId, that.resourceId)
        && Objects.equals(resourceType, that.resourceType)
        && Objects.equals(associations, that.associations);
  }

  @Override
  public int hashCode() {
    return Objects.hash(
        resourceName, resourceNamespace, resourceId, resourceType, associations);
  }

  public String toJson() throws IOException {
    return JacksonMapper.INSTANCE.writeValueAsString(this);
  }

  // Validates resource fields, then for each op: validates the op, defaults
  // the subject to :.ns:name-type for STRONG associations, and enforces that
  // frozen associations use the default subject.
  public void validate(boolean dryRun) {
    checkName(getResourceName(), "resourceName");
    checkName(getResourceNamespace(), "resourceNamespace");
    if (!dryRun && (getResourceId() == null || getResourceId().isEmpty())) {
      throw new IllegalPropertyException("resourceId", "cannot be null or empty");
    }
    if (getResourceType() != null && !getResourceType().isEmpty()) {
      if (!getResourceType().equals(TOPIC_RESOURCE_TYPE)) {
        throw new IllegalPropertyException(
            "resourceType", "must be '" + TOPIC_RESOURCE_TYPE + "'");
      }
    } else {
      setResourceType(TOPIC_RESOURCE_TYPE);
    }
    if (getAssociations() == null || getAssociations().isEmpty()) {
      throw new IllegalPropertyException("associations", "cannot be null or empty");
    }
    for (AssociationOp op : getAssociations()) {
      op.validate(dryRun);
      if (op instanceof AssociationCreateOrUpdateOp) {
        AssociationCreateOrUpdateOp createOrUpdateOp = (AssociationCreateOrUpdateOp) op;
        if (op instanceof AssociationCreateOp) {
          String defaultSubject = QualifiedSubject.CONTEXT_PREFIX + resourceNamespace
              + QualifiedSubject.CONTEXT_DELIMITER + resourceName
              + "-" + createOrUpdateOp.getAssociationType();
          if (createOrUpdateOp.getSubject() == null) {
            createOrUpdateOp.setSubject(defaultSubject);
          }
          // Frozen associations must use the default subject format
          if (Boolean.TRUE.equals(createOrUpdateOp.getFrozen())
              && !createOrUpdateOp.getSubject().equals(defaultSubject)) {
            throw new IllegalPropertyException(
                "subject", "frozen associations must use subject '" + defaultSubject + "'");
          }
          // WEAK associations cannot use the default subject format
          if (createOrUpdateOp.getLifecycle() == LifecyclePolicy.WEAK
              && createOrUpdateOp.getSubject().equals(defaultSubject)) {
            throw new IllegalPropertyException(
                "subject", "WEAK associations cannot use subject '" + defaultSubject + "'");
          }
        }
      }
    }
  }
}