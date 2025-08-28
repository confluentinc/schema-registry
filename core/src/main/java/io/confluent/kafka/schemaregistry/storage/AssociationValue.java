/*
 * Copyright 2025 Confluent Inc.
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

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.confluent.kafka.schemaregistry.client.rest.entities.Association;
import io.confluent.kafka.schemaregistry.client.rest.entities.LifecyclePolicy;
import jakarta.validation.constraints.NotEmpty;
import java.util.Objects;

@JsonInclude(JsonInclude.Include.NON_EMPTY)
@JsonIgnoreProperties(ignoreUnknown = true)
public class AssociationValue extends SubjectValue {

  @NotEmpty
  private String guid;
  private String tenant;
  private String associationType;
  private String resourceType;
  private String resourceName;
  private String resourceNamespace;
  private String resourceId;
  private Lifecycle lifecycle;
  @NotEmpty
  private boolean frozen;

  @JsonCreator
  public AssociationValue(@JsonProperty("subject") String subject,
      @JsonProperty("guid") String guid,
      @JsonProperty("tenant") String tenant,
      @JsonProperty("associationType") String associationType,
      @JsonProperty("resourceType") String resourceType,
      @JsonProperty("resourceName") String resourceName,
      @JsonProperty("resourceNamespace") String resourceNamespace,
      @JsonProperty("resourceId") String resourceId,
      @JsonProperty("lifecycle") Lifecycle lifecycle,
      @JsonProperty("frozen") boolean frozen) {
    super(subject);
    this.guid = guid;
    this.tenant = tenant;
    this.associationType = associationType;
    this.resourceType = resourceType;
    this.resourceName = resourceName;
    this.resourceNamespace = resourceNamespace;
    this.resourceId = resourceId;
    this.lifecycle = lifecycle;
    this.frozen = frozen;
  }

  // getters and setters
  @JsonProperty("guid")
  public String getGuid() {
    return guid;
  }

  @JsonProperty("guid")
  public void setGuid(String guid) {
    this.guid = guid;
  }

  @JsonProperty("tenant")
  public String getTenant() {
    return tenant;
  }

  @JsonProperty("tenant")
  public void setTenant(String tenant) {
    this.tenant = tenant;
  }

  @JsonProperty("associationType")
  public String getAssociationType() {
    return associationType;
  }

  @JsonProperty("associationType")
  public void setAssociationType(String associationType) {
    this.associationType = associationType;
  }

  @JsonProperty("resourceType")
  public String getResourceType() {
    return resourceType;
  }

  @JsonProperty("resourceType")
  public void setResourceType(String resourceType) {
    this.resourceType = resourceType;
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

  @JsonProperty("lifecycle")
  public Lifecycle getLifecycle() {
    return lifecycle;
  }

  @JsonProperty("lifecycle")
  public void setLifecycle(Lifecycle lifecycle) {
    this.lifecycle = lifecycle;
  }

  @JsonProperty("frozen")
  public boolean isFrozen() {
    return frozen;
  }

  @JsonProperty("frozen")
  public void setFrozen(boolean frozen) {
    this.frozen = frozen;
  }

  @Override
  public boolean equals(Object o) {
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    if (!super.equals(o)) {
      return false;
    }
    AssociationValue that = (AssociationValue) o;
    return frozen == that.frozen
        && Objects.equals(guid, that.guid)
        && Objects.equals(tenant, that.tenant)
        && Objects.equals(associationType, that.associationType)
        && Objects.equals(resourceType, that.resourceType)
        && Objects.equals(resourceName, that.resourceName)
        && Objects.equals(resourceNamespace, that.resourceNamespace)
        && Objects.equals(resourceId, that.resourceId)
        && lifecycle == that.lifecycle;
  }

  @Override
  public int hashCode() {
    return Objects.hash(
        super.hashCode(), guid, tenant, associationType, resourceType,
        resourceName, resourceNamespace, resourceId, lifecycle, frozen);
  }

  @Override
  public String toString() {
    return "AssociationValue{"
        + "guid='" + guid + '\''
        + ", tenant='" + tenant + '\''
        + ", associationType='" + associationType + '\''
        + ", resourceType='" + resourceType + '\''
        + ", resourceName='" + resourceName + '\''
        + ", resourceNamespace='" + resourceNamespace + '\''
        + ", resourceId='" + resourceId + '\''
        + ", lifecycle=" + lifecycle
        + ", frozen=" + frozen
        + '}';
  }

  @Override
  public AssociationKey toKey() {
    return new AssociationKey(tenant, associationType, resourceType,
        resourceName, resourceNamespace);
  }

  public Association toAssociationEntity() {
    return new Association(
        getSubject(),
        guid,
        associationType,
        resourceType,
        resourceName,
        resourceNamespace,
        resourceId,
        lifecycle == Lifecycle.STRONG
            ? LifecyclePolicy.STRONG
            : LifecyclePolicy.WEAK,
        frozen
    );
  }

}