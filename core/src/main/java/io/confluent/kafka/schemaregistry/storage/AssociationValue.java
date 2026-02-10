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
import io.confluent.kafka.schemaregistry.client.rest.entities.requests.AssociationCreateOrUpdateRequest;
import io.confluent.kafka.schemaregistry.utils.QualifiedSubject;
import jakarta.validation.constraints.NotEmpty;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;

@JsonInclude(JsonInclude.Include.NON_EMPTY)
@JsonIgnoreProperties(ignoreUnknown = true)
public class AssociationValue extends SubjectValue {

  @NotEmpty
  private String guid;
  private String tenant;
  private String resourceName;
  private String resourceNamespace;
  private String resourceId;
  private String resourceType;
  private String associationType;
  private Lifecycle lifecycle;
  @NotEmpty
  private boolean frozen;
  private Long createTimestamp;

  @JsonCreator
  public AssociationValue(
      @JsonProperty("guid") String guid,
      @JsonProperty("tenant") String tenant,
      @JsonProperty("resourceName") String resourceName,
      @JsonProperty("resourceNamespace") String resourceNamespace,
      @JsonProperty("resourceId") String resourceId,
      @JsonProperty("resourceType") String resourceType,
      @JsonProperty("associationType") String associationType,
      @JsonProperty("subject") String subject,
      @JsonProperty("lifecycle") Lifecycle lifecycle,
      @JsonProperty("frozen") boolean frozen,
      @JsonProperty("createTs") Long createTimestamp) {
    super(subject);
    this.guid = guid;
    this.tenant = tenant;
    this.resourceName = resourceName;
    this.resourceNamespace = resourceNamespace;
    this.resourceId = resourceId;
    this.resourceType = resourceType;
    this.associationType = associationType;
    this.lifecycle = lifecycle;
    this.frozen = frozen;
    this.createTimestamp = createTimestamp;
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

  @JsonProperty("associationType")
  public String getAssociationType() {
    return associationType;
  }

  @JsonProperty("associationType")
  public void setAssociationType(String associationType) {
    this.associationType = associationType;
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

  @JsonProperty("createTs")
  public Long getCreateTimestamp() {
    return createTimestamp;
  }

  @JsonProperty("createTs")
  public void setCreateTimestamp(Long createTimestamp) {
    this.createTimestamp = createTimestamp;
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
        && Objects.equals(resourceName, that.resourceName)
        && Objects.equals(resourceNamespace, that.resourceNamespace)
        && Objects.equals(resourceId, that.resourceId)
        && Objects.equals(resourceType, that.resourceType)
        && Objects.equals(associationType, that.associationType)
        && lifecycle == that.lifecycle
        && Objects.equals(createTimestamp, that.createTimestamp);
  }

  @Override
  public int hashCode() {
    return Objects.hash(
        super.hashCode(), guid, tenant, resourceName, resourceNamespace, resourceId,
        resourceType, associationType, lifecycle, frozen, createTimestamp);
  }

  @Override
  public String toString() {
    return "AssociationValue{"
        + "guid='" + guid + '\''
        + ", tenant='" + tenant + '\''
        + ", resourceName='" + resourceName + '\''
        + ", resourceNamespace='" + resourceNamespace + '\''
        + ", resourceId='" + resourceId + '\''
        + ", resourceType='" + resourceType + '\''
        + ", associationType='" + associationType + '\''
        + ", subject='" + getSubject() + '\''
        + ", lifecycle=" + lifecycle
        + ", frozen=" + frozen
        + ", createTs=" + createTimestamp
        + '}';
  }

  @Override
  public AssociationKey toKey() {
    return new AssociationKey(tenant,
        resourceName, resourceNamespace, resourceType, associationType, getSubject());
  }

  public Association toAssociationEntity() {
    QualifiedSubject qs = QualifiedSubject.create(tenant, getSubject());
    return new Association(
        qs.toUnqualifiedSubject(),
        guid,
        resourceName,
        resourceNamespace,
        resourceId,
        resourceType,
        associationType,
        lifecycle == Lifecycle.STRONG
            ? LifecyclePolicy.STRONG
            : LifecyclePolicy.WEAK,
        frozen,
        createTimestamp,
        timestamp
    );
  }

  public static List<AssociationValue> fromAssociationCreateOrUpdateRequest(
      String tenant, AssociationCreateOrUpdateRequest request,
      List<Association> oldAssociations, Set<String> assocTypesToSkip) {
    Map<String, Association> oldMap = oldAssociations.stream()
        .collect(Collectors.toMap(Association::getAssociationType, a -> a));
    return request.getAssociations().stream()
        .filter(info -> !assocTypesToSkip.contains(info.getAssociationType()))
        .map(info -> {
          Association old = oldMap.get(info.getAssociationType());
          String subject = old == null ? info.getSubject() : old.getSubject();
          String qualifiedSubject =
              QualifiedSubject.createFromUnqualified(tenant, subject).toQualifiedSubject();
          return old == null
              ? new AssociationValue(
                  UUID.randomUUID().toString(),
                  tenant,
                  request.getResourceName(),
                  request.getResourceNamespace(),
                  request.getResourceId(),
                  request.getResourceType(),
                  info.getAssociationType(),
                  qualifiedSubject,
                  info.getLifecycle() == LifecyclePolicy.STRONG
                      ? Lifecycle.STRONG
                      : Lifecycle.WEAK,
                  Boolean.TRUE.equals(info.getFrozen()),
                  null)
              : new AssociationValue(
                  old.getGuid(),
                  tenant,
                  old.getResourceName(),
                  old.getResourceNamespace(),
                  old.getResourceId(),
                  old.getResourceType(),
                  info.getAssociationType(),
                  qualifiedSubject,
                  info.getLifecycle() != null
                      ? (info.getLifecycle() == LifecyclePolicy.STRONG
                          ? Lifecycle.STRONG
                          : Lifecycle.WEAK)
                      : (old.getLifecycle() == LifecyclePolicy.STRONG
                          ? Lifecycle.STRONG
                          : Lifecycle.WEAK),
                  info.getFrozen() != null
                      ? info.getFrozen()
                      : old.isFrozen(),
                  null);
        })
        .toList();
  }
}