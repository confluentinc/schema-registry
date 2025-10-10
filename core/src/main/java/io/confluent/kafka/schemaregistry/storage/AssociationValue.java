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
import io.confluent.kafka.schemaregistry.client.rest.entities.Schema;
import io.confluent.kafka.schemaregistry.client.rest.entities.requests.AssociationCreateRequest;
import io.confluent.kafka.schemaregistry.client.rest.entities.requests.AssociationInfo;
import io.confluent.kafka.schemaregistry.client.rest.entities.requests.AssociationResponse;
import io.confluent.kafka.schemaregistry.client.rest.entities.requests.AssociationUpdateRequest;
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

  @JsonCreator
  public AssociationValue(@JsonProperty("subject") String subject,
      @JsonProperty("guid") String guid,
      @JsonProperty("tenant") String tenant,
      @JsonProperty("resourceName") String resourceName,
      @JsonProperty("resourceNamespace") String resourceNamespace,
      @JsonProperty("resourceId") String resourceId,
      @JsonProperty("resourceType") String resourceType,
      @JsonProperty("associationType") String associationType,
      @JsonProperty("lifecycle") Lifecycle lifecycle,
      @JsonProperty("frozen") boolean frozen) {
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
        && lifecycle == that.lifecycle;
  }

  @Override
  public int hashCode() {
    return Objects.hash(
        super.hashCode(), guid, tenant, resourceName, resourceNamespace, resourceId,
        resourceType, associationType, lifecycle, frozen);
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
        + ", lifecycle=" + lifecycle
        + ", frozen=" + frozen
        + '}';
  }

  @Override
  public AssociationKey toKey() {
    return new AssociationKey(tenant,
        resourceName, resourceNamespace, resourceType, associationType);
  }

  public Association toAssociationEntity() {
    return new Association(
        getSubject(),
        guid,
        resourceName,
        resourceNamespace,
        resourceId,
        resourceType,
        associationType,
        lifecycle == Lifecycle.STRONG
            ? LifecyclePolicy.STRONG
            : LifecyclePolicy.WEAK,
        frozen
    );
  }

  public static List<AssociationValue> fromAssociationCreateRequest(
      String tenant, AssociationCreateRequest request, Set<String> assocTypesToSkip) {
    return request.getAssociations().stream()
        .filter(info -> !assocTypesToSkip.contains(info.getAssociationType()))
        .map(info -> new AssociationValue(
            info.getSubject(),
            UUID.randomUUID().toString(),
            tenant,
            request.getResourceName(),
            request.getResourceNamespace(),
            request.getResourceId(),
            request.getResourceType(),
            info.getAssociationType(),
            info.getLifecycle() == LifecyclePolicy.STRONG
                ? Lifecycle.STRONG
                : Lifecycle.WEAK,
            info.isFrozen()))
        .toList();
  }

  public static List<AssociationValue> fromAssociationUpdateRequest(
      String tenant, AssociationUpdateRequest request,
      List<Association> oldAssociations, Set<String> assocTypesToSkip) {
    Map<String, Association> oldMap = oldAssociations.stream()
        .collect(Collectors.toMap(Association::getAssociationType, a -> a));
    return request.getAssociations().stream()
        .filter(info -> !assocTypesToSkip.contains(info.getAssociationType()))
        .map(info -> {
          Association old = oldMap.get(info.getAssociationType());
          if (old == null) {
            throw new IllegalArgumentException("Association type " + info.getAssociationType()
                + " does not exist in the existing associations.");
          }
          return new AssociationValue(
              old.getSubject(),
              old.getGuid(),
              tenant,
              old.getResourceName(),
              old.getResourceNamespace(),
              old.getResourceId(),
              old.getResourceType(),
              info.getAssociationType(),
              info.getLifecycle().isPresent()
                  ? (info.getLifecycle().get() == LifecyclePolicy.STRONG
                      ? Lifecycle.STRONG
                      : Lifecycle.WEAK)
                  : old.getLifecycle() == LifecyclePolicy.STRONG
                      ? Lifecycle.STRONG
                      : Lifecycle.WEAK,
              info.getFrozen().isPresent()
                  ? info.getFrozen().get()
                  : old.isFrozen());
        })
        .toList();
  }

  public static AssociationResponse toAssociationResponse(
      List<AssociationValue> associations, Map<String, Schema> schemas) {
    // Check all associations have same resourceName, resourceNamespace, resourceId, resourceType
    for (int i = 1; i < associations.size(); i++) {
      AssociationValue a1 = associations.get(i - 1);
      AssociationValue a2 = associations.get(i);
      if (!Objects.equals(a1.getResourceName(), a2.getResourceName())
          || !Objects.equals(a1.getResourceNamespace(), a2.getResourceNamespace())
          || !Objects.equals(a1.getResourceId(), a2.getResourceId())
          || !Objects.equals(a1.getResourceType(), a2.getResourceType())) {
        throw new IllegalArgumentException("All associations must have the same resourceName, "
            + "resourceNamespace, resourceId, and resourceType.");
      }
    }
    List<AssociationInfo> infos = associations.stream()
        .map(a -> new AssociationInfo(
            a.getSubject(),
            a.getAssociationType(),
            a.getLifecycle() == Lifecycle.STRONG
                ? LifecyclePolicy.STRONG
                : LifecyclePolicy.WEAK,
            a.isFrozen(),
            schemas.get(a.associationType)))
        .toList();
    return new AssociationResponse(
        associations.get(0).getResourceName(),
        associations.get(0).getResourceNamespace(),
        associations.get(0).getResourceId(),
        associations.get(0).getResourceType(),
        infos);

  }
}