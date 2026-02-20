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
import com.fasterxml.jackson.annotation.JsonProperty;
import io.confluent.kafka.schemaregistry.client.rest.entities.requests.AssociationCreateOrUpdateInfo;
import io.confluent.kafka.schemaregistry.client.rest.entities.requests.AssociationResponse;
import io.confluent.kafka.schemaregistry.client.rest.entities.requests.AssociationInfo;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;

@JsonInclude(JsonInclude.Include.NON_EMPTY)
@JsonIgnoreProperties(ignoreUnknown = true)
public class Association implements Comparable<Association> {

  private String subject;
  private String guid;
  private String resourceName;
  private String resourceNamespace;
  private String resourceId;
  private String resourceType;
  private String associationType;
  private LifecyclePolicy lifecycle;
  private boolean frozen;
  private Long createTimestamp;
  private Long updateTimestamp;

  public Association(@JsonProperty("subject") String subject,
      @JsonProperty("guid") String guid,
      @JsonProperty("resourceName") String resourceName,
      @JsonProperty("resourceNamespace") String resourceNamespace,
      @JsonProperty("resourceId") String resourceId,
      @JsonProperty("resourceType") String resourceType,
      @JsonProperty("associationType") String associationType,
      @JsonProperty("lifecycle") LifecyclePolicy lifecycle,
      @JsonProperty("frozen") boolean frozen) {
    this(subject, guid, resourceName, resourceNamespace, resourceId, resourceType, associationType,
        lifecycle, frozen, null, null);
  }

  @JsonCreator
  public Association(@JsonProperty("subject") String subject,
      @JsonProperty("guid") String guid,
      @JsonProperty("resourceName") String resourceName,
      @JsonProperty("resourceNamespace") String resourceNamespace,
      @JsonProperty("resourceId") String resourceId,
      @JsonProperty("resourceType") String resourceType,
      @JsonProperty("associationType") String associationType,
      @JsonProperty("lifecycle") LifecyclePolicy lifecycle,
      @JsonProperty("frozen") boolean frozen,
      @JsonProperty("createTs") Long createTimestamp,
      @JsonProperty("updateTs") Long updateTimestamp) {
    this.subject = subject;
    this.guid = guid;
    this.resourceName = resourceName;
    this.resourceNamespace = resourceNamespace;
    this.resourceId = resourceId;
    this.resourceType = resourceType;
    this.associationType = associationType;
    this.lifecycle = lifecycle;
    this.frozen = frozen;
    this.createTimestamp = createTimestamp;
    this.updateTimestamp = updateTimestamp;
  }

  @JsonProperty("subject")
  public String getSubject() {
    return subject;
  }

  @JsonProperty("subject")
  public void setSubject(String subject) {
    this.subject = subject;
  }

  @JsonProperty("guid")
  public String getGuid() {
    return guid;
  }

  @JsonProperty("guid")
  public void setGuid(String guid) {
    this.guid = guid;
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
  public LifecyclePolicy getLifecycle() {
    return lifecycle;
  }

  @JsonProperty("lifecycle")
  public void setLifecycle(LifecyclePolicy lifecycle) {
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

  @JsonProperty("updateTs")
  public Long getUpdateTimestamp() {
    return updateTimestamp;
  }

  @JsonProperty("updateTs")
  public void setUpdateTimestamp(Long updateTimestamp) {
    this.updateTimestamp = updateTimestamp;
  }

  @Override
  public boolean equals(Object o) {
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    Association that = (Association) o;
    return frozen == that.frozen
        && Objects.equals(subject, that.subject)
        && Objects.equals(guid, that.guid)
        && Objects.equals(resourceName, that.resourceName)
        && Objects.equals(resourceNamespace, that.resourceNamespace)
        && Objects.equals(resourceId, that.resourceId)
        && Objects.equals(resourceType, that.resourceType)
        && Objects.equals(associationType, that.associationType)
        && lifecycle == that.lifecycle
        && Objects.equals(createTimestamp, that.createTimestamp)
        && Objects.equals(updateTimestamp, that.updateTimestamp);
  }

  @Override
  public int hashCode() {
    return Objects.hash(
        subject, guid, resourceName, resourceNamespace, resourceId,
        resourceType, associationType, lifecycle, frozen, createTimestamp, updateTimestamp);
  }

  public boolean isEquivalent(AssociationCreateOrUpdateInfo info) {
    return Objects.equals(subject, info.getSubject())
        && Objects.equals(associationType, info.getAssociationType())
        && (info.getLifecycle() == null || Objects.equals(info.getLifecycle(), getLifecycle()))
        && (info.getFrozen() == null || Objects.equals(info.getFrozen(), isFrozen()));
  }

  @Override
  public int compareTo(Association that) {
    if (this.getResourceName() == null && that.getResourceName() == null) {
      // pass
    } else if (this.getResourceName() == null) {
      return -1;
    } else if (that.getResourceName() == null) {
      return 1;
    } else {
      int resourceNameComparison = this.getResourceName().compareTo(that.getResourceName());
      if (resourceNameComparison != 0) {
        return resourceNameComparison < 0 ? -1 : 1;
      }
    }

    if (this.getResourceNamespace() == null && that.getResourceNamespace() == null) {
      // pass
    } else if (this.getResourceNamespace() == null) {
      return -1;
    } else if (that.getResourceNamespace() == null) {
      return 1;
    } else {
      int resourceNamespaceComparison =
          this.getResourceNamespace().compareTo(that.getResourceNamespace());
      if (resourceNamespaceComparison != 0) {
        return resourceNamespaceComparison < 0 ? -1 : 1;
      }
    }

    if (this.getResourceType() == null && that.getResourceType() == null) {
      // pass
    } else if (this.getResourceType() == null) {
      return -1;
    } else if (that.getResourceType() == null) {
      return 1;
    } else {
      int resourceTypeComparison = this.getResourceType().compareTo(that.getResourceType());
      if (resourceTypeComparison != 0) {
        return resourceTypeComparison < 0 ? -1 : 1;
      }
    }

    if (this.getAssociationType() == null && that.getAssociationType() == null) {
      return 0;
    } else if (this.getAssociationType() == null) {
      return -1;
    } else if (that.getAssociationType() == null) {
      return 1;
    } else {
      int assocTypeComparison = this.getAssociationType().compareTo(that.getAssociationType());
      if (assocTypeComparison != 0) {
        return assocTypeComparison < 0 ? -1 : 1;
      }
    }

    if (this.getSubject() == null && that.getSubject() == null) {
      return 0;
    } else if (this.getSubject() == null) {
      return -1;
    } else if (that.getSubject() == null) {
      return 1;
    } else {
      return this.getSubject().compareTo(that.getSubject());
    }
  }

  public static AssociationResponse toAssociationResponse(
      String resourceName,
      String resourceNamespace,
      String resourceId,
      String resourceType,
      List<Association> associations,
      Map<String, Schema> schemas) {
    if (associations == null || associations.isEmpty()) {
      return new AssociationResponse(
          resourceName,
          resourceNamespace,
          resourceId,
          resourceType,
          Collections.emptyList());
    }
    List<AssociationInfo> infos = new ArrayList<>();
    for (Association a1 : associations) {
      // Check all associations have same resourceName, resourceNamespace, resourceId, resourceType
      if (!Objects.equals(a1.getResourceName(), resourceName)
          || !Objects.equals(a1.getResourceNamespace(), resourceNamespace)
          || !Objects.equals(a1.getResourceId(), resourceId)
          || !Objects.equals(a1.getResourceType(), resourceType)) {
        throw new IllegalArgumentException("All associations must have the same resourceName, "
            + "resourceNamespace, resourceId, and resourceType.");
      }
      infos.add(new AssociationInfo(
          a1.getSubject(),
          a1.getAssociationType(),
          a1.getLifecycle(),
          a1.isFrozen(),
          schemas.get(a1.getAssociationType())));
    }
    return new AssociationResponse(
        resourceName,
        resourceNamespace,
        resourceId,
        resourceType,
        infos);
  }
}