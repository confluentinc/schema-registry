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
import static io.confluent.kafka.schemaregistry.client.rest.utils.RestValidation.checkSubject;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.confluent.kafka.schemaregistry.client.rest.entities.LifecyclePolicy;
import io.confluent.kafka.schemaregistry.client.rest.exceptions.IllegalPropertyException;
import io.confluent.kafka.schemaregistry.utils.JacksonMapper;
import java.io.IOException;
import java.util.List;
import java.util.Objects;

@JsonInclude(JsonInclude.Include.NON_EMPTY)
@JsonIgnoreProperties(ignoreUnknown = true)
public class AssociationCreateRequest {

  private static final String DEFAULT_RESOURCE_TYPE = "topic";
  private static final String DEFAULT_ASSOCIATION_TYPE = "value";
  private static final LifecyclePolicy DEFAULT_LIFECYCLE = LifecyclePolicy.STRONG;

  private String resourceName;
  private String resourceNamespace;
  private String resourceId;
  private String resourceType;
  private List<AssociationCreateInfo> associations;

  @JsonCreator
  public AssociationCreateRequest(
      @JsonProperty("resourceName") String resourceName,
      @JsonProperty("resourceNamespace") String resourceNamespace,
      @JsonProperty("resourceId") String resourceId,
      @JsonProperty("resourceType") String resourceType,
      @JsonProperty("associations") List<AssociationCreateInfo> associations) {
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
  public List<AssociationCreateInfo> getAssociations() {
    return associations;
  }

  @JsonProperty("associations")
  public void setAssociations(List<AssociationCreateInfo> associations) {
    this.associations = associations;
  }

  @Override
  public boolean equals(Object o) {
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    AssociationCreateRequest that = (AssociationCreateRequest) o;
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

  public void validate() {
    checkName(getResourceName(), "resourceName");
    checkName(getResourceNamespace(), "resourceNamespace");
    if (getResourceId() == null || getResourceId().isEmpty()) {
      throw new IllegalPropertyException("resourceId", "cannot be null or empty");
    }
    if (getResourceType() != null && !getResourceType().isEmpty()) {
      checkName(getResourceType(), "resourceType");
    } else {
      setResourceType(DEFAULT_RESOURCE_TYPE);
    }
    for (AssociationCreateInfo info : getAssociations()) {
      checkSubject(info.getSubject());
      if (info.getAssociationType() != null && !info.getAssociationType().isEmpty()) {
        checkName(info.getAssociationType(), "associationType");
      } else {
        info.setAssociationType(DEFAULT_ASSOCIATION_TYPE);
      }
      if (info.getLifecycle() == null) {
        info.setLifecycle(DEFAULT_LIFECYCLE);
      }
    }
  }
}