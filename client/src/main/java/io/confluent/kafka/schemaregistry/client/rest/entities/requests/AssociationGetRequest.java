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
import io.confluent.kafka.schemaregistry.client.rest.entities.LifecyclePolicy;
import io.confluent.kafka.schemaregistry.client.rest.exceptions.IllegalPropertyException;
import io.confluent.kafka.schemaregistry.utils.JacksonMapper;
import java.io.IOException;
import java.util.List;
import java.util.Objects;

@JsonInclude(JsonInclude.Include.NON_EMPTY)
@JsonIgnoreProperties(ignoreUnknown = true)
public class AssociationGetRequest {

  private String resourceName;
  private String resourceNamespace;
  private String resourceId;
  private String resourceType;
  private List<String> associationTypes;
  private LifecyclePolicy lifecycle;

  public AssociationGetRequest(
      String resourceId,
      String resourceType,
      List<String> associationTypes,
      LifecyclePolicy lifecycle) {
    this(null, null, resourceId, resourceType, associationTypes, lifecycle);
  }

  public AssociationGetRequest(
      String resourceName,
      String resourceNamespace,
      String resourceType,
      List<String> associationTypes,
      LifecyclePolicy lifecycle) {
    this(resourceName, resourceNamespace, null, resourceType, associationTypes, lifecycle);
  }

  @JsonCreator
  public AssociationGetRequest(
      @JsonProperty("resourceName") String resourceName,
      @JsonProperty("resourceNamespace") String resourceNamespace,
      @JsonProperty("resourceId") String resourceId,
      @JsonProperty("resourceType") String resourceType,
      @JsonProperty("associationTypes") List<String> associationTypes,
      @JsonProperty("lifecycle") LifecyclePolicy lifecycle) {
    this.resourceName = resourceName;
    this.resourceNamespace = resourceNamespace;
    this.resourceId = resourceId;
    this.resourceType = resourceType;
    this.associationTypes = associationTypes;
    this.lifecycle = lifecycle;
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

  @JsonProperty("associationTypes")
  public List<String> getAssociationTypes() {
    return associationTypes;
  }

  @JsonProperty("associationTypes")
  public void setAssociationTypes(List<String> associationTypes) {
    this.associationTypes = associationTypes;
  }

  @JsonProperty("lifecycle")
  public LifecyclePolicy getLifecycle() {
    return lifecycle;
  }

  @JsonProperty("lifecycle")
  public void setLifecycle(LifecyclePolicy lifecycle) {
    this.lifecycle = lifecycle;
  }

  @Override
  public boolean equals(Object o) {
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    AssociationGetRequest that = (AssociationGetRequest) o;
    return Objects.equals(resourceName, that.resourceName)
        && Objects.equals(resourceNamespace, that.resourceNamespace)
        && Objects.equals(resourceId, that.resourceId)
        && Objects.equals(resourceType, that.resourceType)
        && Objects.equals(associationTypes, that.associationTypes)
        && lifecycle == that.lifecycle;
  }

  @Override
  public int hashCode() {
    return Objects.hash(
        resourceName, resourceNamespace, resourceId, resourceType, associationTypes, lifecycle);
  }

  public void validate() {
    boolean hasResourceId = resourceId != null && !resourceId.isEmpty();
    boolean hasResourceName = resourceName != null && !resourceName.isEmpty();
    if (!hasResourceId && !hasResourceName) {
      throw new IllegalPropertyException(
          "resourceId", "either resourceId or resourceName must be specified");
    }
    if (hasResourceName && (resourceNamespace == null || resourceNamespace.isEmpty())) {
      throw new IllegalPropertyException(
          "resourceNamespace", "resourceNamespace is required when resourceName is specified");
    }
  }

  public String toJson() throws IOException {
    return JacksonMapper.INSTANCE.writeValueAsString(this);
  }
}
