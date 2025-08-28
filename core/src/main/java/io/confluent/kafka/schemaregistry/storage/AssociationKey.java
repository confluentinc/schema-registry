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

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonInclude.Include;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import jakarta.validation.constraints.NotEmpty;
import java.util.Objects;

@JsonInclude(Include.NON_EMPTY)
@JsonIgnoreProperties(ignoreUnknown = true)
@JsonPropertyOrder(value = {"keytype", "tenant", "associationType", "resourceType",
    "resourceName", "resourceNamespace"})
public class AssociationKey extends SchemaRegistryKey {

  private static final int MAGIC_BYTE = 0;
  @NotEmpty
  private String tenant;
  private String associationType;
  private String resourceType;
  private String resourceName;
  private String resourceNamespace;

  public AssociationKey(@JsonProperty("tenant") String tenant,
      @JsonProperty("associationType") String associationType,
      @JsonProperty("resourceType") String resourceType,
      @JsonProperty("resourceName") String resourceName,
      @JsonProperty("resourceNamespace") String resourceNamespace) {
    super(SchemaRegistryKeyType.ASSOC);
    this.magicByte = MAGIC_BYTE;
  }

  public String getTenant() {
    return tenant;
  }

  public void setTenant(String tenant) {
    this.tenant = tenant;
  }

  public String getAssociationType() {
    return associationType;
  }

  public void setAssociationType(String associationType) {
    this.associationType = associationType;
  }

  public String getResourceType() {
    return resourceType;
  }

  public void setResourceType(String resourceType) {
    this.resourceType = resourceType;
  }

  public String getResourceName() {
    return resourceName;
  }

  public void setResourceName(String resourceName) {
    this.resourceName = resourceName;
  }

  public String getResourceNamespace() {
    return resourceNamespace;
  }

  public void setResourceNamespace(String resourceNamespace) {
    this.resourceNamespace = resourceNamespace;
  }

  @Override
  public boolean equals(Object o) {
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    if (!super.equals(o)) {
      return false;
    }
    AssociationKey that = (AssociationKey) o;
    return Objects.equals(tenant, that.tenant)
        && Objects.equals(associationType, that.associationType)
        && Objects.equals(resourceType, that.resourceType)
        && Objects.equals(resourceName, that.resourceName)
        && Objects.equals(resourceNamespace, that.resourceNamespace);
  }

  @Override
  public int hashCode() {
    return Objects.hash(super.hashCode(), tenant, associationType, resourceType,
        resourceName, resourceNamespace);
  }

  @Override
  public String toString() {
    return "AssociationKey{"
        + "tenant='" + tenant + '\''
        + ", associationType='" + associationType + '\''
        + ", resourceType='" + resourceType + '\''
        + ", resourceName='" + resourceName + '\''
        + ", resourceNamespace='" + resourceNamespace + '\''
        + '}';
  }
}
