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
@JsonPropertyOrder(value = {"keytype", "tenant", "resourceName", "resourceNamespace",
    "resourceType", "associationType"})
public class AssociationKey extends SchemaRegistryKey {

  private static final int MAGIC_BYTE = 0;
  @NotEmpty
  private String tenant;
  private String resourceName;
  private String resourceNamespace;
  private String resourceType;
  private String associationType;

  public AssociationKey(@JsonProperty("tenant") String tenant,
      @JsonProperty("resourceName") String resourceName,
      @JsonProperty("resourceNamespace") String resourceNamespace,
      @JsonProperty("resourceType") String resourceType,
      @JsonProperty("associationType") String associationType) {
    super(SchemaRegistryKeyType.ASSOC);
    this.magicByte = MAGIC_BYTE;
    this.tenant = tenant;
    this.resourceName = resourceName;
    this.resourceNamespace = resourceNamespace;
    this.resourceType = resourceType;
    this.associationType = associationType;
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
        && Objects.equals(resourceName, that.resourceName)
        && Objects.equals(resourceNamespace, that.resourceNamespace)
        && Objects.equals(resourceType, that.resourceType)
        && Objects.equals(associationType, that.associationType);
  }

  @Override
  public int hashCode() {
    return Objects.hash(super.hashCode(), tenant,
        resourceName, resourceNamespace, resourceType, associationType);
  }

  @Override
  public String toString() {
    return "AssociationKey{"
        + "tenant='" + tenant + '\''
        + ", resourceName='" + resourceName + '\''
        + ", resourceNamespace='" + resourceNamespace + '\''
        + ", resourceType='" + resourceType + '\''
        + ", associationType='" + associationType + '\''
        + '}';
  }


  @Override
  public int compareTo(SchemaRegistryKey o) {
    int compare = super.compareTo(o);
    if (compare == 0) {
      AssociationKey that = (AssociationKey) o;
      if (this.getTenant() == null && that.getTenant() == null) {
        // pass
      } else if (this.getTenant() == null) {
        return -1;
      } else if (that.getTenant() == null) {
        return 1;
      } else {
        int tenantComparison = this.getTenant().compareTo(that.getTenant());
        if (tenantComparison != 0) {
          return tenantComparison < 0 ? -1 : 1;
        }
      }

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
        return this.getAssociationType().compareTo(that.getAssociationType());
      }
    } else {
      return compare;
    }
  }
}
