/*
 * Copyright 2018 Confluent Inc.
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
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;

import java.util.Objects;

@JsonInclude(JsonInclude.Include.NON_EMPTY)
@JsonIgnoreProperties(ignoreUnknown = true)
@JsonPropertyOrder(value = {"keytype", "tenant", "context"})
public class ContextKey extends SchemaRegistryKey {

  private String tenant;
  private String context;

  public ContextKey(@JsonProperty("tenant") String tenant,
                    @JsonProperty("context") String context) {
    super(SchemaRegistryKeyType.CONTEXT);
    this.tenant = tenant;
    this.context = context;
  }

  @JsonProperty("tenant")
  public String getTenant() {
    return this.tenant;
  }

  @JsonProperty("tenant")
  public void setTenant(String tenant) {
    this.tenant = tenant;
  }

  @JsonProperty("context")
  public String getContext() {
    return this.context;
  }

  @JsonProperty("context")
  public void setContext(String context) {
    this.context = context;
  }

  @Override
  public boolean equals(Object o) {
    if (!super.equals(o)) {
      return false;
    }

    ContextKey that = (ContextKey) o;
    return Objects.equals(tenant, that.tenant)
            && Objects.equals(context, that.context);
  }

  @Override
  public int hashCode() {
    return Objects.hash(context, tenant);
  }

  @Override
  public int compareTo(SchemaRegistryKey o) {
    int compare = super.compareTo(o);
    if (compare == 0) {
      ContextKey that = (ContextKey) o;
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

      if (this.getContext() == null && that.getContext() == null) {
        return 0;
      } else if (this.getContext() == null) {
        return -1;
      } else if (that.getContext() == null) {
        return 1;
      } else {
        return this.getContext().compareTo(that.getContext());
      }
    } else {
      return compare;
    }
  }
}
