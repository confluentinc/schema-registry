/*
 * Copyright 2021 Confluent Inc.
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

import java.util.Objects;

@JsonInclude(JsonInclude.Include.NON_EMPTY)
@JsonIgnoreProperties(ignoreUnknown = true)
public class ContextValue extends SchemaRegistryValue {

  private String tenant;
  private String context;

  public ContextValue(@JsonProperty("tenant") String tenant,
                      @JsonProperty("context") String context) {
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
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    ContextValue that = (ContextValue) o;
    return Objects.equals(tenant, that.tenant)
            && Objects.equals(context, that.context);
  }

  @Override
  public int hashCode() {
    return Objects.hash(tenant, context);
  }
}
