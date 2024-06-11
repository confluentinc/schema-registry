/*
 * Copyright 2023 Confluent Inc.
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

package io.confluent.dekregistry.storage;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.fasterxml.jackson.annotation.JsonTypeInfo.As;
import io.confluent.dekregistry.client.rest.entities.KeyType;
import java.util.Objects;

@JsonTypeInfo(
    use = JsonTypeInfo.Id.NAME,
    include = As.EXISTING_PROPERTY,
    property = "type"
)
@JsonSubTypes({
    @JsonSubTypes.Type(value = DataEncryptionKeyId.class, name = "DEK"),
    @JsonSubTypes.Type(value = KeyEncryptionKeyId.class, name = "KEK")
})
public abstract class EncryptionKeyId implements Comparable<EncryptionKeyId> {

  private final String tenant;
  private final KeyType type;

  @JsonCreator
  public EncryptionKeyId(
      @JsonProperty("tenant") String tenant,
      @JsonProperty("type") KeyType type
  ) {
    this.tenant = tenant;
    this.type = type;
  }

  @JsonProperty("tenant")
  public String getTenant() {
    return this.tenant;
  }

  @JsonProperty("type")
  public KeyType getType() {
    return this.type;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    EncryptionKeyId that = (EncryptionKeyId) o;
    return Objects.equals(tenant, that.tenant)
        && type == that.type;
  }

  @Override
  public int hashCode() {
    return Objects.hash(tenant, type);
  }


  @Override
  public int compareTo(EncryptionKeyId that) {
    if (this.tenant == null && that.tenant == null) {
      // pass
    } else if (this.tenant == null) {
      return -1;
    } else if (that.tenant == null) {
      return 1;
    } else {
      int tenantComparison = this.tenant.compareTo(that.tenant);
      if (tenantComparison != 0) {
        return tenantComparison < 0 ? -1 : 1;
      }
    }

    if (this.type == null && that.type == null) {
      return 0;
    } else if (this.type == null) {
      return -1;
    } else if (that.type == null) {
      return 1;
    } else {
      return this.type.compareTo(that.type);
    }
  }
}
