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
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonInclude.Include;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.confluent.dekregistry.client.rest.entities.KeyType;
import java.util.Objects;

@JsonInclude(Include.NON_NULL)
@JsonIgnoreProperties(ignoreUnknown = true)
public class KeyEncryptionKeyId extends EncryptionKeyId {

  private final String name;

  @JsonCreator
  public KeyEncryptionKeyId(
      @JsonProperty("tenant") String tenant,
      @JsonProperty("name") String name
  ) {
    super(tenant, KeyType.KEK);
    this.name = name;
  }

  @JsonProperty("name")
  public String getName() {
    return this.name;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    if (!super.equals(o)) {
      return false;
    }
    KeyEncryptionKeyId that = (KeyEncryptionKeyId) o;
    return Objects.equals(name, that.name);
  }

  @Override
  public int hashCode() {
    return Objects.hash(super.hashCode(), name);
  }

  @Override
  public int compareTo(EncryptionKeyId o) {
    int compare = super.compareTo(o);
    if (compare != 0) {
      return compare;
    }

    KeyEncryptionKeyId that = (KeyEncryptionKeyId) o;
    if (this.getName() == null && that.getName() == null) {
      return 0;
    } else if (this.getName() == null) {
      return -1;
    } else if (that.getName() == null) {
      return 1;
    } else {
      return this.getName().compareTo(that.getName());
    }
  }
}
