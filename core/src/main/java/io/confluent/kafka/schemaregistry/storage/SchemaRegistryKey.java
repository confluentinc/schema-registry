/*
 * Copyright 2014-2025 Confluent Inc.
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

import javax.validation.constraints.NotEmpty;

import javax.validation.constraints.Min;

@JsonInclude(Include.NON_EMPTY)
@JsonIgnoreProperties(ignoreUnknown = true)
public abstract class SchemaRegistryKey implements Comparable<SchemaRegistryKey> {

  @Min(0)
  protected int magicByte;
  @NotEmpty
  protected SchemaRegistryKeyType keyType;

  public SchemaRegistryKey(@JsonProperty("keytype") SchemaRegistryKeyType keyType) {
    this.keyType = keyType;
  }

  @JsonProperty("magic")
  public int getMagicByte() {
    return this.magicByte;
  }

  @JsonProperty("magic")
  public void setMagicByte(int magicByte) {
    this.magicByte = magicByte;
  }

  @JsonProperty("keytype")
  public SchemaRegistryKeyType getKeyType() {
    return this.keyType;
  }

  @JsonProperty("keytype")
  public void setKeyType(SchemaRegistryKeyType keyType) {
    this.keyType = keyType;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    SchemaRegistryKey that = (SchemaRegistryKey) o;

    if (this.magicByte != that.magicByte) {
      return false;
    }
    if (!this.keyType.equals(that.keyType)) {
      return false;
    }
    return true;
  }

  @Override
  public int hashCode() {
    int result = 31 * this.magicByte;
    result = 31 * result + this.keyType.hashCode();
    return result;
  }

  @Override
  public int compareTo(SchemaRegistryKey otherKey) {
    return this.keyType.compareTo(otherKey.keyType);
  }
}
