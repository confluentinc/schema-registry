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
    @JsonSubTypes.Type(value = DataEncryptionKey.class, name = "DEK"),
    @JsonSubTypes.Type(value = KeyEncryptionKey.class, name = "KEK")
})
public abstract class EncryptionKey {

  protected final KeyType type;
  protected final boolean deleted;
  protected Long offset;
  protected Long timestamp;

  @JsonCreator
  public EncryptionKey(
      @JsonProperty("type") KeyType type,
      @JsonProperty("deleted") boolean deleted
  ) {
    this.type = type;
    this.deleted = deleted;
  }

  @JsonProperty("type")
  public KeyType getType() {
    return this.type;
  }

  @JsonProperty("deleted")
  public boolean isDeleted() {
    return this.deleted;
  }

  @JsonProperty("offset")
  public Long getOffset() {
    return this.offset;
  }

  @JsonProperty("offset")
  public void setOffset(Long offset) {
    this.offset = offset;
  }

  @JsonProperty("ts")
  public Long getTimestamp() {
    return this.timestamp;
  }

  @JsonProperty("ts")
  public void setTimestamp(Long timestamp) {
    this.timestamp = timestamp;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    EncryptionKey that = (EncryptionKey) o;
    return deleted == that.deleted
        && type == that.type;
  }

  @Override
  public int hashCode() {
    return Objects.hash(type, deleted);
  }

  public abstract EncryptionKeyId toKey(String tenant);
}
