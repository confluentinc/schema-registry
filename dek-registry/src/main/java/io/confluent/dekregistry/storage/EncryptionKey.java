/*
 * Copyright 2023 Confluent Inc.
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

  private final KeyType type;
  private final boolean deleted;

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
}
