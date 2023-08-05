/*
 * Copyright 2023 Confluent Inc.
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
