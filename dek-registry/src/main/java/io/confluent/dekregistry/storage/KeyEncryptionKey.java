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
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonInclude.Include;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import io.confluent.dekregistry.client.rest.entities.Kek;
import io.confluent.dekregistry.client.rest.entities.KeyType;
import java.util.Collections;
import java.util.Objects;
import java.util.SortedMap;

@JsonInclude(Include.NON_NULL)
@JsonIgnoreProperties(ignoreUnknown = true)
public class KeyEncryptionKey extends EncryptionKey {

  private final String name;
  private final String kmsType;
  private final String kmsKeyId;
  @JsonPropertyOrder(alphabetic = true)
  private final SortedMap<String, String> kmsProps;
  private final String doc;
  private final boolean shared;

  @JsonCreator
  public KeyEncryptionKey(
      @JsonProperty("name") String name,
      @JsonProperty("kmsType") String kmsType,
      @JsonProperty("kmsKeyId") String kmsKeyId,
      @JsonProperty("kmsProps") SortedMap<String, String> kmsProps,
      @JsonProperty("doc") String doc,
      @JsonProperty("shared") boolean shared,
      @JsonProperty("deleted") boolean deleted
  ) {
    super(KeyType.KEK, deleted);
    this.name = name;
    this.kmsType = kmsType;
    this.kmsKeyId = kmsKeyId;
    this.kmsProps = kmsProps != null
        ? Collections.unmodifiableSortedMap(kmsProps)
        : Collections.emptySortedMap();
    this.doc = doc;
    this.shared = shared;
  }

  @JsonProperty("name")
  public String getName() {
    return this.name;
  }

  @JsonProperty("kmsType")
  public String getKmsType() {
    return this.kmsType;
  }

  @JsonProperty("kmsKeyId")
  public String getKmsKeyId() {
    return this.kmsKeyId;
  }

  @JsonProperty("kmsProps")
  public SortedMap<String, String> getKmsProps() {
    return this.kmsProps;
  }

  @JsonProperty("doc")
  public String getDoc() {
    return this.doc;
  }

  @JsonProperty("shared")
  public boolean isShared() {
    return this.shared;
  }

  @JsonIgnore
  public boolean isEquivalent(KeyEncryptionKey that) {
    return shared == that.shared
        && Objects.equals(name, that.name)
        && Objects.equals(kmsType, that.kmsType)
        && Objects.equals(kmsKeyId, that.kmsKeyId)
        && Objects.equals(kmsProps, that.kmsProps)
        && Objects.equals(doc, that.doc);
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
    KeyEncryptionKey that = (KeyEncryptionKey) o;
    return shared == that.shared
        && Objects.equals(name, that.name)
        && Objects.equals(kmsType, that.kmsType)
        && Objects.equals(kmsKeyId, that.kmsKeyId)
        && Objects.equals(kmsProps, that.kmsProps)
        && Objects.equals(doc, that.doc);
  }

  @Override
  public int hashCode() {
    return Objects.hash(super.hashCode(), name, kmsType, kmsKeyId, kmsProps, doc, shared);
  }

  @Override
  public KeyEncryptionKeyId toKey(String tenant) {
    return new KeyEncryptionKeyId(tenant, name);
  }

  public Kek toKekEntity() {
    return new Kek(name, kmsType, kmsKeyId, kmsProps, doc, shared, timestamp,
        deleted ? true : null);
  }
}
