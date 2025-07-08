/*
 * Copyright 2023 Confluent Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.confluent.dekregistry.client.rest.entities;

import static io.confluent.kafka.schemaregistry.encryption.tink.KmsDriver.KMS_TYPE_SUFFIX;
import static io.confluent.kafka.schemaregistry.encryption.tink.KmsDriver.TEST_CLIENT;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonInclude.Include;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import com.google.crypto.tink.Aead;
import com.google.crypto.tink.KmsClient;
import io.confluent.kafka.schemaregistry.encryption.tink.KmsDriverManager;
import io.confluent.kafka.schemaregistry.utils.JacksonMapper;
import java.io.IOException;
import java.security.GeneralSecurityException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.SortedMap;
import java.util.TreeMap;

@JsonInclude(Include.NON_NULL)
@JsonIgnoreProperties(ignoreUnknown = true)
@io.swagger.v3.oas.annotations.media.Schema(description = "Kek")
public class Kek {

  private final String name;
  private final String kmsType;
  private final String kmsKeyId;
  @JsonPropertyOrder(alphabetic = true)
  private final SortedMap<String, String> kmsProps;
  private final String doc;
  private final boolean shared;
  private final Long timestamp;
  private final Boolean deleted;

  @JsonCreator
  public Kek(
      @JsonProperty("name") String name,
      @JsonProperty("kmsType") String kmsType,
      @JsonProperty("kmsKeyId") String kmsKeyId,
      @JsonProperty("kmsProps") Map<String, String> kmsProps,
      @JsonProperty("doc") String doc,
      @JsonProperty("shared") boolean shared,
      @JsonProperty("ts") Long timestamp,
      @JsonProperty("deleted") Boolean deleted
  ) {
    this.name = name;
    this.kmsType = kmsType;
    this.kmsKeyId = kmsKeyId;
    SortedMap<String, String> sortedKmsProps = kmsProps != null
        ? new TreeMap<>(kmsProps)
        : Collections.emptySortedMap();
    this.kmsProps = Collections.unmodifiableSortedMap(sortedKmsProps);
    this.doc = doc;
    this.shared = shared;
    this.timestamp = timestamp;
    this.deleted = deleted;
  }

  @io.swagger.v3.oas.annotations.media.Schema(description = "Name of the kek")
  @JsonProperty("name")
  public String getName() {
    return this.name;
  }

  @io.swagger.v3.oas.annotations.media.Schema(description = "KMS type of the kek")
  @JsonProperty("kmsType")
  public String getKmsType() {
    return this.kmsType;
  }

  @io.swagger.v3.oas.annotations.media.Schema(description = "KMS key ID of the kek")
  @JsonProperty("kmsKeyId")
  public String getKmsKeyId() {
    return this.kmsKeyId;
  }

  @io.swagger.v3.oas.annotations.media.Schema(description = "Properties of the kek")
  @JsonProperty("kmsProps")
  public SortedMap<String, String> getKmsProps() {
    return this.kmsProps;
  }

  @io.swagger.v3.oas.annotations.media.Schema(description = "Description of the kek")
  @JsonProperty("doc")
  public String getDoc() {
    return this.doc;
  }

  @io.swagger.v3.oas.annotations.media.Schema(description = "Whether the kek is shared")
  @JsonProperty("shared")
  public boolean isShared() {
    return this.shared;
  }

  @io.swagger.v3.oas.annotations.media.Schema(description = "Timestamp of the kek")
  @JsonProperty("ts")
  public Long getTimestamp() {
    return this.timestamp;
  }

  @io.swagger.v3.oas.annotations.media.Schema(description = "Whether the kek is deleted")
  @JsonProperty("deleted")
  public Boolean getDeleted() {
    return this.deleted;
  }

  @JsonIgnore
  public boolean isDeleted() {
    return Boolean.TRUE.equals(this.deleted);
  }

  @JsonIgnore
  public Aead toAead(Map<String, ?> configs) throws GeneralSecurityException {
    String kekUrl = getKmsType() + KMS_TYPE_SUFFIX + getKmsKeyId();
    Map<String, Object> props = new HashMap<>(getKmsProps());
    if (configs.containsKey(TEST_CLIENT)) {
      props.put(TEST_CLIENT, configs.get(TEST_CLIENT));
    }
    KmsClient kmsClient = getKmsClient(props, kekUrl);
    if (kmsClient == null) {
      throw new GeneralSecurityException("No kms client found for " + kekUrl);
    }
    return kmsClient.getAead(kekUrl);
  }

  private static KmsClient getKmsClient(Map<String, ?> configs, String kekUrl)
      throws GeneralSecurityException {
    try {
      return KmsDriverManager.getDriver(kekUrl).getKmsClient(kekUrl);
    } catch (GeneralSecurityException e) {
      return KmsDriverManager.getDriver(kekUrl).registerKmsClient(configs, Optional.of(kekUrl));
    }
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    Kek kek = (Kek) o;
    return shared == kek.shared
        && Objects.equals(name, kek.name)
        && Objects.equals(kmsType, kek.kmsType)
        && Objects.equals(kmsKeyId, kek.kmsKeyId)
        && Objects.equals(kmsProps, kek.kmsProps)
        && Objects.equals(doc, kek.doc);
  }

  @Override
  public int hashCode() {
    return Objects.hash(name, kmsType, kmsKeyId, kmsProps, doc, shared);
  }

  @Override
  public String toString() {
    try {
      return toJson();
    } catch (IOException e) {
      throw new IllegalStateException(e);
    }
  }

  public String toJson() throws IOException {
    return JacksonMapper.INSTANCE.writeValueAsString(this);
  }
}
