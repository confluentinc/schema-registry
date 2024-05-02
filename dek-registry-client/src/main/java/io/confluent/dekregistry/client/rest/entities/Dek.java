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

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonInclude.Include;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.confluent.kafka.schemaregistry.encryption.tink.DekFormat;
import io.confluent.kafka.schemaregistry.utils.JacksonMapper;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.Objects;

@JsonInclude(Include.NON_NULL)
@JsonIgnoreProperties(ignoreUnknown = true)
@io.swagger.v3.oas.annotations.media.Schema(description = "Dek")
public class Dek {

  private final String kekName;
  private final String subject;
  private final int version;
  private final DekFormat algorithm;
  private final String encryptedKeyMaterial;
  private String keyMaterial;
  private final Long timestamp;
  private final Boolean deleted;

  private byte[] encryptedKeyMaterialBytes;
  private byte[] keyMaterialBytes;

  @JsonCreator
  public Dek(
      @JsonProperty("kekName") String kekName,
      @JsonProperty("subject") String subject,
      @JsonProperty("version") int version,
      @JsonProperty("algorithm") DekFormat algorithm,
      @JsonProperty("encryptedKeyMaterial") String encryptedKeyMaterial,
      @JsonProperty("keyMaterial") String keyMaterial,
      @JsonProperty("ts") Long timestamp,
      @JsonProperty("deleted") Boolean deleted
  ) {
    this.kekName = kekName;
    this.subject = subject;
    this.version = version;
    this.algorithm = algorithm;
    this.encryptedKeyMaterial = encryptedKeyMaterial;
    this.keyMaterial = keyMaterial;
    this.timestamp = timestamp;
    this.deleted = deleted;
  }

  @io.swagger.v3.oas.annotations.media.Schema(description = "Kek name of the dek")
  @JsonProperty("kekName")
  public String getKekName() {
    return this.kekName;
  }

  @io.swagger.v3.oas.annotations.media.Schema(description = "Subject of the dek")
  @JsonProperty("subject")
  public String getSubject() {
    return this.subject;
  }

  @io.swagger.v3.oas.annotations.media.Schema(description = "Version of the dek")
  @JsonProperty("version")
  public int getVersion() {
    return this.version;
  }

  @io.swagger.v3.oas.annotations.media.Schema(description = "Algorithm of the dek")
  @JsonProperty("algorithm")
  public DekFormat getAlgorithm() {
    return this.algorithm;
  }

  @io.swagger.v3.oas.annotations.media.Schema(description = "Encrypted key material of the dek")
  @JsonProperty("encryptedKeyMaterial")
  public String getEncryptedKeyMaterial() {
    return this.encryptedKeyMaterial;
  }

  @io.swagger.v3.oas.annotations.media.Schema(description = "Raw key material of the dek")
  @JsonProperty("keyMaterial")
  public String getKeyMaterial() {
    return this.keyMaterial;
  }

  @JsonProperty("keyMaterial")
  public void setKeyMaterial(byte[] keyMaterialBytes) {
    this.keyMaterial = keyMaterialBytes != null
        ? new String(Base64.getEncoder().encode(keyMaterialBytes), StandardCharsets.UTF_8)
        : null;
  }

  @io.swagger.v3.oas.annotations.media.Schema(description = "Timestamp of the dek")
  @JsonProperty("ts")
  public Long getTimestamp() {
    return this.timestamp;
  }

  @io.swagger.v3.oas.annotations.media.Schema(description = "Whether the dek is deleted")
  @JsonProperty("deleted")
  public Boolean getDeleted() {
    return this.deleted;
  }

  @JsonIgnore
  public boolean isDeleted() {
    return Boolean.TRUE.equals(this.deleted);
  }

  @JsonIgnore
  public byte[] getEncryptedKeyMaterialBytes() {
    if (encryptedKeyMaterial == null) {
      return null;
    }
    if (encryptedKeyMaterialBytes == null) {
      encryptedKeyMaterialBytes =
          Base64.getDecoder().decode(encryptedKeyMaterial.getBytes(StandardCharsets.UTF_8));
    }
    return encryptedKeyMaterialBytes;
  }

  @JsonIgnore
  public byte[] getKeyMaterialBytes() {
    if (keyMaterial == null) {
      return null;
    }
    if (keyMaterialBytes == null) {
      keyMaterialBytes =
          Base64.getDecoder().decode(keyMaterial.getBytes(StandardCharsets.UTF_8));
    }
    return keyMaterialBytes;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    Dek dek = (Dek) o;
    return version == dek.version
        && Objects.equals(kekName, dek.kekName)
        && Objects.equals(subject, dek.subject)
        && algorithm == dek.algorithm
        && Objects.equals(encryptedKeyMaterial, dek.encryptedKeyMaterial)
        && Objects.equals(keyMaterial, dek.keyMaterial);
  }

  @Override
  public int hashCode() {
    return Objects.hash(kekName, subject, version, algorithm, encryptedKeyMaterial, keyMaterial);
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
