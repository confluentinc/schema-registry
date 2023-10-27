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
import io.confluent.dekregistry.client.rest.entities.Dek;
import io.confluent.kafka.schemaregistry.encryption.tink.DekFormat;
import io.confluent.dekregistry.client.rest.entities.KeyType;
import java.util.Objects;

@JsonInclude(Include.NON_NULL)
@JsonIgnoreProperties(ignoreUnknown = true)
public class DataEncryptionKey extends EncryptionKey {

  private final String kekName;
  private final String subject;
  private final DekFormat algorithm;
  private final int version;
  private final String encryptedKeyMaterial;
  private String keyMaterial;

  @JsonCreator
  public DataEncryptionKey(
      @JsonProperty("kekName") String kekName,
      @JsonProperty("subject") String subject,
      @JsonProperty("algorithm") DekFormat algorithm,
      @JsonProperty("version") int version,
      @JsonProperty("encryptedKeyMaterial") String encryptedKeyMaterial,
      @JsonProperty("deleted") boolean deleted
  ) {
    super(KeyType.DEK, deleted);
    this.kekName = kekName;
    this.subject = subject;
    this.algorithm = algorithm;
    this.version = version;
    this.encryptedKeyMaterial = encryptedKeyMaterial;
  }

  @JsonProperty("kekName")
  public String getKekName() {
    return this.kekName;
  }

  @JsonProperty("subject")
  public String getSubject() {
    return this.subject;
  }

  @JsonProperty("algorithm")
  public DekFormat getAlgorithm() {
    return this.algorithm;
  }

  @JsonProperty("version")
  public int getVersion() {
    return this.version;
  }

  @JsonProperty("encryptedKeyMaterial")
  public String getEncryptedKeyMaterial() {
    return this.encryptedKeyMaterial;
  }

  @JsonIgnore
  public String getKeyMaterial() {
    return this.keyMaterial;
  }

  @JsonIgnore
  public void setKeyMaterial(String keyMaterial) {
    this.keyMaterial = keyMaterial;
  }

  @JsonIgnore
  public boolean isEquivalent(DataEncryptionKey that) {
    return Objects.equals(kekName, that.kekName)
        && Objects.equals(subject, that.subject)
        && algorithm == that.algorithm
        && version == that.version
        && Objects.equals(encryptedKeyMaterial, that.encryptedKeyMaterial);
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
    DataEncryptionKey that = (DataEncryptionKey) o;
    return Objects.equals(kekName, that.kekName)
        && Objects.equals(subject, that.subject)
        && algorithm == that.algorithm
        && version == that.version
        && Objects.equals(encryptedKeyMaterial, that.encryptedKeyMaterial);
  }

  @Override
  public int hashCode() {
    return Objects.hash(
        super.hashCode(), kekName, subject, algorithm, version, encryptedKeyMaterial);
  }

  @Override
  public DataEncryptionKeyId toKey(String tenant) {
    return new DataEncryptionKeyId(tenant, kekName, subject, algorithm, version);
  }

  public Dek toDekEntity() {
    return new Dek(
        kekName, subject, version, algorithm, encryptedKeyMaterial, keyMaterial, timestamp,
        deleted ? true : null);
  }
}
