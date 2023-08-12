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
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonInclude.Include;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.confluent.kafka.schemaregistry.encryption.tink.DekFormat;
import io.confluent.kafka.schemaregistry.utils.JacksonMapper;
import java.io.IOException;
import java.util.Objects;

@JsonInclude(Include.NON_NULL)
@JsonIgnoreProperties(ignoreUnknown = true)
public class Dek {

  private final String kekName;
  private final String subject;
  private final DekFormat algorithm;
  private final String encryptedKeyMaterial;
  private final String keyMaterial;

  @JsonCreator
  public Dek(
      @JsonProperty("kekName") String kekName,
      @JsonProperty("subject") String subject,
      @JsonProperty("algorithm") DekFormat algorithm,
      @JsonProperty("encryptedKeyMaterial") String encryptedKeyMaterial,
      @JsonProperty("keyMaterial") String keyMaterial
  ) {
    this.kekName = kekName;
    this.subject = subject;
    this.algorithm = algorithm;
    this.encryptedKeyMaterial = encryptedKeyMaterial;
    this.keyMaterial = keyMaterial;
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

  @JsonProperty("encryptedKeyMaterial")
  public String getEncryptedKeyMaterial() {
    return this.encryptedKeyMaterial;
  }

  @JsonProperty("keyMaterial")
  public String getKeyMaterial() {
    return this.keyMaterial;
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
    return Objects.equals(kekName, dek.kekName)
        && Objects.equals(subject, dek.subject)
        && Objects.equals(algorithm, dek.algorithm)
        && Objects.equals(encryptedKeyMaterial, dek.encryptedKeyMaterial)
        && Objects.equals(keyMaterial, dek.keyMaterial);
  }

  @Override
  public int hashCode() {
    return Objects.hash(kekName, subject, algorithm, encryptedKeyMaterial, keyMaterial);
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
