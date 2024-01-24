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
public class CreateDekRequest {

  private String subject;
  private Integer version;
  private DekFormat algorithm;
  private String encryptedKeyMaterial;
  private boolean deleted;

  public static CreateDekRequest fromJson(String json) throws IOException {
    return JacksonMapper.INSTANCE.readValue(json, CreateDekRequest.class);
  }

  @JsonProperty("subject")
  public String getSubject() {
    return this.subject;
  }

  @JsonProperty("subject")
  public void setSubject(String subject) {
    this.subject = subject;
  }

  @JsonProperty("version")
  public Integer getVersion() {
    return this.version;
  }

  @JsonProperty("version")
  public void setVersion(Integer version) {
    this.version = version;
  }

  @JsonProperty("algorithm")
  public DekFormat getAlgorithm() {
    return this.algorithm;
  }

  @JsonProperty("algorithm")
  public void setAlgorithm(DekFormat algorithm) {
    this.algorithm = algorithm;
  }

  @JsonProperty("encryptedKeyMaterial")
  public String getEncryptedKeyMaterial() {
    return this.encryptedKeyMaterial;
  }

  @JsonProperty("encryptedKeyMaterial")
  public void setEncryptedKeyMaterial(String encryptedKeyMaterial) {
    this.encryptedKeyMaterial = encryptedKeyMaterial;
  }

  @JsonProperty("deleted")
  public boolean isDeleted() {
    return this.deleted;
  }

  @JsonProperty("deleted")
  public void setDeleted(boolean deleted) {
    this.deleted = deleted;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    CreateDekRequest dek = (CreateDekRequest) o;
    return Objects.equals(subject, dek.subject)
        && Objects.equals(version, dek.version)
        && Objects.equals(algorithm, dek.algorithm)
        && Objects.equals(encryptedKeyMaterial, dek.encryptedKeyMaterial)
        && deleted == dek.deleted;
  }

  @Override
  public int hashCode() {
    return Objects.hash(subject, version, algorithm, encryptedKeyMaterial, deleted);
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
