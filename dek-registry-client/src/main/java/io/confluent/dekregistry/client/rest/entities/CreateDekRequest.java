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

  private String scope;
  private DekFormat algorithm;
  private String encryptedKeyMaterial;

  public static CreateDekRequest fromJson(String json) throws IOException {
    return JacksonMapper.INSTANCE.readValue(json, CreateDekRequest.class);
  }

  @JsonProperty("scope")
  public String getScope() {
    return this.scope;
  }

  @JsonProperty("scope")
  public void setScope(String scope) {
    this.scope = scope;
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

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    CreateDekRequest dek = (CreateDekRequest) o;
    return Objects.equals(scope, dek.scope)
        && Objects.equals(algorithm, dek.algorithm)
        && Objects.equals(encryptedKeyMaterial, dek.encryptedKeyMaterial);
  }

  @Override
  public int hashCode() {
    return Objects.hash(scope, algorithm, encryptedKeyMaterial);
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
