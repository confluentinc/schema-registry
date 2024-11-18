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
import io.confluent.kafka.schemaregistry.utils.JacksonMapper;
import java.io.IOException;
import java.util.Map;
import java.util.Objects;

@JsonInclude(Include.NON_NULL)
@JsonIgnoreProperties(ignoreUnknown = true)
@io.swagger.v3.oas.annotations.media.Schema(description = "Create kek request")
public class CreateKekRequest {

  private String name;
  private String kmsType;
  private String kmsKeyId;
  private Map<String, String> kmsProps;
  private String doc;
  private boolean shared;
  private boolean deleted;

  public static CreateKekRequest fromJson(String json) throws IOException {
    return JacksonMapper.INSTANCE.readValue(json, CreateKekRequest.class);
  }

  @io.swagger.v3.oas.annotations.media.Schema(description = "Name of the kek")
  @JsonProperty("name")
  public String getName() {
    return this.name;
  }

  @JsonProperty("name")
  public void setName(String name) {
    this.name = name;
  }

  @io.swagger.v3.oas.annotations.media.Schema(description = "KMS type of the kek")
  @JsonProperty("kmsType")
  public String getKmsType() {
    return this.kmsType;
  }

  @JsonProperty("kmsType")
  public void setKmsType(String kmsType) {
    this.kmsType = kmsType;
  }

  @io.swagger.v3.oas.annotations.media.Schema(description = "KMS key ID of the kek")
  @JsonProperty("kmsKeyId")
  public String getKmsKeyId() {
    return this.kmsKeyId;
  }

  @JsonProperty("kmsKeyId")
  public void setKmsKeyId(String kmsKeyId) {
    this.kmsKeyId = kmsKeyId;
  }

  @io.swagger.v3.oas.annotations.media.Schema(description = "Properties of the kek")
  @JsonProperty("kmsProps")
  public Map<String, String> getKmsProps() {
    return this.kmsProps;
  }

  @JsonProperty("kmsProps")
  public void setKmsProps(Map<String, String> kmsProps) {
    this.kmsProps = kmsProps;
  }

  @io.swagger.v3.oas.annotations.media.Schema(description = "Description of the kek")
  @JsonProperty("doc")
  public String getDoc() {
    return this.doc;
  }

  @JsonProperty("doc")
  public void setDoc(String doc) {
    this.doc = doc;
  }

  @io.swagger.v3.oas.annotations.media.Schema(description = "Whether the kek is shared")
  @JsonProperty("shared")
  public boolean isShared() {
    return this.shared;
  }

  @JsonProperty("shared")
  public void setShared(boolean shared) {
    this.shared = shared;
  }

  @io.swagger.v3.oas.annotations.media.Schema(description = "Whether the kek is deleted")
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
    CreateKekRequest kek = (CreateKekRequest) o;
    return shared == kek.shared
        && deleted == kek.deleted
        && Objects.equals(name, kek.name)
        && Objects.equals(kmsType, kek.kmsType)
        && Objects.equals(kmsKeyId, kek.kmsKeyId)
        && Objects.equals(kmsProps, kek.kmsProps)
        && Objects.equals(doc, kek.doc);
  }

  @Override
  public int hashCode() {
    return Objects.hash(name, kmsType, kmsKeyId, kmsProps, doc, shared, deleted);
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
