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

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.confluent.kafka.schemaregistry.utils.JacksonMapper;
import java.io.IOException;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonIgnoreProperties(ignoreUnknown = true)
@io.swagger.v3.oas.annotations.media.Schema(description = "Update kek request")
public class UpdateKekRequest {

  private Optional<Map<String, String>> kmsProps;
  private Optional<String> doc;
  private Optional<Boolean> shared;

  public static UpdateKekRequest fromJson(String json) throws IOException {
    return JacksonMapper.INSTANCE.readValue(json, UpdateKekRequest.class);
  }

  @io.swagger.v3.oas.annotations.media.Schema(description = "Properties of the kek")
  @JsonProperty("kmsProps")
  public Optional<Map<String, String>> getOptionalKmsProps() {
    return this.kmsProps;
  }

  @JsonIgnore
  public Map<String, String> getKmsProps() {
    return kmsProps != null ? kmsProps.orElse(null) : null;
  }

  @JsonProperty("kmsProps")
  public void setKmsProps(Optional<Map<String, String>> kmsProps) {
    this.kmsProps = kmsProps;
  }

  @JsonIgnore
  public void setKmsProps(Map<String, String> kmsProps) {
    this.kmsProps = kmsProps != null ? Optional.of(kmsProps) : null;
  }

  @io.swagger.v3.oas.annotations.media.Schema(description = "Description of the kek")
  @JsonProperty("doc")
  public Optional<String> getOptionalDoc() {
    return doc;
  }

  @JsonIgnore
  public String getDoc() {
    return doc != null ? doc.orElse(null) : null;
  }

  @JsonProperty("doc")
  public void setDoc(Optional<String> doc) {
    this.doc = doc;
  }

  @JsonIgnore
  public void setDoc(String doc) {
    this.doc = doc != null ? Optional.of(doc) : null;
  }

  @io.swagger.v3.oas.annotations.media.Schema(description = "Whether the kek is shared")
  @JsonProperty("shared")
  public Optional<Boolean> isOptionalShared() {
    return shared;
  }

  @JsonIgnore
  public Boolean isShared() {
    return shared != null ? shared.orElse(null) : null;
  }

  @JsonProperty("shared")
  public void setShared(Optional<Boolean> shared) {
    this.shared = shared;
  }

  @JsonIgnore
  public void setShared(Boolean shared) {
    this.shared = shared != null ? Optional.of(shared) : null;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    UpdateKekRequest that = (UpdateKekRequest) o;
    return Objects.equals(kmsProps, that.kmsProps)
        && Objects.equals(doc, that.doc)
        && Objects.equals(shared, that.shared);
  }

  @Override
  public int hashCode() {
    return Objects.hash(kmsProps, doc, shared);
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
