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
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import io.confluent.kafka.schemaregistry.utils.JacksonMapper;
import java.io.IOException;
import java.util.Collections;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.stream.Collectors;

@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonIgnoreProperties(ignoreUnknown = true)
public class UpdateKekRequest {

  @JsonPropertyOrder(alphabetic = true)
  private SortedMap<String, String> kmsProps;
  private String doc;
  private Boolean shared;

  public static UpdateKekRequest fromJson(String json) throws IOException {
    return JacksonMapper.INSTANCE.readValue(json, UpdateKekRequest.class);
  }

  @JsonProperty("kmsProps")
  public SortedMap<String, String> getKmsProps() {
    return this.kmsProps;
  }

  @JsonProperty("kmsProps")
  public void setKmsProps(Map<String, String> kmsProps) {
    SortedMap<String, String> sortedKmsProps = kmsProps != null
        ? kmsProps.entrySet().stream()
        .sorted(Map.Entry.comparingByKey())
        .collect(Collectors.toMap(
            Entry::getKey,
            Entry::getValue,
            (e1, e2) -> e1,
            TreeMap::new))
        : Collections.emptySortedMap();
    this.kmsProps = Collections.unmodifiableSortedMap(sortedKmsProps);
  }

  @JsonProperty("doc")
  public String getDoc() {
    return doc;
  }

  @JsonProperty("doc")
  public void setDoc(String doc) {
    this.doc = doc;
  }

  @JsonProperty("shared")
  public Boolean isShared() {
    return shared;
  }

  @JsonProperty("shared")
  public void setShared(Boolean shared) {
    this.shared = shared;
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
