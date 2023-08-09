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
import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import java.util.Collections;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.stream.Collectors;

@JsonInclude(Include.NON_NULL)
@JsonIgnoreProperties(ignoreUnknown = true)
public class Kek {

  private final String name;
  private final String kmsType;
  private final String kmsKeyId;
  @JsonPropertyOrder(alphabetic = true)
  private final SortedMap<String, String> kmsProps;
  private final String doc;
  private final boolean shared;

  @JsonCreator
  public Kek(
      @JsonProperty("name") String name,
      @JsonProperty("kmsType") String kmsType,
      @JsonProperty("kmsKeyId") String kmsKeyId,
      @JsonProperty("kmsProps") Map<String, String> kmsProps,
      @JsonProperty("doc") String doc,
      @JsonProperty("shared") boolean shared
  ) {
    this.name = name;
    this.kmsType = kmsType;
    this.kmsKeyId = kmsKeyId;
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
}
