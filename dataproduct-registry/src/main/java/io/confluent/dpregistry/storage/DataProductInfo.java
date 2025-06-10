/*
 * Copyright 2022 Confluent Inc.
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

package io.confluent.dpregistry.storage;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import io.swagger.v3.oas.annotations.media.Schema;
import java.util.Collections;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.SortedMap;
import java.util.SortedSet;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.stream.Collectors;

@JsonInclude(JsonInclude.Include.NON_EMPTY)
@JsonIgnoreProperties(ignoreUnknown = true)
@Schema(description = "Data Product Info")
public class DataProductInfo {

  public static final int NAME_MAX_LENGTH = 256;

  private final String name;
  private final String doc;
  private final SortedSet<String> tags;
  @JsonPropertyOrder(alphabetic = true)
  private final SortedMap<String, String> params;
  private final String owner;
  private final String contact;

  @JsonCreator
  public DataProductInfo(@JsonProperty("name") String name,
              @JsonProperty("doc") String doc,
              @JsonProperty("tags") Set<String> tags,
              @JsonProperty("params") Map<String, String> params,
              @JsonProperty("owner") String owner,
              @JsonProperty("contact") String contact) {
    this.name = name;
    this.doc = doc;
    SortedSet<String> sortedTags = tags != null
        ? tags.stream()
        .sorted()
        .collect(Collectors.toCollection(TreeSet::new))
        : Collections.emptySortedSet();
    SortedMap<String, String> sortedParams = params != null
        ? new TreeMap<>(params)
        : Collections.emptySortedMap();
    this.tags = Collections.unmodifiableSortedSet(sortedTags);
    this.params = Collections.unmodifiableSortedMap(sortedParams);
    this.owner = owner;
    this.contact = contact;
  }

  public DataProductInfo(
      io.confluent.dpregistry.client.rest.entities.DataProductInfo dataProductInfo) {
    this.name = dataProductInfo.getName();
    this.doc = dataProductInfo.getDoc();
    this.tags = dataProductInfo.getTags() != null
        ? Collections.unmodifiableSortedSet(new TreeSet<>(dataProductInfo.getTags()))
        : Collections.emptySortedSet();
    this.params = dataProductInfo.getParams() != null
        ? Collections.unmodifiableSortedMap(new TreeMap<>(dataProductInfo.getParams()))
        : Collections.emptySortedMap();
    this.owner = dataProductInfo.getOwner();
    this.contact = dataProductInfo.getContact();
  }

  @Schema(description = "Data product name")
  @JsonProperty("name")
  public String getName() {
    return name;
  }

  @Schema(description = "Data product doc")
  @JsonProperty("doc")
  public String getDoc() {
    return doc;
  }

  @Schema(description = "Data product tags")
  @JsonProperty("tags")
  public SortedSet<String> getTags() {
    return tags;
  }

  @Schema(description = "Data product params")
  @JsonProperty("params")
  public SortedMap<String, String> getParams() {
    return params;
  }

  @Schema(description = "Data product owner")
  @JsonProperty("owner")
  public String getOwner() {
    return owner;
  }

  @Schema(description = "Data product contact")
  @JsonProperty("contact")
  public String getContact() {
    return contact;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    DataProductInfo rule = (DataProductInfo) o;
    return Objects.equals(name, rule.name)
        && Objects.equals(doc, rule.doc)
        && Objects.equals(tags, rule.tags)
        && Objects.equals(params, rule.params)
        && Objects.equals(owner, rule.owner)
        && Objects.equals(contact, rule.contact);
  }

  @Override
  public int hashCode() {
    return Objects.hash(name, doc, tags, params, owner, contact);
  }

  public io.confluent.dpregistry.client.rest.entities.DataProductInfo toEntity() {
    return new io.confluent.dpregistry.client.rest.entities.DataProductInfo(
        name,
        doc,
        tags,
        params,
        owner,
        contact
    );
  }
}
