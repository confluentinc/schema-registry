/*
 * Copyright 2018 Confluent Inc.
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

package io.confluent.kafka.schemaregistry.storage;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import java.util.Collections;
import java.util.Objects;
import java.util.SortedMap;
import java.util.SortedSet;

/**
 * Metadata, which includes path annotations, arbitrary key-value properties,
 * and a set of sensitive properties.
 */

@JsonInclude(JsonInclude.Include.NON_EMPTY)
@JsonIgnoreProperties(ignoreUnknown = true)
public class Metadata {

  @JsonPropertyOrder(alphabetic = true)
  private final SortedMap<String, SortedSet<String>> paths;
  @JsonPropertyOrder(alphabetic = true)
  private final SortedMap<String, String> properties;
  private final SortedSet<String> sensitive;

  @JsonCreator
  public Metadata(
      @JsonProperty("paths") SortedMap<String, SortedSet<String>> paths,
      @JsonProperty("properties") SortedMap<String, String> properties,
      @JsonProperty("sensitive") SortedSet<String> sensitive
  ) {
    this.paths = paths != null
        ? Collections.unmodifiableSortedMap(paths)
        : Collections.emptySortedMap();
    this.properties = properties != null
        ? Collections.unmodifiableSortedMap(properties)
        : Collections.emptySortedMap();
    this.sensitive = sensitive != null
        ? Collections.unmodifiableSortedSet(sensitive)
        : Collections.emptySortedSet();
  }

  public Metadata(io.confluent.kafka.schemaregistry.client.rest.entities.Metadata metadata) {
    this.paths = Collections.unmodifiableSortedMap(metadata.getPaths());
    this.properties = Collections.unmodifiableSortedMap(metadata.getProperties());
    this.sensitive = Collections.unmodifiableSortedSet(metadata.getSensitive());
  }

  public SortedMap<String, SortedSet<String>> getPaths() {
    return paths;
  }

  public SortedMap<String, String> getProperties() {
    return properties;
  }

  public SortedSet<String> getSensitive() {
    return sensitive;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    Metadata metadata = (Metadata) o;
    return Objects.equals(paths, metadata.paths)
        && Objects.equals(properties, metadata.properties)
        && Objects.equals(sensitive, metadata.sensitive);
  }

  @Override
  public int hashCode() {
    return Objects.hash(paths, properties, sensitive);
  }

  @Override
  public String toString() {
    return "Metadata{"
        + "paths=" + paths
        + ", properties=" + properties
        + ", sensitive=" + sensitive
        + '}';
  }

  public io.confluent.kafka.schemaregistry.client.rest.entities.Metadata toMetadataEntity() {
    return new io.confluent.kafka.schemaregistry.client.rest.entities.Metadata(
        getPaths(),
        getProperties(),
        getSensitive()
    );
  }
}
