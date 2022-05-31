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

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import java.util.Collections;
import java.util.Objects;
import java.util.SortedMap;
import java.util.SortedSet;

/**
 * Metadata, which includes field annotations, arbitrary key-value properties, and a list of
 * references for included properties.
 */

@JsonInclude(JsonInclude.Include.NON_EMPTY)
@JsonIgnoreProperties(ignoreUnknown = true)
public class Metadata {

  public static final Metadata EMPTY_METADATA =
      new Metadata(
          Collections.emptySortedMap(), Collections.emptySortedMap(), Collections.emptySortedSet());

  @JsonPropertyOrder(alphabetic = true)
  private final SortedMap<String, SortedSet<String>> annotations;
  @JsonPropertyOrder(alphabetic = true)
  private final SortedMap<String, String> properties;
  private final SortedSet<String> sensitive;

  public Metadata(SortedMap<String, SortedSet<String>> annotations,
      SortedMap<String, String> properties, SortedSet<String> sensitive) {
    this.annotations = Collections.unmodifiableSortedMap(annotations);
    this.properties = Collections.unmodifiableSortedMap(properties);
    this.sensitive = Collections.unmodifiableSortedSet(sensitive);
  }

  public Metadata(io.confluent.kafka.schemaregistry.client.rest.entities.Metadata metadata) {
    this.annotations = Collections.unmodifiableSortedMap(metadata.getAnnotations());
    this.properties = Collections.unmodifiableSortedMap(metadata.getProperties());
    this.sensitive = Collections.unmodifiableSortedSet(metadata.getSensitive());
  }

  public SortedMap<String, SortedSet<String>> getAnnotations() {
    return annotations;
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
    return Objects.equals(annotations, metadata.annotations)
        && Objects.equals(properties, metadata.properties)
        && Objects.equals(sensitive, metadata.sensitive);
  }

  @Override
  public int hashCode() {
    return Objects.hash(annotations, properties, sensitive);
  }

  @Override
  public String toString() {
    return "Metadata{"
        + "annotations=" + annotations
        + ", properties=" + properties
        + ", sensitive=" + sensitive
        + '}';
  }

  public io.confluent.kafka.schemaregistry.client.rest.entities.Metadata toMetadataEntity() {
    return new io.confluent.kafka.schemaregistry.client.rest.entities.Metadata(
        getAnnotations(),
        getProperties(),
        getSensitive()
    );
  }
}
