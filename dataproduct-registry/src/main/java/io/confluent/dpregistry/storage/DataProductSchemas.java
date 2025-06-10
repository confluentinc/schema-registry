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
import io.confluent.kafka.schemaregistry.storage.SchemaReference;
import io.confluent.kafka.schemaregistry.storage.SchemaValue;
import io.swagger.v3.oas.annotations.media.Schema;
import java.util.Collections;
import java.util.Map;
import java.util.Objects;
import java.util.SortedMap;
import java.util.TreeMap;

@JsonInclude(JsonInclude.Include.NON_EMPTY)
@JsonIgnoreProperties(ignoreUnknown = true)
@Schema(description = "Data Product Schemas")
public class DataProductSchemas {

  public static final int NAME_MAX_LENGTH = 256;

  private final Map<String, SchemaValue> headers;
  private final SchemaValue key;
  private final SchemaReference keyReference;
  private final SchemaValue value;
  private final SchemaReference valueReference;

  @JsonCreator
  public DataProductSchemas(@JsonProperty("headers") Map<String, SchemaValue> headers,
              @JsonProperty("key") SchemaValue key,
              @JsonProperty("keyReference") SchemaReference keyReference,
              @JsonProperty("value") SchemaValue value,
              @JsonProperty("valueReference") SchemaReference valueReference) {
    SortedMap<String, SchemaValue> sortedHeaders = headers != null
        ? new TreeMap<>(headers)
        : Collections.emptySortedMap();
    this.headers = Collections.unmodifiableSortedMap(sortedHeaders);
    this.key = key;
    this.keyReference = keyReference;
    this.value = value;
    this.valueReference = valueReference;
  }

  public DataProductSchemas(
      io.confluent.dpregistry.client.rest.entities.DataProductSchemas dataProductSchemas) {
    this.headers = dataProductSchemas.getHeaders() != null
        ? Collections.unmodifiableSortedMap(dataProductSchemas.getHeaders().entrySet().stream()
            .collect(
                TreeMap::new,
                (m, e) ->
                    m.put(e.getKey(), new SchemaValue(e.getValue())),
                TreeMap::putAll))
        : Collections.emptySortedMap();
    this.key = dataProductSchemas.getKey() != null
        ? new SchemaValue(dataProductSchemas.getKey())
        : null;
    this.keyReference = dataProductSchemas.getKeyReference() != null
        ? new SchemaReference(dataProductSchemas.getKeyReference())
        : null;
    this.value = dataProductSchemas.getValue() != null
        ? new SchemaValue(dataProductSchemas.getValue())
        : null;
    this.valueReference = dataProductSchemas.getValueReference() != null
        ? new SchemaReference(dataProductSchemas.getValueReference())
        : null;
  }

  @Schema(description = "Headers")
  @JsonProperty("headers")
  public Map<String, SchemaValue> getHeaders() {
    return headers;
  }

  @Schema(description = "Key")
  @JsonProperty("key")
  public SchemaValue getKey() {
    return key;
  }

  @Schema(description = "Key Reference")
  @JsonProperty("keyReference")
  public SchemaReference getKeyReference() {
    return keyReference;
  }

  @Schema(description = "Value")
  @JsonProperty("value")
  public SchemaValue getValue() {
    return value;
  }

  @Schema(description = "Value Reference")
  @JsonProperty("valueReference")
  public SchemaReference getValueReference() {
    return valueReference;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    DataProductSchemas rule = (DataProductSchemas) o;
    return Objects.equals(headers, rule.headers)
        && Objects.equals(key, rule.key)
        && Objects.equals(keyReference, rule.keyReference)
        && Objects.equals(value, rule.value)
        && Objects.equals(valueReference, rule.valueReference);
  }

  @Override
  public int hashCode() {
    return Objects.hash(headers, key, keyReference, value, valueReference);
  }

  public io.confluent.dpregistry.client.rest.entities.DataProductSchemas toEntity() {
    return new io.confluent.dpregistry.client.rest.entities.DataProductSchemas(
        headers != null
            ? headers.entrySet().stream()
                .collect(
                    TreeMap::new,
                    (m, e) ->
                        m.put(e.getKey(), e.getValue().toSchemaEntity()),
                    TreeMap::putAll)
            : Collections.emptySortedMap(),
        key != null ? key.toSchemaEntity() : null,
        keyReference != null ? keyReference.toRefEntity() : null,
        value != null ? value.toSchemaEntity() : null,
        valueReference != null ? valueReference.toRefEntity() : null
    );
  }
}
