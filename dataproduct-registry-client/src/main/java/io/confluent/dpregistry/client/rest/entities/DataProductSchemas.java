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

package io.confluent.dpregistry.client.rest.entities;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.confluent.kafka.schemaregistry.client.rest.entities.Schema;
import io.confluent.kafka.schemaregistry.client.rest.entities.SchemaReference;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.util.Collections;
import java.util.Map;
import java.util.Objects;
import java.util.SortedMap;
import java.util.TreeMap;

@JsonInclude(JsonInclude.Include.NON_EMPTY)
@JsonIgnoreProperties(ignoreUnknown = true)
@io.swagger.v3.oas.annotations.media.Schema(description = "Data Product Schemas")
public class DataProductSchemas {

  public static final int NAME_MAX_LENGTH = 256;

  private Map<String, Schema> headers;
  private Schema key;
  private SchemaReference keyReference;
  private Schema value;
  private SchemaReference valueReference;

  @JsonCreator
  public DataProductSchemas(@JsonProperty("headers") Map<String, Schema> headers,
              @JsonProperty("key") Schema key,
              @JsonProperty("keyReference") SchemaReference keyReference,
              @JsonProperty("value") Schema value,
              @JsonProperty("valueReference") SchemaReference valueReference) {
    SortedMap<String, Schema> sortedHeaders = headers != null
        ? new TreeMap<>(headers)
        : Collections.emptySortedMap();
    this.headers = Collections.unmodifiableSortedMap(sortedHeaders);
    this.key = key;
    this.keyReference = keyReference;
    this.value = value;
    this.valueReference = valueReference;
  }

  @io.swagger.v3.oas.annotations.media.Schema(description = "Headers")
  @JsonProperty("headers")
  public Map<String, Schema> getHeaders() {
    return headers;
  }

  @io.swagger.v3.oas.annotations.media.Schema(description = "Key")
  @JsonProperty("key")
  public Schema getKey() {
    return key;
  }

  @JsonProperty("key")
  public void setKey(Schema key) {
    this.key = key;
  }

  @io.swagger.v3.oas.annotations.media.Schema(description = "Key Reference")
  @JsonProperty("keyReference")
  public SchemaReference getKeyReference() {
    return keyReference;
  }

  @JsonProperty("keyReference")
  public void setKeyReference(SchemaReference keyReference) {
    this.keyReference = keyReference;
  }

  @io.swagger.v3.oas.annotations.media.Schema(description = "Value")
  @JsonProperty("value")
  public Schema getValue() {
    return value;
  }

  @JsonProperty("value")
  public void setValue(Schema value) {
    this.value = value;
  }

  @io.swagger.v3.oas.annotations.media.Schema(description = "Value Reference")
  @JsonProperty("valueReference")
  public SchemaReference getValueReference() {
    return valueReference;
  }

  @JsonProperty("valueReference")
  public void setValueReference(SchemaReference valueReference) {
    this.valueReference = valueReference;
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

  public void updateHash(MessageDigest md) {
    if (headers != null) {
      headers.forEach((key, value) -> {
        md.update(key.getBytes(StandardCharsets.UTF_8));
        if (value != null) {
          value.updateHash(md);
        }
      });
    }
    if (key != null) {
      key.updateHash(md);
    }
    if (keyReference != null) {
      keyReference.updateHash(md);
    }
    if (value != null) {
      value.updateHash(md);
    }
    if (valueReference != null) {
      valueReference.updateHash(md);
    }
  }
}
