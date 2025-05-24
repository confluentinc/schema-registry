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
import io.swagger.v3.oas.annotations.media.Schema;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
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

  private final Map<String, DataProductSchema> headers;
  private final DataProductSchema key;
  private final DataProductSchema value;

  @JsonCreator
  public DataProductSchemas(@JsonProperty("headers") Map<String, DataProductSchema> headers,
              @JsonProperty("key") DataProductSchema key,
              @JsonProperty("value") DataProductSchema value) {
    SortedMap<String, DataProductSchema> sortedHeaders = headers != null
        ? new TreeMap<>(headers)
        : Collections.emptySortedMap();
    this.headers = Collections.unmodifiableSortedMap(sortedHeaders);
    this.key = key;
    this.value = value;
  }

  public DataProductSchemas(
      io.confluent.dpregistry.client.rest.entities.DataProductSchemas dataProductSchemas) {
    this.headers = dataProductSchemas.getHeaders() != null
        ? Collections.unmodifiableSortedMap(dataProductSchemas.getHeaders().entrySet().stream()
            .collect(
                TreeMap::new,
                (m, e) ->
                    m.put(e.getKey(), new DataProductSchema(e.getValue())),
                TreeMap::putAll))
        : Collections.emptySortedMap();
    this.key = dataProductSchemas.getKey() != null
        ? new DataProductSchema(dataProductSchemas.getKey())
        : null;
    this.value = dataProductSchemas.getValue() != null
        ? new DataProductSchema(dataProductSchemas.getValue())
        : null;
  }

  @Schema(description = "Headers")
  @JsonProperty("headers")
  public Map<String, DataProductSchema> getHeaders() {
    return headers;
  }

  @Schema(description = "Key")
  @JsonProperty("key")
  public DataProductSchema getKey() {
    return key;
  }

  @Schema(description = "Value")
  @JsonProperty("value")
  public DataProductSchema getValue() {
    return value;
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
        && Objects.equals(value, rule.value);
  }

  @Override
  public int hashCode() {
    return Objects.hash(headers, key, value);
  }

  public io.confluent.dpregistry.client.rest.entities.DataProductSchemas toEntity() {
    return new io.confluent.dpregistry.client.rest.entities.DataProductSchemas(
        headers != null
            ? headers.entrySet().stream()
                .collect(
                    TreeMap::new,
                    (m, e) ->
                        m.put(e.getKey(), e.getValue().toEntity()),
                    TreeMap::putAll)
            : Collections.emptySortedMap(),
        key != null ? key.toEntity() : null,
        value != null ? value.toEntity() : null
    );
  }
}
