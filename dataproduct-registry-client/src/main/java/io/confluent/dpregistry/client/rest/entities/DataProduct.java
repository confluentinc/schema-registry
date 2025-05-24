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
@Schema(description = "Data Product")
public class DataProduct {

  public static final int NAME_MAX_LENGTH = 256;

  private final DataProductInfo info;
  private final DataProductSchemas schemas;
  private final Map<String, String> configs;

  @JsonCreator
  public DataProduct(@JsonProperty("info") DataProductInfo info,
              @JsonProperty("schemas") DataProductSchemas schemas,
              @JsonProperty("configs") Map<String, String> configs) {
    this.info = info;
    this.schemas = schemas;
    SortedMap<String, String> sortedConfigs = configs != null
        ? new TreeMap<>(configs)
        : Collections.emptySortedMap();
    this.configs = Collections.unmodifiableSortedMap(sortedConfigs);
  }

  @Schema(description = "Data product info")
  @JsonProperty("info")
  public DataProductInfo getInfo() {
    return info;
  }

  @Schema(description = "Data product schemas")
  @JsonProperty("schemas")
  public DataProductSchemas getSchemas() {
    return schemas;
  }

  @Schema(description = "Data product configs")
  @JsonProperty("configs")
  public Map<String, String> getConfigs() {
    return configs;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    DataProduct dataProduct = (DataProduct) o;
    return Objects.equals(info, dataProduct.info)
        && Objects.equals(schemas, dataProduct.schemas)
        && Objects.equals(configs, dataProduct.configs);
  }

  @Override
  public int hashCode() {
    return Objects.hash(info, schemas, configs);
  }

  public void updateHash(MessageDigest md) {
    if (info != null) {
      info.updateHash(md);
    }
    if (schemas != null) {
      schemas.updateHash(md);
    }
    if (configs != null) {
      configs.forEach((key, value) -> {
        md.update(key.getBytes(StandardCharsets.UTF_8));
        if (value != null) {
          md.update(value.getBytes(StandardCharsets.UTF_8));
        }
      });
    }
  }
}
