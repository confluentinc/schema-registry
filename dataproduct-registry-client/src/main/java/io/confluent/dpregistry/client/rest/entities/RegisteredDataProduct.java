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
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.confluent.kafka.schemaregistry.utils.JacksonMapper;
import io.swagger.v3.oas.annotations.media.Schema;
import java.io.IOException;
import java.util.Collections;
import java.util.Map;
import java.util.Objects;
import java.util.SortedMap;
import java.util.TreeMap;

@JsonInclude(JsonInclude.Include.NON_EMPTY)
@JsonIgnoreProperties(ignoreUnknown = true)
@Schema(description = "Registered Data Product")
public class RegisteredDataProduct {

  private final Integer version;
  private final String guid;
  private final DataProductInfo info;
  private final DataProductSchemas schemas;
  private final Map<String, String> configs;
  private final Long timestamp;
  private final Boolean deleted;

  @JsonCreator
  public RegisteredDataProduct(
      @JsonProperty("version") Integer version,
      @JsonProperty("guid") String guid,
      @JsonProperty("info") DataProductInfo info,
      @JsonProperty("schemas") DataProductSchemas schemas,
      @JsonProperty("configs") Map<String, String> configs,
      @JsonProperty("ts") Long timestamp,
      @JsonProperty("deleted") Boolean deleted) {
    this.version = version;
    this.guid = guid;
    this.info = info;
    this.schemas = schemas;
    SortedMap<String, String> sortedConfigs = configs != null
        ? new TreeMap<>(configs)
        : Collections.emptySortedMap();
    this.configs = Collections.unmodifiableSortedMap(sortedConfigs);
    this.timestamp = timestamp;
    this.deleted = deleted;
  }

  @Schema(description = "Data product version")
  @JsonProperty("version")
  public Integer getVersion() {
    return version;
  }

  @Schema(description = "Data product guid")
  @JsonProperty("guid")
  public String getGuid() {
    return guid;
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

  @Schema(description = "Data product timestamp")
  @JsonProperty("ts")
  public Long getTimestamp() {
    return timestamp;
  }

  @Schema(description = "Data product deleted flag")
  @JsonProperty("deleted")
  public Boolean isDeleted() {
    return deleted;
  }

  @JsonIgnore
  public boolean isEquivalent(RegisteredDataProduct that) {
    return Objects.equals(info, that.info)
        && Objects.equals(schemas, that.schemas)
        && Objects.equals(configs, that.configs);
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    RegisteredDataProduct dataProduct = (RegisteredDataProduct) o;
    return Objects.equals(version, dataProduct.version)
        && Objects.equals(guid, dataProduct.guid)
        && Objects.equals(info, dataProduct.info)
        && Objects.equals(schemas, dataProduct.schemas)
        && Objects.equals(configs, dataProduct.configs);
  }

  @Override
  public int hashCode() {
    return Objects.hash(version, guid, info, schemas, configs);
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
