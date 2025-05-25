/*
 * Copyright 2023 Confluent Inc.
 *
 * Licensed under the Confluent Community License (the "License"); you may not use
 * this file except in compliance with the License.  You may obtain a copy of the
 * License at
 *
 * http://www.confluent.io/confluent-community-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OF ANY KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations under the License.
 */

package io.confluent.dpregistry.storage;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonInclude.Include;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.confluent.dpregistry.client.rest.entities.DataProduct;
import io.confluent.dpregistry.client.rest.entities.RegisteredDataProduct;
import java.util.Collections;
import java.util.Map;
import java.util.Objects;
import java.util.SortedMap;
import java.util.TreeMap;

@JsonInclude(Include.NON_NULL)
@JsonIgnoreProperties(ignoreUnknown = true)
public class DataProductValue {

  private final String guid;
  private final DataProductInfo info;
  private final DataProductSchemas schemas;
  private final Map<String, String> configs;
  private final boolean deleted;
  private final Long offset;
  private final Long timestamp;

  @JsonCreator
  public DataProductValue(
      @JsonProperty("guid") String guid,
      @JsonProperty("info") DataProductInfo info,
      @JsonProperty("schemas") DataProductSchemas schemas,
      @JsonProperty("configs") Map<String, String> configs,
      @JsonProperty("deleted") boolean deleted,
      @JsonProperty("offset") Long offset,
      @JsonProperty("ts") Long timestamp) {
    this.guid = guid;
    this.info = info;
    this.schemas = schemas;
    SortedMap<String, String> sortedConfigs = configs != null
        ? new TreeMap<>(configs)
        : Collections.emptySortedMap();
    this.configs = Collections.unmodifiableSortedMap(sortedConfigs);
    this.deleted = deleted;
    this.offset = offset;
    this.timestamp = timestamp;
  }

  public DataProductValue(String guid, DataProduct dataProduct) {
    this.guid = guid; // GUID is not part of DataProduct
    this.info = dataProduct.getInfo() != null
        ? new DataProductInfo(dataProduct.getInfo())
        : null;
    this.schemas = dataProduct.getSchemas() != null
        ? new DataProductSchemas(dataProduct.getSchemas())
        : null;
    SortedMap<String, String> sortedConfigs = dataProduct.getConfigs() != null
        ? new TreeMap<>(dataProduct.getConfigs())
        : Collections.emptySortedMap();
    this.configs = Collections.unmodifiableSortedMap(sortedConfigs);
    this.deleted = false;
    this.offset = null;
    this.timestamp = null;
  }

  @JsonProperty("guid")
  public String getGuid() {
    return guid;
  }

  @JsonProperty("info")
  public DataProductInfo getInfo() {
    return info;
  }

  @JsonProperty("schemas")
  public DataProductSchemas getSchemas() {
    return schemas;
  }

  @JsonProperty("configs")
  public Map<String, String> getConfigs() {
    return configs;
  }

  @JsonProperty("deleted")
  public boolean isDeleted() {
    return deleted;
  }

  @JsonProperty("offset")
  public Long getOffset() {
    return offset;
  }

  @JsonProperty("ts")
  public Long getTimestamp() {
    return timestamp;
  }

  @JsonIgnore
  public boolean isEquivalent(DataProductValue that) {
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
    DataProductValue dataProduct = (DataProductValue) o;
    return Objects.equals(guid, dataProduct.guid)
        && Objects.equals(info, dataProduct.info)
        && Objects.equals(schemas, dataProduct.schemas)
        && Objects.equals(configs, dataProduct.configs);
  }

  @Override
  public int hashCode() {
    return Objects.hash(guid, info, schemas, configs);
  }

  public RegisteredDataProduct toEntity() {
    return new RegisteredDataProduct(
        guid,
        info != null ? info.toEntity() : null,
        schemas != null ? schemas.toEntity() : null,
        configs,
        timestamp,
        deleted ? true : null
    );
  }
}
