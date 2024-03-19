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

package io.confluent.kafka.schemaregistry.client.rest.entities;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.EnumHashBiMap;
import java.util.Objects;
import java.util.Set;

@JsonInclude(JsonInclude.Include.NON_EMPTY)
@JsonIgnoreProperties(ignoreUnknown = true)
public class SchemaEntity {

  private final String entityPath;
  private final EntityType entityType;
  private final String normalizedPath;

  @JsonCreator
  public SchemaEntity(@JsonProperty("entityPath") String entityPath,
                      @JsonProperty("entityType") EntityType entityType) {
    this.entityPath = entityPath;
    this.entityType = entityType;
    if (this.entityPath.startsWith(".")) {
      this.normalizedPath = entityPath.substring(1);
    } else {
      this.normalizedPath = this.entityPath;
    }
  }

  @JsonProperty("entityPath")
  public String getEntityPath() {
    return entityPath;
  }

  @JsonProperty("entityType")
  public EntityType getEntityType() {
    return entityType;
  }

  @JsonIgnore
  public String getNormalizedPath() {
    return normalizedPath;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    SchemaEntity other = (SchemaEntity) o;
    return Objects.equals(this.getNormalizedPath(), other.getNormalizedPath())
      && Objects.equals(this.entityType, other.getEntityType());
  }

  @Override
  public int hashCode() {
    int result = Objects.hashCode(getNormalizedPath());
    result = 31 * result + Objects.hashCode(entityType);
    return result;
  }

  @Override
  public String toString() {
    return "SchemaEntity{" +
        "entityType=" + entityType +
        ", normalizedPath='" + normalizedPath + '\'' +
        '}';
  }

  public enum EntityType {
    @JsonProperty("sr_record")
    SR_RECORD("sr_record"),
    @JsonProperty("sr_field")
    SR_FIELD("sr_field");

    private static final EnumHashBiMap<SchemaEntity.EntityType, String> lookup =
        EnumHashBiMap.create(SchemaEntity.EntityType.class);

    static {
      for (SchemaEntity.EntityType type : SchemaEntity.EntityType.values()) {
        lookup.put(type, type.symbol());
      }
    }

    private final String symbol;

    EntityType(String symbol) {
      this.symbol = symbol;
    }

    public String symbol() {
      return symbol;
    }

    public static SchemaEntity.EntityType get(String symbol) {
      return lookup.inverse().get(symbol);
    }

    public static Set<String> symbols() {
      return lookup.inverse().keySet();
    }

    @Override
    public String toString() {
      return symbol();
    }
  }
}
