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
import java.util.Objects;

@JsonInclude(JsonInclude.Include.NON_EMPTY)
@JsonIgnoreProperties(ignoreUnknown = true)
@Schema(description = "Data Product Schema")
public class DataProductSchema {

  private final String schema;

  @JsonCreator
  public DataProductSchema(@JsonProperty("schema") String schema) {
    this.schema = schema;
  }

  public DataProductSchema(
      io.confluent.dpregistry.client.rest.entities.DataProductSchema dataProductSchema) {
    this.schema = dataProductSchema.getSchema();
  }

  @Schema(description = "Schema")
  @JsonProperty("schema")
  public String getSchema() {
    return schema;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    DataProductSchema rule = (DataProductSchema) o;
    return Objects.equals(schema, rule.schema);
  }

  @Override
  public int hashCode() {
    return Objects.hash(schema);
  }

  public io.confluent.dpregistry.client.rest.entities.DataProductSchema toEntity() {
    return new io.confluent.dpregistry.client.rest.entities.DataProductSchema(schema);
  }
}
