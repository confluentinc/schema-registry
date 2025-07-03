/*
 * Copyright 2020-2025 Confluent Inc.
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

package io.confluent.kafka.serializers.json;

import java.util.Objects;

import io.confluent.kafka.schemaregistry.json.JsonSchema;

public class JsonSchemaAndValue {

  private final JsonSchema schema;
  private final Object value;

  public JsonSchemaAndValue(JsonSchema schema, Object value) {
    this.schema = schema;
    this.value = value;
  }

  public JsonSchema getSchema() {
    return schema;
  }

  public Object getValue() {
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
    JsonSchemaAndValue that = (JsonSchemaAndValue) o;
    return Objects.equals(schema, that.schema) && Objects.equals(value, that.value);
  }

  @Override
  public int hashCode() {
    return Objects.hash(schema, value);
  }
}
