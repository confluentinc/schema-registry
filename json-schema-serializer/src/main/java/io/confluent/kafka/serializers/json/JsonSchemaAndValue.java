/*
 * Copyright 2020 Confluent Inc.
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

import io.confluent.kafka.schemaregistry.ParsedSchema;
import io.confluent.kafka.schemaregistry.ParsedSchemaAndValue;
import io.confluent.kafka.schemaregistry.json.JsonSchema;
import java.util.Collections;
import java.util.Map;
import java.util.Objects;

public class JsonSchemaAndValue implements ParsedSchemaAndValue {

  private final JsonSchema schema;
  private final Object value;
  private final SchemaInfo writerSchemaInfo;
  private final ParsedSchema writerSchema;
  private final Map<String, Object> ruleData;

  public JsonSchemaAndValue(JsonSchema schema, Object value) {
    this(schema, value, null, null, Collections.emptyMap());
  }

  public JsonSchemaAndValue(
      JsonSchema schema,
      Object value,
      SchemaInfo writerSchemaInfo,
      ParsedSchema writerSchema,
      Map<String, Object> ruleData) {
    this.schema = schema;
    this.value = value;
    this.writerSchemaInfo = writerSchemaInfo;
    this.writerSchema = writerSchema;
    this.ruleData = ruleData != null ? ruleData : Collections.emptyMap();
  }

  @Override
  public JsonSchema getSchema() {
    return schema;
  }

  @Override
  public Object getValue() {
    return value;
  }

  @Override
  public SchemaInfo getWriterSchemaInfo() {
    return writerSchemaInfo;
  }

  @Override
  public ParsedSchema getWriterSchema() {
    return writerSchema;
  }

  @Override
  public Map<String, Object> getRuleData() {
    return ruleData;
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
