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

package io.confluent.kafka.serializers.protobuf;

import io.confluent.kafka.schemaregistry.ParsedSchema;
import io.confluent.kafka.schemaregistry.ParsedSchemaAndValue;
import io.confluent.kafka.schemaregistry.RuleResult;
import io.confluent.kafka.schemaregistry.protobuf.ProtobufSchema;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

public class ProtobufSchemaAndValue implements ParsedSchemaAndValue {

  private final ProtobufSchema schema;
  private final Object value;
  private final SchemaInfo writerSchemaInfo;
  private final ProtobufSchema writerSchema;
  private final List<RuleResult> ruleResults;

  public ProtobufSchemaAndValue(ProtobufSchema schema, Object value) {
    this(schema, value, null, null, Collections.emptyList());
  }

  public ProtobufSchemaAndValue(
      ProtobufSchema schema,
      Object value,
      SchemaInfo writerSchemaInfo,
      ProtobufSchema writerSchema,
      List<RuleResult> ruleResults) {
    this.schema = schema;
    this.value = value;
    this.writerSchemaInfo = writerSchemaInfo;
    this.writerSchema = writerSchema;
    this.ruleResults = ruleResults != null ? ruleResults : Collections.emptyList();
  }

  @Override
  public ProtobufSchema getSchema() {
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
  public ProtobufSchema getWriterSchema() {
    return writerSchema;
  }

  @Override
  public List<RuleResult> getRuleResults() {
    return ruleResults;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    ProtobufSchemaAndValue that = (ProtobufSchemaAndValue) o;
    return Objects.equals(schema, that.schema) && Objects.equals(value, that.value);
  }

  @Override
  public int hashCode() {
    return Objects.hash(schema, value);
  }
}
