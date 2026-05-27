/*
 * Copyright 2026 Confluent Inc.
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

package io.confluent.kafka.schemaregistry.type.logical.json.v1;

import io.confluent.kafka.schemaregistry.json.JsonSchema;
import io.confluent.kafka.schemaregistry.type.logical.LogicalType;
import io.confluent.kafka.schemaregistry.type.logical.json.JsonToLogicalTypeConverter;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Verifies that JSON Schema's {@code default} keyword on object properties is
 * captured into the path-keyed {@link LogicalType#getDefaultValues()} map.
 * Null defaults are skipped (matches the "no default vs default null"
 * ambiguity convention). SchemaLoader-loaded schemas route {@code default}
 * through {@code unprocessedProperties}; that fallback is exercised here too.
 */
class JsonReaderDefaultsTest {

  @Test
  void defaultsOnObjectProperties() {
    String json =
        "{\n"
            + "  \"type\": \"object\",\n"
            + "  \"properties\": {\n"
            + "    \"i\": {\"type\": \"integer\", \"default\": 7},\n"
            + "    \"s\": {\"type\": \"string\", \"default\": \"hi\"},\n"
            + "    \"b\": {\"type\": \"boolean\", \"default\": true},\n"
            + "    \"none\": {\"type\": \"integer\"}\n"
            + "  }\n"
            + "}";
    LogicalType lt = JsonToLogicalTypeConverter.toLogicalType(new JsonSchema(json));

    Map<List<Integer>, Object> defaults = lt.getDefaultValues();
    // Property order is everit's iteration order; assert by lookup rather than
    // by exact path positions to keep the test resilient to ordering.
    // JSON `integer` maps to LT BIGINT (Long) — defaults arrive as Long.
    assertThat(defaults.values()).contains(7L, "hi", true);
    // The property without `default` produces no entry.
    assertThat(defaults).hasSize(3);
  }

  @Test
  void nullDefaultIsSkipped() {
    // JSON Schema's `default: null` is ambiguous (no-default vs explicit null);
    // skipped to avoid the ambiguity.
    String json =
        "{\n"
            + "  \"type\": \"object\",\n"
            + "  \"properties\": {\n"
            + "    \"x\": {\"type\": [\"string\", \"null\"], \"default\": null}\n"
            + "  }\n"
            + "}";
    LogicalType lt = JsonToLogicalTypeConverter.toLogicalType(new JsonSchema(json));
    assertThat(lt.getDefaultValues()).isEmpty();
  }

  @Test
  void noDefaultProducesEmptyMap() {
    String json =
        "{\n"
            + "  \"type\": \"object\",\n"
            + "  \"properties\": {\n"
            + "    \"x\": {\"type\": \"integer\"},\n"
            + "    \"y\": {\"type\": \"string\"}\n"
            + "  }\n"
            + "}";
    LogicalType lt = JsonToLogicalTypeConverter.toLogicalType(new JsonSchema(json));
    assertThat(lt.getDefaultValues()).isEmpty();
  }
}
