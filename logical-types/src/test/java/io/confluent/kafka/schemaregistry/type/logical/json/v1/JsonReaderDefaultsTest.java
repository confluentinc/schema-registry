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

  @Test
  void aDefinitionsDefaultsSitUnderThePropertyUsingIt() {
    LogicalType lt = JsonToLogicalTypeConverter.toLogicalType(new JsonSchema(
        "{\"type\":\"object\",\"properties\":{\"a\":{\"$ref\":\"#/definitions/D\"},"
            + "\"n\":{\"type\":\"integer\",\"default\":1}},"
            + "\"definitions\":{\"D\":{\"type\":\"object\",\"properties\":{"
            + "\"x\":{\"type\":\"integer\",\"default\":5}}}}}"));

    assertThat(lt.getDefaultValues()).containsOnly(
        Map.entry(List.of(0, 0), 5L), Map.entry(List.of(1), 1L));
  }

  @Test
  void aDefinitionUsedTwiceHasItsDefaultsUnderEachUse() {
    LogicalType lt = JsonToLogicalTypeConverter.toLogicalType(new JsonSchema(
        "{\"type\":\"object\",\"properties\":{\"a\":{\"$ref\":\"#/definitions/D\"},"
            + "\"b\":{\"$ref\":\"#/definitions/D\"}},"
            + "\"definitions\":{\"D\":{\"type\":\"object\",\"properties\":{"
            + "\"s\":{\"type\":\"string\",\"default\":\"z\"},"
            + "\"x\":{\"type\":\"integer\",\"default\":5}}}}}"));

    assertThat(lt.getDefaultValues()).containsOnly(
        Map.entry(List.of(0, 0), "z"), Map.entry(List.of(0, 1), 5L),
        Map.entry(List.of(1, 0), "z"), Map.entry(List.of(1, 1), 5L));
  }

  @Test
  void aChainOfReferencesPutsTheDefaultAtItsEnd() {
    LogicalType lt = JsonToLogicalTypeConverter.toLogicalType(new JsonSchema(
        "{\"type\":\"object\",\"properties\":{\"p\":{\"$ref\":\"#/definitions/D\"}},"
            + "\"definitions\":{\"D\":{\"type\":\"object\",\"properties\":{"
            + "\"q\":{\"$ref\":\"#/definitions/E\"}}},"
            + "\"E\":{\"type\":\"object\",\"properties\":{"
            + "\"y\":{\"type\":\"boolean\",\"default\":true}}}}}"));

    assertThat(lt.getDefaultValues()).containsOnly(Map.entry(List.of(0, 0, 0), true));
  }

  @Test
  void aRecursiveDefinitionsDefaultsStopWhereItRecurs() {
    LogicalType lt = JsonToLogicalTypeConverter.toLogicalType(new JsonSchema(
        "{\"type\":\"object\",\"properties\":{\"p\":{\"$ref\":\"#/definitions/D\"}},"
            + "\"definitions\":{\"D\":{\"type\":\"object\",\"properties\":{"
            + "\"d\":{\"$ref\":\"#/definitions/D\"},"
            + "\"x\":{\"type\":\"integer\",\"default\":5}}}}}"));

    assertThat(lt.getDefaultValues()).containsOnly(Map.entry(List.of(0, 1), 5L));
  }

  @Test
  void aModernDefsDefinitionsDefaultsSitUnderThePropertyUsingIt() {
    // 2020-12 keeps $defs, so they convert up front rather than at their first $ref.
    LogicalType lt = JsonToLogicalTypeConverter.toLogicalType(new JsonSchema(
        "{\"$schema\":\"https://json-schema.org/draft/2020-12/schema\",\"type\":\"object\","
            + "\"properties\":{\"a\":{\"$ref\":\"#/$defs/D\"},"
            + "\"n\":{\"type\":\"integer\",\"default\":1}},"
            + "\"$defs\":{\"D\":{\"type\":\"object\",\"properties\":{"
            + "\"x\":{\"type\":\"integer\",\"default\":5}}}}}"));

    assertThat(lt.getDefaultValues()).containsOnly(
        Map.entry(List.of(0, 0), 5L), Map.entry(List.of(1), 1L));
  }

  @Test
  void aDefinitionReenteredWhileItConvertsKeepsItsCompleteDefaults() {
    // D's f refers to E, whose union holds D again: D converts inside E before E is known, then
    // again in full; its default is the full conversion's.
    LogicalType lt = JsonToLogicalTypeConverter.toLogicalType(new JsonSchema(
        "{\"$schema\":\"https://json-schema.org/draft/2020-12/schema\",\"type\":\"object\","
            + "\"properties\":{\"d\":{\"$ref\":\"#/$defs/D\"}},"
            + "\"$defs\":{\"D\":{\"type\":\"object\",\"properties\":{"
            + "\"f\":{\"$ref\":\"#/$defs/E\",\"default\":7}}},"
            + "\"E\":{\"oneOf\":[{\"type\":\"integer\"},{\"$ref\":\"#/$defs/D\"}]}}}"));

    assertThat(lt.getDefaultValues()).containsOnly(Map.entry(List.of(0, 0), 7L));
  }
}
