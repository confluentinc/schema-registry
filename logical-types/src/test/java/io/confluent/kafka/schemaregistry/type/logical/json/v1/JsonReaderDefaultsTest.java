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
import io.confluent.kafka.schemaregistry.type.logical.common.LogicalTypeVersion;
import io.confluent.kafka.schemaregistry.type.logical.json.JsonToLogicalTypeConverter;
import io.confluent.kafka.schemaregistry.type.logical.json.LogicalTypeToJsonConverter;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.time.LocalDate;
import java.time.LocalTime;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertTimeoutPreemptively;

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

  @Test
  void aConversionPlacesNoDefaultsUntilTheyAreRead() {
    // Each definition holds the next twice: 2^30 paths to the last one's default.
    StringBuilder defs = new StringBuilder();
    for (int i = 0; i < 30; i++) {
      defs.append("\"D").append(i).append("\":{\"type\":\"object\",\"properties\":{")
          .append("\"a\":{\"$ref\":\"#/definitions/D").append(i + 1).append("\"},")
          .append("\"b\":{\"$ref\":\"#/definitions/D").append(i + 1).append("\"}}},");
    }
    defs.append("\"D30\":{\"type\":\"object\",\"properties\":{")
        .append("\"x\":{\"type\":\"integer\",\"default\":1}}}");
    JsonSchema schema = new JsonSchema("{\"type\":\"object\",\"properties\":{"
        + "\"p\":{\"$ref\":\"#/definitions/D0\"}},\"definitions\":{" + defs + "}}");
    LogicalType lt = assertTimeoutPreemptively(Duration.ofSeconds(3), () ->
        JsonToLogicalTypeConverter.toLogicalType(schema));
    assertThat(lt.getNamedTypes()).isNotEmpty();
  }

  // -- Composite and Connect date/time defaults, as Flink's JSON converter read them ------------

  @Test
  void anArrayDefaultIsInTheMapOnly() {
    LogicalType lt = property(
        "{\"type\": \"array\", \"items\": {\"type\": \"string\"}, \"default\": [\"x\", null]}");
    assertThat(lt.getDefaultValues().get(List.of(0))).isEqualTo(Arrays.asList("x", null));
    // No writer encodes one, so the field keeps none and a round trip stays as it was.
    assertThat(lt.getRootSchema().getFields().get(0).hasDefaultValue()).isFalse();
  }

  @Test
  void aMapDefaultIsKeyedByItsKeyType() {
    LogicalType byObject = property("{\"type\": \"object\", \"connect.type\": \"map\","
        + " \"additionalProperties\": {\"type\": \"integer\"}, \"default\": {\"k\": 1}}");
    assertThat(byObject.getDefaultValues().get(List.of(0))).isEqualTo(Map.of("k", 1L));
    // A map whose keys are not strings is an array of entries, and so is its default.
    LogicalType byEntries = property("{\"type\": \"array\", \"connect.type\": \"map\","
        + " \"items\": {\"type\": \"object\", \"properties\": {\"key\": {\"type\": \"integer\"},"
        + " \"value\": {\"type\": \"string\"}}}, \"default\": [{\"key\": 1, \"value\": \"a\"}]}");
    assertThat(byEntries.getDefaultValues().get(List.of(0))).isEqualTo(Map.of(1L, "a"));
  }

  @Test
  void aMultisetDefaultCountsItsElements() {
    LogicalType lt = property("{\"type\": \"object\", \"connect.type\": \"map\","
        + " \"flink.type\": \"multiset\", \"additionalProperties\": {\"type\": \"integer\","
        + " \"connect.type\": \"int32\"}, \"default\": {\"a\": 2}}");
    assertThat(lt.getDefaultValues().get(List.of(0))).isEqualTo(Map.of("a", 2));
  }

  @Test
  void aStructDefaultHoldsItsMembersByName() {
    // Members the literal lacks or sets to null are left out, as Flink's converter left them.
    LogicalType lt = property("{\"type\": \"object\", \"properties\": {"
        + "\"z\": {\"type\": \"integer\"}, \"n\": {\"type\": [\"string\", \"null\"]},"
        + " \"m\": {\"type\": \"string\"}},"
        + " \"default\": {\"z\": 4, \"n\": null}}");
    assertThat(lt.getDefaultValues().get(List.of(0))).isEqualTo(Map.of("z", 4L));
    assertThat(lt.getRootSchema().getFields().get(0).hasDefaultValue()).isFalse();
  }

  @Test
  void aCompositeDefaultWithAMemberOfAnotherTypeIsDropped() {
    LogicalType lt = property(
        "{\"type\": \"array\", \"items\": {\"type\": \"integer\"}, \"default\": [\"x\"]}");
    assertThat(lt.getDefaultValues()).isEmpty();
  }

  @Test
  void aConnectDateOrTimeDefaultIsInTheMapOnly() {
    // Days since the epoch and milliseconds of the day, as Connect writes them.
    LogicalType date = property("{\"type\": \"integer\", \"title\":"
        + " \"org.apache.kafka.connect.data.Date\", \"connect.type\": \"int32\","
        + " \"default\": 19000}");
    assertThat(date.getDefaultValues().get(List.of(0))).isEqualTo(LocalDate.ofEpochDay(19000));
    assertThat(date.getRootSchema().getFields().get(0).hasDefaultValue()).isFalse();
    LogicalType time = property("{\"type\": \"integer\", \"title\":"
        + " \"org.apache.kafka.connect.data.Time\", \"connect.type\": \"int32\","
        + " \"default\": 1000}");
    assertThat(time.getDefaultValues().get(List.of(0))).isEqualTo(LocalTime.ofSecondOfDay(1));
  }

  @Test
  void aCompositeDefaultReadsItsMembersThroughReferences() {
    // The members' type is a definition, a named type in the LT: read as the type it names.
    LogicalType lt = JsonToLogicalTypeConverter.toLogicalType(new JsonSchema(
        "{\"type\": \"object\", \"properties\": {\"p\": {\"type\": \"array\","
        + " \"items\": {\"$ref\": \"#/definitions/Pt\"}, \"default\": [{\"x\": 1}]}},"
        + " \"definitions\": {\"Pt\": {\"type\": \"object\","
        + " \"properties\": {\"x\": {\"type\": \"integer\"}}}}}"));
    assertThat(lt.getDefaultValues().get(List.of(0))).isEqualTo(List.of(Map.of("x", 1L)));
  }

  @Test
  void aUnionsCompositeOrConnectDateDefaultIsNotTheFields() {
    // Decoded by the union's first branch, it is as map-only as that branch's own would be.
    LogicalType array = property("{\"oneOf\": [{\"type\": \"array\", \"items\": {\"type\":"
        + " \"string\"}}], \"default\": [\"x\"]}");
    assertThat(array.getRootSchema().getFields().get(0).hasDefaultValue()).isFalse();
    LogicalType date = property("{\"oneOf\": [{\"type\": \"integer\", \"title\":"
        + " \"org.apache.kafka.connect.data.Date\", \"connect.type\": \"int32\"},"
        + " {\"type\": \"string\"}], \"default\": 19000}");
    assertThat(date.getRootSchema().getFields().get(0).hasDefaultValue()).isFalse();
  }

  @Test
  void aUnionLedByAReferenceKeepsNoFieldDefault() {
    // The writer encodes a union's default by its first branch as written, not through a
    // reference: the default is in the map only, and the schema still writes.
    LogicalType lt = JsonToLogicalTypeConverter.toLogicalType(new JsonSchema(
        "{\"type\": \"object\", \"properties\": {\"p\": {\"oneOf\": [{\"$ref\":"
            + " \"#/definitions/S\"}, {\"type\": \"integer\"}], \"default\": \"x\"}},"
            + " \"definitions\": {\"S\": {\"type\": \"string\"}}}"), LogicalTypeVersion.V1);
    assertThat(lt.getRootSchema().getFields().get(0).hasDefaultValue()).isFalse();
    LogicalTypeToJsonConverter.fromLogicalType(lt, "R", LogicalTypeVersion.V2);
  }

  // A schema whose only property, at path [0], is {@code property}.
  private static LogicalType property(String property) {
    return JsonToLogicalTypeConverter.toLogicalType(new JsonSchema(
        "{\"type\": \"object\", \"properties\": {\"p\": " + property + "}}"));
  }
}
