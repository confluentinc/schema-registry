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

package io.confluent.kafka.schemaregistry.type.logical.check;

import static org.assertj.core.api.Assertions.assertThat;

import io.confluent.kafka.schemaregistry.type.logical.LogicalType;
import io.confluent.kafka.schemaregistry.type.logical.Schema;
import io.confluent.kafka.schemaregistry.type.logical.Schema.Field;
import io.confluent.kafka.schemaregistry.type.logical.check.Invalidity.Rule;
import io.confluent.kafka.schemaregistry.type.logical.check.LogicalTypeChecker.Mode;

import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

/**
 * Cases carried over from the type-level suite of the equivalent validator on the Iceberg
 * materialization path, keeping its method names and its source order so the two can be diffed.
 *
 * <p>Ten of that suite's twenty-six methods are type-level and appear here. The other sixteen cover
 * schema formats, changelog modes, serialized schema size and subject-naming strategy — all
 * table-and-topic policy that has no bearing on whether a {@link LogicalType} is usable, and none of
 * it belongs in {@link ValidityChecker}.
 *
 * <p>Two divergences, both deliberate and both recorded in {@link ValidityChecker}'s javadoc:
 *
 * <ul>
 *   <li>MULTISET is rejected there and accepted here, so its four cases invert. Iceberg's own
 *       {@code FlinkTypeToType} maps a multiset to {@code map<T, int>}, which makes that rejection
 *       policy rather than a limit of the type system.
 *   <li>Map paths are written <code>{}</code> and <code>{key}</code> rather than {@code [value]} and
 *       {@code [key]}, matching the Iceberg-schema comparison — the two references disagree with each
 *       other, so one had to be picked.
 * </ul>
 */
class ValidityCheckerIcebergSchemaTest {

  // -----------------------------------------------------------------------------------------------
  // Divergent -- MULTISET is accepted here
  // -----------------------------------------------------------------------------------------------

  @Test
  void testUnsupportedFlinkTypeThrows() {
    LogicalType type = struct(field("name", multiset(type(Schema.Type.INT))));
    assertThat(ValidityChecker.validate(Mode.ICEBERG_V2, type).isValid()).isTrue();
  }

  @Test
  void testUnsupportedFlinkTypeThrowsNested() {
    LogicalType type = struct(field("outerRow", struct0(
        field("innerRow", struct0(
            field("name", multiset(type(Schema.Type.INT))))))));
    assertThat(ValidityChecker.validate(Mode.ICEBERG_V2, type).isValid()).isTrue();
  }

  @Test
  void testArrayMultisetThrows() {
    LogicalType type = struct(field("array_of_rows", array(struct0(
        field("multiset_field", multiset(type(Schema.Type.INT)))))));
    assertThat(ValidityChecker.validate(Mode.ICEBERG_V2, type).isValid()).isTrue();
  }

  @Test
  void testMapMultisetThrows() {
    LogicalType type = struct(field("map_with_multiset", map(string(), struct0(
        field("multiset_field", multiset(type(Schema.Type.INT)))))));
    assertThat(ValidityChecker.validate(Mode.ICEBERG_V2, type).isValid()).isTrue();
  }

  // -----------------------------------------------------------------------------------------------
  // Agreed
  // -----------------------------------------------------------------------------------------------

  @Test
  void testSupportedTypesDontThrow() {
    LogicalType type = struct(
        field("name", map(type(Schema.Type.DOUBLE), type(Schema.Type.INT))));
    assertThat(ValidityChecker.validate(Mode.ICEBERG_V2, type).isValid()).isTrue();
  }

  @Test
  void testComplexValidSchemaAllowed() {
    Schema details = struct0(
        field("timestamp", Schema.createTimestamp(3)),
        field("is_active", type(Schema.Type.BOOLEAN)),
        field("double_value", type(Schema.Type.DOUBLE)));
    Schema arrayElement = struct0(
        field("id", type(Schema.Type.INT)),
        field("details", details));
    Schema innerRow = struct0(
        field("flag", type(Schema.Type.BOOLEAN)),
        field("price", Schema.createDecimal(10, 2)));
    Schema mapValue = struct0(
        field("count", type(Schema.Type.BIGINT)),
        field("description", string()),
        field("inner_row", innerRow));
    LogicalType type = struct(field("outer_row", struct0(
        field("nested_array", array(arrayElement)),
        field("nested_map", map(string(), mapValue)),
        field("basic_field", Schema.createVarchar(255)))));

    assertThat(ValidityChecker.validate(Mode.ICEBERG_V2, type).isValid()).isTrue();
  }

  @Test
  void testEmptyRowThrows() {
    // A top-level empty struct reports at the empty path, as it does there.
    LogicalType type = new LogicalType(nonNull(Schema.createStruct(Collections.emptyList())));
    assertThat(findings(type)).containsExactly(entry(Rule.EMPTY_STRUCT, ""));
  }

  @Test
  void testEmptyRowsNested() {
    LogicalType type = struct(
        field("user", nonNull(Schema.createStruct(Collections.emptyList()))),
        field("age", type(Schema.Type.INT)));
    assertThat(findings(type)).containsExactly(entry(Rule.EMPTY_STRUCT, "user"));
  }

  @Test
  void testEmptyRowDeeplyNested() {
    // There the path reads outer.middle.items[].data[value]; here the map value is written {}.
    Schema empty = nonNull(Schema.createStruct(Collections.emptyList()));
    Schema arrayElement = struct0(field("data", map(string(), empty)));
    Schema middle = struct0(field("items", array(arrayElement)));
    LogicalType type = struct(field("outer", struct0(field("middle", middle))));

    assertThat(findings(type))
        .containsExactly(entry(Rule.EMPTY_STRUCT, "outer.middle.items[].data{}"));
  }

  @Test
  void testNormalRowDoesntThrow() {
    assertThat(ValidityChecker
        .validate(Mode.ICEBERG_V2, struct(field("f0", type(Schema.Type.INT)))).isValid())
        .isTrue();
  }

  // -----------------------------------------------------------------------------------------------
  // Helpers
  // -----------------------------------------------------------------------------------------------

  private static List<String> findings(LogicalType type) {
    return ValidityChecker.validate(Mode.ICEBERG_V2, type).getInvalidities().stream()
        .map(i -> i.getRule() + "@" + i.getPath())
        .collect(Collectors.toList());
  }

  private static String entry(Rule rule, String path) {
    return rule + "@" + path;
  }

  private static Schema nonNull(Schema schema) {
    return schema.setNullable(false);
  }

  private static Field field(String name, Schema type) {
    return new Field(name, nonNull(type), 0);
  }

  /** A nested struct, as a schema rather than a whole {@link LogicalType}. */
  private static Schema struct0(Field... fields) {
    return nonNull(Schema.createStruct(positioned(fields)));
  }

  private static LogicalType struct(Field... fields) {
    return new LogicalType(nonNull(Schema.createStruct(positioned(fields))));
  }

  /** Field positions are per-struct, so rebuild them rather than leaving every field at 0. */
  private static List<Field> positioned(Field... fields) {
    List<Field> out = new ArrayList<>(fields.length);
    for (int i = 0; i < fields.length; i++) {
      out.add(new Field(fields[i].getName(), fields[i].getSchema(), i));
    }
    return out;
  }

  private static Schema array(Schema element) {
    return Schema.createArray(nonNull(element));
  }

  private static Schema multiset(Schema element) {
    return Schema.createMultiset(nonNull(element));
  }

  private static Schema map(Schema key, Schema value) {
    return Schema.createMap(nonNull(key), nonNull(value));
  }

  private static Schema string() {
    return Schema.createString();
  }

  private static Schema type(Schema.Type t) {
    return Schema.create(t);
  }
}
