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

package io.confluent.kafka.schemaregistry.type.logical.statements;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.confluent.kafka.schemaregistry.client.rest.entities.requests.RegisterSchemaRequest;
import io.confluent.kafka.schemaregistry.type.logical.LogicalType;
import io.confluent.kafka.schemaregistry.type.logical.SchemaType;
import io.confluent.kafka.schemaregistry.type.logical.statements.TypeScripts.BodyKind;
import java.util.Map;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

class FormatsTest {

  private static LogicalType struct(String body) {
    return TypeScripts.parse(TypeScripts.createScript("t", BodyKind.STRUCT, body, Map.of(),
        Map.of()));
  }

  @Test
  void aFormatIsNamedInAnyCaseAndARegisteredSchemaWithoutOneIsAvro() {
    assertThat(Formats.of(" protobuf ")).isEqualTo(SchemaType.PROTOBUF);
    assertThat(Formats.ofSchemaType("JSON")).isEqualTo(SchemaType.JSON);
    assertThat(Formats.ofSchemaType(null)).isEqualTo(SchemaType.AVRO);
    assertThatThrownBy(() -> Formats.of("xml")).isInstanceOf(TypeRefusedException.class)
        .hasMessage("Unknown format 'xml'; must be one of AVRO, PROTOBUF, JSON");
  }

  @ParameterizedTest
  @EnumSource(SchemaType.class)
  void aTypeIsRegisteredAsDdlAndConvertsToItsFormat(SchemaType format) {
    LogicalType lt = struct("(id BIGINT NOT NULL, note STRING)");

    RegisterSchemaRequest request = Formats.request(format, lt);
    assertThat(request.getSchemaType()).isEqualTo(format.name());
    assertThat(request.getSchema()).contains("STRUCT t (").contains("id BIGINT NOT NULL");
    assertThat(Formats.toNative(format, lt, "t").schemaType()).isEqualTo(format.name());
  }

  @Test
  void aDefaultIsRefusedOutsideAvro() {
    LogicalType lt = struct("(n INT DEFAULT 5)");
    Formats.refuseUnwritable(SchemaType.AVRO, lt);
    assertThatThrownBy(() -> Formats.refuseUnwritable(SchemaType.PROTOBUF, lt))
        .isInstanceOf(TypeRefusedException.class)
        .hasMessage("Attribute n has a DEFAULT, which a PROTOBUF type cannot apply: a record "
            + "without it reads the zero value");
    assertThatThrownBy(() -> Formats.refuseUnwritable(SchemaType.JSON, lt))
        .isInstanceOf(TypeRefusedException.class).hasMessageEndingWith("reads NULL");
  }

  @Test
  void aNotNullUnionIsRefusedInProtobufAtAnyDepth() {
    LogicalType top = struct("(u UNION(a INT, b STRING) NOT NULL)");
    LogicalType nested = struct("(v ARRAY<UNION(a INT, b STRING) NOT NULL>)");
    Formats.refuseUnwritable(SchemaType.AVRO, top);
    Formats.refuseUnwritable(SchemaType.PROTOBUF, struct("(u UNION(a INT, b STRING))"));
    for (LogicalType lt : new LogicalType[] {top, nested}) {
      assertThatThrownBy(() -> Formats.refuseUnwritable(SchemaType.PROTOBUF, lt))
          .isInstanceOf(TypeRefusedException.class).hasMessageContaining("always nullable");
    }
  }

  // A VARIANT is written as any JSON value, so a value of any branch beside it matches both.
  @Test
  void aJsonUnionHasNoBranchBesideAVariant() {
    for (String union : new String[] {"UNION(v VARIANT, s STRUCT<x INT>)",
        "UNION(a ARRAY<INT>, v VARIANT)", "UNION(v VARIANT, n INT)"}) {
      assertThatThrownBy(() -> Formats.refuseUnwritable(SchemaType.JSON,
          struct("(u " + union + ")")))
          .isInstanceOf(TypeRefusedException.class)
          .hasMessageContaining("which are written as one JSON type");
      Formats.refuseUnwritable(SchemaType.AVRO, struct("(u " + union + ")"));
    }
    Formats.refuseUnwritable(SchemaType.JSON, struct("(u UNION(v VARIANT))"));
    Formats.refuseUnwritable(SchemaType.JSON, struct("(u UNION(s STRUCT<x INT>, n INT))"));
  }

  @Test
  void aJsonUnionsBranchesAreWrittenAsDifferentJsonTypes() {
    Formats.refuseUnwritable(SchemaType.JSON, struct("(u UNION(a INT, b STRING))"));
    Formats.refuseUnwritable(SchemaType.AVRO, struct("(u UNION(a BIGINT, b DOUBLE))"));
    assertThatThrownBy(() -> Formats.refuseUnwritable(SchemaType.JSON,
        struct("(u UNION(a BIGINT, b DOUBLE))")))
        .isInstanceOf(TypeRefusedException.class)
        .hasMessage("A UNION of attribute u in a JSON type cannot have branches a and b, "
            + "which are written as one JSON type: a value of either would match both");

    // A branch that is a union is written as any of its members is: a value of one of them
    // that another branch is written as would match both.
    for (String union : new String[] {"UNION(a INT, b UNION(c INT, d STRING))",
        "UNION(b UNION(c BOOLEAN, d STRING), a VARCHAR(3))",
        "UNION(a INT, b UNION(c BOOLEAN, d UNION(e STRING, f DOUBLE)))",
        "UNION(a UNION(p INT, q STRING), b UNION(c BOOLEAN, d STRING))",
        "UNION(a INT, b UNION(c BOOLEAN, v VARIANT))"}) {
      assertThatThrownBy(() -> Formats.refuseUnwritable(SchemaType.JSON,
          struct("(u " + union + ")"))).as(union)
          .isInstanceOf(TypeRefusedException.class)
          .hasMessageContaining("which are written as one JSON type");
      Formats.refuseUnwritable(SchemaType.PROTOBUF, struct("(u " + union + ")"));
    }
    Formats.refuseUnwritable(SchemaType.JSON,
        struct("(u UNION(a INT, b UNION(c BOOLEAN, d STRING)))"));
  }

  // Rows, arrays and maps are written as objects and arrays: two branches are refused where
  // a value of one would be a value of the other.
  @Test
  void aJsonUnionsRowsArraysAndMapsAreToldApart() {
    for (String union : new String[] {
        // A row's value, written with each of its attributes, is another row's: it has no
        // attribute the other lacks, and each the other requires.
        "UNION(r1 STRUCT<a INT>, r2 STRUCT<a INT, b STRING>)",
        "UNION(r1 STRUCT<a INT, b STRING>, r2 STRUCT<b STRING>)",
        "UNION(r1 STRUCT<a INT>, r2 STRUCT<a INT>)",
        "UNION(r1 STRUCT<a INT>, r2 STRUCT<a STRING>)",
        "UNION(r1 STRUCT<a INT NOT NULL>, r2 STRUCT<a BIGINT NOT NULL>)",
        "UNION(r1 STRUCT<kind ENUM('circle', 'both') NOT NULL>, "
            + "r2 STRUCT<kind ENUM('square', 'both') NOT NULL>)",
        "UNION(r1 STRUCT<kind ENUM('circle')>, r2 STRUCT<kind ENUM('square')>)",
        "UNION(r1 ENUM('A', 'B'), r2 ENUM('B', 'C'))",
        "UNION(r1 ENUM('A', 'B'), r2 VARCHAR(3))",
        "UNION(r1 STRUCT<x INT>, r2 UNION(c STRING, d STRUCT<x INT>))",
        // An empty array is a value of any type written as one.
        "UNION(r1 ARRAY<INT>, r2 ARRAY<STRING>)",
        "UNION(r1 ARRAY<INT>, r2 MAP<INT, STRING>)",
        // A map written as an object takes any property.
        "UNION(r1 MAP<STRING, INT>, r2 STRUCT<a INT>)",
        "UNION(r1 MAP<STRING, INT>, r2 STRUCT<a INT NOT NULL>)",
        "UNION(r1 MAP<STRING, INT>, r2 MAP<STRING, STRING>)",
        "UNION(r1 MULTISET<STRING>, r2 STRUCT<a INT>)"}) {
      assertThatThrownBy(() -> Formats.refuseUnwritable(SchemaType.JSON,
          struct("(u " + union + ")"))).as(union)
          .isInstanceOf(TypeRefusedException.class)
          .hasMessageContaining("cannot have branches r1 and r2");
      Formats.refuseUnwritable(SchemaType.AVRO, struct("(u " + union + ")"));
    }
    for (String union : new String[] {
        "UNION(r1 STRUCT<a INT>, r2 STRUCT<b STRING>)",
        "UNION(r1 STRUCT<a INT>, r2 STRUCT<a INT, b STRING NOT NULL>)",
        // An attribute both require, whose value in one is never one in the other, tells
        // two rows apart, and a row from a map of other values.
        "UNION(r1 STRUCT<a INT NOT NULL>, r2 STRUCT<a STRING NOT NULL>)",
        // As rows tagged by an attribute that is one symbol in each.
        "UNION(r1 STRUCT<kind ENUM('circle') NOT NULL, r DOUBLE>, "
            + "r2 STRUCT<kind ENUM('square') NOT NULL, r DOUBLE>)",
        "UNION(r1 ENUM('A', 'B'), r2 ENUM('C'))",
        "UNION(r1 STRUCT<a STRUCT<x INT NOT NULL> NOT NULL>, "
            + "r2 STRUCT<a STRUCT<y STRING NOT NULL> NOT NULL>)",
        "UNION(r1 MAP<STRING, STRING>, r2 STRUCT<a INT NOT NULL>)",
        "UNION(r1 STRUCT<a INT>, r2 ARRAY<INT>, r3 INT)",
        "UNION(r1 MAP<STRING, INT>, r2 ARRAY<INT>)",
        "UNION(r1 MAP<STRING, INT>, r2 MAP<INT, STRING>)"}) {
      Formats.refuseUnwritable(SchemaType.JSON, struct("(u " + union + ")"));
    }
  }
}
