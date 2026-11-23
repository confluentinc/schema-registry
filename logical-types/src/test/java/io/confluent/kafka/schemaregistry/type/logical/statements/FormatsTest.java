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
        .hasMessage("A UNION in a JSON type cannot have branches a and b, which are written "
            + "as one JSON type: a value of either would match both");
  }
}
