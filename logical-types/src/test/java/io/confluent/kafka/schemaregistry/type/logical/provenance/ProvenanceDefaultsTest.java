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


package io.confluent.kafka.schemaregistry.type.logical.provenance;

import io.confluent.kafka.schemaregistry.avro.AvroSchema;
import io.confluent.kafka.schemaregistry.protobuf.ProtobufSchema;
import io.confluent.kafka.schemaregistry.type.logical.LogicalType;
import io.confluent.kafka.schemaregistry.type.logical.LogicalTypeConversion;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.time.LocalDate;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests for the default value each member of a {@link ProvenanceReport} carries.
 *
 * <p>The contract is "what reading this member yields when a writer has no counterpart for it",
 * not "what the author declared" — which is why a Protobuf column always has one and an Avro
 * column may not.
 */
class ProvenanceDefaultsTest {

  @Test
  void anAvroDefaultIsReportedAgainstItsMember() {
    Map<List<Integer>, Object> defaults = defaultsOf(avro(
        "{\"name\":\"id\",\"type\":\"int\"}",
        "{\"name\":\"name\",\"type\":\"string\",\"default\":\"unknown\"}",
        "{\"name\":\"score\",\"type\":\"double\",\"default\":1.5}"));

    // A column with no declared default has nothing to read, which is the fact that lets a
    // consumer reject an unsatisfiable NOT NULL column rather than invent a value for it.
    assertThat(defaults).containsOnly(
        entry(Arrays.asList(1), "unknown"),
        entry(Arrays.asList(2), 1.5));
  }

  @Test
  void defaultsAreNormalisedNotLeftInTheSourceFormatsShape() {
    // Avro writes a date as a day count and a decimal as bytes; neither is readable without
    // knowing it came from Avro.
    Map<List<Integer>, Object> defaults = defaultsOf(avro(
        "{\"name\":\"d\",\"type\":{\"type\":\"int\",\"logicalType\":\"date\"},\"default\":1},"
            + "{\"name\":\"dec\",\"type\":{\"type\":\"bytes\",\"logicalType\":\"decimal\","
            + "\"precision\":5,\"scale\":2},\"default\":\"\\u0001\"}"));

    assertThat(defaults.get(Arrays.asList(0))).isEqualTo(LocalDate.ofEpochDay(1));
    assertThat(defaults.get(Arrays.asList(1))).isEqualTo(new BigDecimal("0.01"));
  }

  @Test
  void bothUsesOfASharedNamedTypeCarryItsDefault() {
    // The reader records a named type's defaults once, under the type's first occurrence. Both
    // inlined copies must still report it -- they are the same definition, so the same default.
    Map<List<Integer>, Object> defaults = defaultsOf(new AvroSchema(
        "{\"type\":\"record\",\"name\":\"R\",\"fields\":["
            + "{\"name\":\"home\",\"type\":{\"type\":\"record\",\"name\":\"Address\",\"fields\":["
            + "{\"name\":\"city\",\"type\":\"string\",\"default\":\"c\"}]}},"
            + "{\"name\":\"work\",\"type\":\"Address\"}]}"));

    assertThat(defaults).containsOnly(
        entry(Arrays.asList(0, 0), "c"),
        entry(Arrays.asList(1, 0), "c"));
  }

  @Test
  void aDefaultBehindTwoNamedTypesIsFound() {
    Map<List<Integer>, Object> defaults = defaultsOf(new AvroSchema(
        "{\"type\":\"record\",\"name\":\"R\",\"fields\":["
            + "{\"name\":\"home\",\"type\":{\"type\":\"record\",\"name\":\"Address\",\"fields\":["
            + "{\"name\":\"detail\",\"type\":{\"type\":\"record\",\"name\":\"Detail\","
            + "\"fields\":[{\"name\":\"x\",\"type\":\"int\",\"default\":9}]}}]}},"
            + "{\"name\":\"work\",\"type\":\"Address\"}]}"));

    assertThat(defaults).containsOnly(
        entry(Arrays.asList(0, 0, 0), 9),
        entry(Arrays.asList(1, 0, 0), 9));
  }

  @Test
  void aStructDefaultIsLeftOut() {
    // The readers normalise scalars and collections but pass a struct default through in the
    // source format's own shape, so there is nothing a consumer could read from it.
    Map<List<Integer>, Object> defaults = defaultsOf(new AvroSchema(
        "{\"type\":\"record\",\"name\":\"R\",\"fields\":["
            + "{\"name\":\"inner\",\"type\":{\"type\":\"record\",\"name\":\"I\",\"fields\":["
            + "{\"name\":\"q\",\"type\":\"int\"}]},\"default\":{\"q\":1}}]}"));

    assertThat(defaults).isEmpty();
  }

  @Test
  void aProtobufColumnAlwaysHasSomethingToRead() {
    // Protobuf's own rule: an unset scalar reads as its zero value, an absent repeated field as
    // an empty list. Reported without qualification, because that is what a reader would get.
    Map<List<Integer>, Object> defaults = defaultsOf(new ProtobufSchema(
        "syntax = \"proto3\";\npackage p;\n"
            + "message Row {\n  int32 id = 1;\n  string name = 2;\n  repeated int32 xs = 3;\n}\n"));

    assertThat(defaults).containsOnly(
        entry(Arrays.asList(0), 0),
        entry(Arrays.asList(1), ""),
        entry(Arrays.asList(2), Collections.emptyList()));
  }

  // -------------------------------------------------------------------------------------------
  // Helpers
  // -------------------------------------------------------------------------------------------

  private static Map<List<Integer>, Object> defaultsOf(
      io.confluent.kafka.schemaregistry.ParsedSchema schema) {
    LogicalType logicalType = LogicalTypeConversion.toLogicalType(schema);
    return ProvenanceComputer
        .report(Collections.singletonList(logicalType),
            IdentityPolicy.forSchemaType(schema.schemaType()))
        .getVersions().get(0).getMembers().stream()
        .filter(m -> m.getDefaultValue() != null)
        .collect(Collectors.toMap(ProvenanceReport.Member::getPath,
            ProvenanceReport.Member::getDefaultValue));
  }

  private static Map.Entry<List<Integer>, Object> entry(List<Integer> path, Object value) {
    return new java.util.AbstractMap.SimpleEntry<>(path, value);
  }

  private static AvroSchema avro(String... fields) {
    return new AvroSchema("{\"type\":\"record\",\"name\":\"R\",\"fields\":["
        + String.join(",", fields) + "]}");
  }
}
