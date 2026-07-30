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

package io.confluent.kafka.schemaregistry.type.logical;

import io.confluent.kafka.schemaregistry.avro.AvroSchema;
import io.confluent.kafka.schemaregistry.json.JsonSchema;
import io.confluent.kafka.schemaregistry.protobuf.ProtobufSchema;
import io.confluent.kafka.schemaregistry.type.logical.CompatibilityChecker.Mode;
import io.confluent.kafka.schemaregistry.type.logical.Incompatibility.Rule;
import io.confluent.kafka.schemaregistry.type.logical.avro.AvroToLogicalTypeConverter;
import io.confluent.kafka.schemaregistry.type.logical.json.JsonToLogicalTypeConverter;
import io.confluent.kafka.schemaregistry.type.logical.protobuf.ProtoToLogicalTypeConverter;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Compatibility checks driven from real Avro, Protobuf, and JSON Schema documents rather than from
 * hand-built logical types.
 *
 * <p><b>Why this file exists.</b> Every other compatibility test constructs an SRLT directly, which
 * makes them precise about the rules but blind to the derivation. That blindness hid a real defect:
 * the container relaxation read a default from {@link Schema.Field}, whereas the converters record a
 * derived default (an absent {@code repeated} field is an empty list, an absent proto map an empty
 * map) only in the schema's path-keyed map. Hand-built fixtures set the field flag, so the tests
 * passed while the relaxation never fired on anything a converter actually produced. Cases here start
 * from schema text, so an assumption about the derivation cannot go unnoticed again.
 *
 * <p>These are deliberately few and load-bearing. They cover the derivation facts the rules depend
 * on, not the rule matrix — that belongs in the per-mode suites.
 */
class CompatibilityCheckerEndToEndTest {

  // ---------------------------------------------------------------------------------------------
  // Helpers
  // ---------------------------------------------------------------------------------------------

  private static LogicalType fromProto(String protoText) {
    return ProtoToLogicalTypeConverter.toLogicalType(new ProtobufSchema(protoText));
  }

  private static LogicalType fromAvro(String avroText) {
    return AvroToLogicalTypeConverter.toLogicalType(new AvroSchema(avroText));
  }

  private static LogicalType fromJson(String jsonText) {
    return JsonToLogicalTypeConverter.toLogicalType(new JsonSchema(jsonText));
  }

  private static void assertCompatible(Mode mode, LogicalType original, LogicalType update) {
    CompatibilityResult result = CompatibilityChecker.compare(mode, original, update);
    assertTrue(result.isCompatible(),
        mode + " expected compatible but got: " + result.describe());
  }

  private static void assertSingle(
      Mode mode, LogicalType original, LogicalType update, Rule expected) {
    CompatibilityResult result = CompatibilityChecker.compare(mode, original, update);
    List<Rule> rules = result.getIncompatibilities().stream()
        .map(Incompatibility::getRule)
        .collect(Collectors.toList());
    assertEquals(1, rules.size(), mode + ": " + result.describe());
    assertEquals(expected, rules.get(0), mode + ": " + result.describe());
  }

  // ---------------------------------------------------------------------------------------------
  // Protobuf -- derived container defaults, the case that was broken
  // ---------------------------------------------------------------------------------------------

  @Test
  void addingARepeatedProtoFieldIsAcceptedBecauseItsDefaultIsAnEmptyList() {
    LogicalType before = fromProto(
        "syntax = \"proto3\";\n"
            + "package t;\n"
            + "message M { string name = 1; }\n");
    LogicalType after = fromProto(
        "syntax = \"proto3\";\n"
            + "package t;\n"
            + "message M { string name = 1; repeated string tags = 2; }\n");

    // The derived ARRAY is NOT NULL, so this only passes if the relaxation finds the empty-list
    // default the converter recorded in the path-keyed map.
    assertCompatible(Mode.ICEBERG, before, after);
    assertCompatible(Mode.FLINK, before, after);
  }

  @Test
  void addingAProtoMapFieldIsAcceptedBecauseItsDefaultIsAnEmptyMap() {
    LogicalType before = fromProto(
        "syntax = \"proto3\";\n"
            + "package t;\n"
            + "message M { string name = 1; }\n");
    LogicalType after = fromProto(
        "syntax = \"proto3\";\n"
            + "package t;\n"
            + "message M { string name = 1; map<string, string> labels = 2; }\n");

    assertCompatible(Mode.ICEBERG, before, after);
    assertCompatible(Mode.FLINK, before, after);
  }

  @Test
  void addingARepeatedFieldInsideANestedProtoMessageIsAccepted() {
    // Inner is declared inside M, so the file has a single top-level message and the recorded
    // default paths line up with field positions in the returned root struct.
    LogicalType before = fromProto(
        "syntax = \"proto3\";\n"
            + "package t;\n"
            + "message M { Inner inner = 1;\n"
            + "  message Inner { string a = 1; } }\n");
    LogicalType after = fromProto(
        "syntax = \"proto3\";\n"
            + "package t;\n"
            + "message M { Inner inner = 1;\n"
            + "  message Inner { string a = 1; repeated string tags = 2; } }\n");

    // Exercises the index path one level down.
    assertCompatible(Mode.ICEBERG, before, after);
    assertCompatible(Mode.FLINK, before, after);
  }

  @Test
  void nestedContainerDefaultsAreNotFoundWhenTheProtoFileHasSeveralTopLevelMessages() {
    // Known limitation, pinned so the behaviour is deliberate rather than accidental.
    //
    // With two top-level messages the converter records the default index paths as {[1,0], [1,1]},
    // while the root schema it returns has "inner" at position 0 -- the first path component is not
    // a field position in the returned struct. A position-derived lookup therefore misses, and the
    // relaxation does not fire. That is the safe direction (a spurious rejection, never a spurious
    // acceptance), but it is a converter inconsistency rather than an intended rule: declaring the
    // same types with Inner nested inside M yields {[0,0], [0,1]} and works. See
    // addingARepeatedFieldInsideANestedProtoMessageIsAccepted for the working form.
    LogicalType before = fromProto(
        "syntax = \"proto3\";\n"
            + "package t;\n"
            + "message M { Inner inner = 1; }\n"
            + "message Inner { string a = 1; }\n");
    LogicalType after = fromProto(
        "syntax = \"proto3\";\n"
            + "package t;\n"
            + "message M { Inner inner = 1; }\n"
            + "message Inner { string a = 1; repeated string tags = 2; }\n");

    assertSingle(Mode.ICEBERG, before, after, Rule.REQUIRED_FIELD_ADDED);
  }

  @Test
  void addingAProtoScalarIsAcceptedBecauseProto3GivesItAnImplicitDefault() {
    // A proto3 singular scalar has implicit presence, so it derives as NOT NULL -- but the converter
    // records the wire-level zero value as its default, which is what makes this readable for rows
    // written before the field existed.
    LogicalType before = fromProto(
        "syntax = \"proto3\";\n"
            + "package t;\n"
            + "message M { string name = 1; }\n");
    LogicalType after = fromProto(
        "syntax = \"proto3\";\n"
            + "package t;\n"
            + "message M { string name = 1; int32 count = 2; }\n");

    assertCompatible(Mode.FLINK, before, after);
    // Iceberg mode relaxes containers only, so a scalar is still rejected there.
    assertSingle(Mode.ICEBERG, before, after, Rule.REQUIRED_FIELD_ADDED);
  }

  @Test
  void widenedProtoIntegerIsAcceptedAndNarrowedIsNot() {
    LogicalType asInt32 = fromProto(
        "syntax = \"proto3\";\npackage t;\nmessage M { int32 n = 1; }\n");
    LogicalType asInt64 = fromProto(
        "syntax = \"proto3\";\npackage t;\nmessage M { int64 n = 1; }\n");

    assertCompatible(Mode.ICEBERG, asInt32, asInt64);
    assertCompatible(Mode.FLINK, asInt32, asInt64);
    assertSingle(Mode.ICEBERG, asInt64, asInt32, Rule.UNSUPPORTED_TYPE_CHANGE);
    assertSingle(Mode.FLINK, asInt64, asInt32, Rule.UNSUPPORTED_TYPE_CHANGE);
  }

  @Test
  void reorderingProtoFieldDeclarationsIsRejectedEvenThoughTagsAreUnchanged() {
    // Tag numbers are untouched, so the format layer is indifferent. The derived column order is not.
    LogicalType before = fromProto(
        "syntax = \"proto3\";\npackage t;\nmessage M { string a = 1; string b = 2; }\n");
    LogicalType after = fromProto(
        "syntax = \"proto3\";\npackage t;\nmessage M { string b = 2; string a = 1; }\n");

    assertSingle(Mode.FLINK, before, after, Rule.FIELD_REORDERED);
    assertSingle(Mode.ICEBERG, before, after, Rule.FIELD_REORDERED);
  }

  // ---------------------------------------------------------------------------------------------
  // Avro -- annotations that change the derived type
  // ---------------------------------------------------------------------------------------------

  @Test
  void annotatingAnAvroLongAsATimestampChangesTheDerivedType() {
    // The wire bytes are identical and Avro resolution is indifferent, but the derived type is not.
    LogicalType asLong = fromAvro(
        "{\"type\":\"record\",\"name\":\"R\",\"fields\":["
            + "{\"name\":\"ts\",\"type\":\"long\"}]}");
    LogicalType asTimestamp = fromAvro(
        "{\"type\":\"record\",\"name\":\"R\",\"fields\":["
            + "{\"name\":\"ts\",\"type\":{\"type\":\"long\",\"logicalType\":\"timestamp-millis\"}}]}");

    assertSingle(Mode.FLINK, asLong, asTimestamp, Rule.UNSUPPORTED_TYPE_CHANGE);
    assertSingle(Mode.ICEBERG, asLong, asTimestamp, Rule.UNSUPPORTED_TYPE_CHANGE);
  }

  @Test
  void addingAnAvroFieldWithADeclaredDefaultIsAccepted() {
    LogicalType before = fromAvro(
        "{\"type\":\"record\",\"name\":\"R\",\"fields\":["
            + "{\"name\":\"a\",\"type\":\"string\"}]}");
    LogicalType after = fromAvro(
        "{\"type\":\"record\",\"name\":\"R\",\"fields\":["
            + "{\"name\":\"a\",\"type\":\"string\"},"
            + "{\"name\":\"b\",\"type\":\"string\",\"default\":\"x\"}]}");

    assertCompatible(Mode.FLINK, before, after);
  }

  @Test
  void addingARequiredAvroFieldWithNoDefaultIsRejected() {
    // Contrast with the proto cases: Avro has no implicit default for a missing field, so nothing
    // rescues this one.
    LogicalType before = fromAvro(
        "{\"type\":\"record\",\"name\":\"R\",\"fields\":["
            + "{\"name\":\"a\",\"type\":\"string\"}]}");
    LogicalType after = fromAvro(
        "{\"type\":\"record\",\"name\":\"R\",\"fields\":["
            + "{\"name\":\"a\",\"type\":\"string\"},"
            + "{\"name\":\"b\",\"type\":\"string\"}]}");

    assertSingle(Mode.FLINK, before, after, Rule.REQUIRED_FIELD_ADDED);
    assertSingle(Mode.ICEBERG, before, after, Rule.REQUIRED_FIELD_ADDED);
  }

  @Test
  void addingAnAvroArrayWithoutADefaultIsRejectedUnlikeProto() {
    // An absent Avro array is not defined to be empty, so the container relaxation has nothing to
    // find. This is the derivation difference between the two formats, made explicit.
    LogicalType before = fromAvro(
        "{\"type\":\"record\",\"name\":\"R\",\"fields\":["
            + "{\"name\":\"a\",\"type\":\"string\"}]}");
    LogicalType after = fromAvro(
        "{\"type\":\"record\",\"name\":\"R\",\"fields\":["
            + "{\"name\":\"a\",\"type\":\"string\"},"
            + "{\"name\":\"tags\",\"type\":{\"type\":\"array\",\"items\":\"string\"}}]}");

    assertSingle(Mode.ICEBERG, before, after, Rule.REQUIRED_FIELD_ADDED);
  }

  @Test
  void makingAnAvroFieldNullableIsAcceptedAndTheReverseIsNot() {
    LogicalType required = fromAvro(
        "{\"type\":\"record\",\"name\":\"R\",\"fields\":["
            + "{\"name\":\"a\",\"type\":\"string\"}]}");
    LogicalType nullable = fromAvro(
        "{\"type\":\"record\",\"name\":\"R\",\"fields\":["
            + "{\"name\":\"a\",\"type\":[\"null\",\"string\"],\"default\":null}]}");

    assertCompatible(Mode.FLINK, required, nullable);
    assertCompatible(Mode.ICEBERG, required, nullable);
    assertSingle(Mode.FLINK, nullable, required, Rule.NULLABLE_TO_NON_NULLABLE);
    assertSingle(Mode.ICEBERG, nullable, required, Rule.NULLABLE_TO_NON_NULLABLE);
  }

  @Test
  void addingAnAvroEnumSymbolDoesNotChangeTheDerivedType() {
    // Enums derive to an unbounded VARCHAR, so symbol changes are invisible to both modes -- even
    // though Avro's own resolution would reject reading an unknown symbol.
    LogicalType before = fromAvro(
        "{\"type\":\"record\",\"name\":\"R\",\"fields\":[{\"name\":\"e\",\"type\":"
            + "{\"type\":\"enum\",\"name\":\"E\",\"symbols\":[\"A\"]}}]}");
    LogicalType after = fromAvro(
        "{\"type\":\"record\",\"name\":\"R\",\"fields\":[{\"name\":\"e\",\"type\":"
            + "{\"type\":\"enum\",\"name\":\"E\",\"symbols\":[\"A\",\"B\"]}}]}");

    assertCompatible(Mode.FLINK, before, after);
    assertCompatible(Mode.ICEBERG, before, after);
  }

  // ---------------------------------------------------------------------------------------------
  // JSON Schema
  // ---------------------------------------------------------------------------------------------

  @Test
  void addingAnOptionalJsonPropertyIsAccepted() {
    LogicalType before = fromJson(
        "{\"type\":\"object\",\"properties\":{"
            + "\"a\":{\"type\":\"string\",\"connect.index\":0}},\"required\":[\"a\"]}");
    LogicalType after = fromJson(
        "{\"type\":\"object\",\"properties\":{"
            + "\"a\":{\"type\":\"string\",\"connect.index\":0},"
            + "\"b\":{\"type\":\"string\",\"connect.index\":1}},\"required\":[\"a\"]}");

    assertCompatible(Mode.FLINK, before, after);
    assertCompatible(Mode.ICEBERG, before, after);
  }

  @Test
  void addingARequiredJsonPropertyIsRejected() {
    LogicalType before = fromJson(
        "{\"type\":\"object\",\"properties\":{"
            + "\"a\":{\"type\":\"string\",\"connect.index\":0}},\"required\":[\"a\"]}");
    LogicalType after = fromJson(
        "{\"type\":\"object\",\"properties\":{"
            + "\"a\":{\"type\":\"string\",\"connect.index\":0},"
            + "\"b\":{\"type\":\"string\",\"connect.index\":1}},\"required\":[\"a\",\"b\"]}");

    assertSingle(Mode.FLINK, before, after, Rule.REQUIRED_FIELD_ADDED);
    assertSingle(Mode.ICEBERG, before, after, Rule.REQUIRED_FIELD_ADDED);
  }

  @Test
  void relaxingAJsonIntegerToANumberChangesTheDerivedType() {
    // "integer" derives to BIGINT and "number" to DOUBLE. A JSON-Schema widening, but not a type
    // widening either mode accepts.
    LogicalType asInteger = fromJson(
        "{\"type\":\"object\",\"properties\":{"
            + "\"n\":{\"type\":\"integer\",\"connect.index\":0}},\"required\":[\"n\"]}");
    LogicalType asNumber = fromJson(
        "{\"type\":\"object\",\"properties\":{"
            + "\"n\":{\"type\":\"number\",\"connect.index\":0}},\"required\":[\"n\"]}");

    assertSingle(Mode.FLINK, asInteger, asNumber, Rule.UNSUPPORTED_TYPE_CHANGE);
    assertSingle(Mode.ICEBERG, asInteger, asNumber, Rule.UNSUPPORTED_TYPE_CHANGE);
  }

  @Test
  void insertingAJsonPropertyWithoutAConnectIndexShiftsColumnOrder() {
    // Unindexed properties sort alphabetically, so inserting "b" between "a" and "c" repositions
    // "c". The property set is otherwise unchanged, which is what makes this trap worth pinning.
    LogicalType before = fromJson(
        "{\"type\":\"object\",\"properties\":{"
            + "\"a\":{\"type\":\"string\"},\"c\":{\"type\":\"string\"}}}");
    LogicalType after = fromJson(
        "{\"type\":\"object\",\"properties\":{"
            + "\"a\":{\"type\":\"string\"},\"b\":{\"type\":\"string\"},"
            + "\"c\":{\"type\":\"string\"}}}");

    // "b" lands in the middle, so it is an addition rather than a reorder -- and being optional it
    // is accepted. Pinned to document that alphabetical ordering makes middle insertion the norm.
    assertCompatible(Mode.FLINK, before, after);
  }
}
