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

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import io.confluent.kafka.schemaregistry.avro.AvroSchema;
import io.confluent.kafka.schemaregistry.client.rest.entities.SchemaReference;
import io.confluent.kafka.schemaregistry.protobuf.ProtobufSchema;
import io.confluent.kafka.schemaregistry.type.logical.avro.AvroToLogicalTypeConverter;
import io.confluent.kafka.schemaregistry.type.logical.avro.LogicalTypeToAvroConverter;
import io.confluent.kafka.schemaregistry.type.logical.json.LogicalTypeToJsonConverter;
import io.confluent.kafka.schemaregistry.type.logical.protobuf.LogicalTypeToProtoConverter;
import io.confluent.kafka.schemaregistry.type.logical.protobuf.ProtoToLogicalTypeConverter;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.Map;
import org.junit.jupiter.api.Test;

/**
 * The names of the types a writer generates for a field, kept by the avro.* and protobuf.* name
 * params: pinned before a rename, read back from a native schema whose names do not derive from
 * its fields, and yielded by a derived name that a pinned one takes.
 */
class NativeTypeNamesTest {

  private static LogicalType parse(String ddl) {
    LogicalTypesSchemaVisitor visitor = new LogicalTypesSchemaVisitor();
    visitor.visit(LogicalTypesParserFactory.parse(ddl));
    return visitor.toLogicalType();
  }

  private static String avro(String ddl) {
    return LogicalTypeToAvroConverter.fromLogicalType(parse(ddl), "p").canonicalString();
  }

  private static String proto(String ddl) {
    return LogicalTypeToProtoConverter.fromLogicalType(parse(ddl), "p").canonicalString();
  }

  private static String ddl(LogicalType logicalType) {
    return LogicalTypeToDdlConverter.toDdl(logicalType).replaceAll("\\s+", " ");
  }

  private static String avroRoundTrip(String ddl) {
    return ddl(AvroToLogicalTypeConverter.toLogicalType(new AvroSchema(avro(ddl))));
  }

  private static String protoRoundTrip(String ddl) {
    return ddl(ProtoToLogicalTypeConverter.toLogicalType(new ProtobufSchema(proto(ddl))));
  }

  @Test
  void testAvroRenameKeepsTypeNames() {
    String ddl = "STRUCT p ("
        + "rf0 STRUCT(x INT, s STRUCT(z INT)) WITH ('avro.aliases' = 'f0', 'avro.name' = 'p_f0'), "
        + "rm MAP<INT, STRUCT(y STRING)> WITH ('avro.aliases' = 'm', 'avro.name' = 'p_m'), "
        + "ru UNION(b STRUCT(x INT), s STRING) WITH ('avro.aliases' = 'u', 'avro.name' = 'p_u'));";
    assertThat(avro(ddl)).contains("\"name\":\"p_f0\"", "\"name\":\"p_f0_s\"",
        "\"name\":\"p_m_Entry\"", "\"name\":\"p_m_value\"", "\"name\":\"p_u_b\"");
    assertThat(avroRoundTrip(ddl)).contains("'avro.name' = 'p_f0'", "'avro.name' = 'p_m'",
        "'avro.name' = 'p_u'");
  }

  // The renamed field holds p_f0, so the new f0 takes the next free name, which reads back pinned.
  @Test
  void testAvroDerivedNameTakenByPinnedOneGetsSuffix() {
    String ddl = "STRUCT p ("
        + "rf0 STRUCT(x INT, s STRUCT(z INT)) WITH ('avro.aliases' = 'f0', 'avro.name' = 'p_f0'), "
        + "f0 STRUCT(y STRING, s STRUCT(w INT)));";
    assertThat(avro(ddl)).contains("\"name\":\"p_f0\"", "\"name\":\"p_f0_s\"",
        "\"name\":\"p_f0_2\"", "\"name\":\"p_f0_2_s\"");
    assertThat(avroRoundTrip(ddl)).contains(
        "f0 STRUCT(y STRING, s STRUCT(w INT)) WITH('avro.name' = 'p_f0_2')");
  }

  @Test
  void testAvroDerivedNamesReadBackWithoutParams() {
    assertThat(avroRoundTrip("STRUCT p (f0 STRUCT(s STRUCT(x INT)), "
        + "u UNION(b STRUCT(x INT), s STRING), m MAP<INT, STRUCT(y STRING)>, "
        + "n MAP<STRING, STRUCT(z INT)>, a ARRAY<STRUCT(q INT)>, h BINARY(4), "
        + "mu MAP<INT, UNION(f BINARY(4), s STRING)>);"))
        .doesNotContain("avro.");
  }

  // A fixed's name and a Connect map entry's are not derived from the field; they are kept.
  @Test
  void testAvroHandWrittenNamesWriteBackCompatibly() {
    assertWritesBack("{\"type\":\"record\",\"name\":\"Order\",\"fields\":["
        + "{\"name\":\"hash\",\"type\":{\"type\":\"fixed\",\"name\":\"MD5\",\"size\":16}},"
        + "{\"name\":\"prev\",\"type\":\"MD5\"},"
        + "{\"name\":\"u\",\"type\":[\"null\",\"MD5\",\"string\"]}]}");
    assertWritesBack("{\"type\":\"record\",\"name\":\"Order\",\"namespace\":\"com.acme\","
        + "\"fields\":[{\"name\":\"m\",\"type\":{\"type\":\"array\",\"items\":{\"type\":\"record\","
        + "\"name\":\"MapEntry\",\"namespace\":\"io.confluent.connect.avro\",\"fields\":["
        + "{\"name\":\"key\",\"type\":\"int\"},"
        + "{\"name\":\"value\",\"type\":{\"type\":\"fixed\",\"name\":\"Tok\",\"size\":2}}]}}}]}");
  }

  private static void assertWritesBack(String avro) {
    AvroSchema original = new AvroSchema(avro);
    AvroSchema written = LogicalTypeToAvroConverter.fromLogicalType(
        AvroToLogicalTypeConverter.toLogicalType(original), original.name());
    assertThat(written.isBackwardCompatible(original)).isEmpty();
  }

  @Test
  void testAvroTypeNameParams() {
    LogicalType logicalType = parse("STRUCT p (f0 STRUCT(x INT), m MAP<INT, STRING>, "
        + "u UNION(b STRUCT(x INT), s STRING), h BINARY(4), i INT);");
    assertEquals(Collections.singletonMap("avro.name", "p_f0"),
        LogicalTypeToAvroConverter.typeNameParams(logicalType, "p", "f0"));
    assertEquals(Collections.singletonMap("avro.name", "p_m"),
        LogicalTypeToAvroConverter.typeNameParams(logicalType, "p", "m"));
    assertEquals(Collections.singletonMap("avro.name", "p_u"),
        LogicalTypeToAvroConverter.typeNameParams(logicalType, "p", "u"));
    assertEquals(Collections.singletonMap("avro.name", "p_h"),
        LogicalTypeToAvroConverter.typeNameParams(logicalType, "p", "h"));
    assertThat(LogicalTypeToAvroConverter.typeNameParams(logicalType, "p", "i")).isEmpty();
  }

  // A union member's types under a collection are named from its base, as a field's are.
  @Test
  void testAvroNamesUnderUnionCollectionsKept() {
    for (String pin : new String[] {"a ARRAY<STRUCT(x INT)> WITH ('avro.name' = 'Kept')",
        "m MAP<INT, STRING> WITH ('avro.entry.name' = 'Kept')"}) {
      String ddl = "STRUCT p (u UNION(" + pin + ", b BOOLEAN));";
      assertThat(avro(ddl)).contains("\"name\":\"Kept\"");
      String readBack = avroRoundTrip(ddl);
      assertThat(readBack).contains("= 'Kept'");
      assertEquals(readBack, avroRoundTrip(readBack));
    }
    assertEquals(Collections.singletonMap("avro.name", "p_u"), LogicalTypeToAvroConverter
        .typeNameParams(parse("STRUCT p (u UNION(a ARRAY<STRUCT(x INT)>, b BOOLEAN));"), "p", "u"));
  }

  // A union in a collection takes its branch names from the collection, and its base with them.
  @Test
  void testAvroBaseOfUnionInCollectionKept() {
    for (String type : new String[] {"ARRAY<UNION(b STRUCT(x INT), s STRING)>",
        "MAP<STRING, UNION(b STRUCT(x INT), s STRING)>"}) {
      String ddl = "STRUCT p (a " + type + " WITH ('avro.name' = 'Kept'));";
      assertThat(avro(ddl)).contains("\"name\":\"Kept_b\"");
      assertThat(avroRoundTrip(ddl)).contains("'avro.name' = 'Kept'").doesNotContain("Kept_b");
      assertEquals(Collections.singletonMap("avro.name", "Kept"),
          LogicalTypeToAvroConverter.typeNameParams(parse(ddl), "p", "a"));
    }
  }

  // A map's key or value that is itself a map is named by its entry's base, which reads back.
  @Test
  void testAvroNamesOfMapInMapKept() {
    for (String ddl : new String[] {
        "STRUCT p (m MAP<INT, MAP<INT, STRING>> WITH ('avro.value.name' = 'Kept'));",
        "STRUCT p (m MAP<MAP<INT, STRING>, INT> WITH ('avro.key.name' = 'Kept'));"}) {
      assertThat(avro(ddl)).contains("\"name\":\"Kept_Entry\"");
      String readBack = avroRoundTrip(ddl);
      assertThat(readBack).contains("= 'Kept'");
      assertEquals(readBack, avroRoundTrip(readBack));
    }
    assertWritesBack("{\"type\":\"record\",\"name\":\"Order\",\"fields\":[{\"name\":\"m\","
        + "\"type\":{\"type\":\"array\",\"items\":{\"type\":\"record\",\"name\":\"M_Entry\","
        + "\"connect.internal.type\":\"MapEntry\",\"fields\":[{\"name\":\"key\",\"type\":\"int\"},"
        + "{\"name\":\"value\",\"type\":{\"type\":\"array\",\"items\":{\"type\":\"record\","
        + "\"name\":\"Inner_Entry\",\"connect.internal.type\":\"MapEntry\",\"fields\":["
        + "{\"name\":\"key\",\"type\":\"int\"},{\"name\":\"value\",\"type\":\"string\"}]}}}]}}}]}");
  }

  // A field's map name exceptions are its own, though another field derives the same base.
  @Test
  void testAvroMapNameExceptionsScopedToField() {
    String ddl = "STRUCT p (a STRUCT(b MAP<INT, STRING> WITH ('avro.entry.name' = 'Special')), "
        + "a_b MAP<INT, BIGINT>);";
    assertThat(avro(ddl)).contains("\"name\":\"Special\"", "\"name\":\"p_a_b_Entry\"");
    String readBack = avroRoundTrip(ddl);
    assertEquals(readBack, avroRoundTrip(readBack));
  }

  // A declared type's names are its own: the map name exceptions of the field using it do not
  // reach into it, though one of its fields derives the field's base.
  @Test
  void testAvroMapNameExceptionsNotInDeclaredType() {
    String ddl = "STRUCT p (f MAP<INT, N> WITH ('avro.name' = 'N_m', 'avro.entry.name' = 'Kept'));"
        + " STRUCT N (m MAP<INT, STRING>);";
    assertThat(avro(ddl)).contains("\"name\":\"Kept\"", "\"name\":\"N_m_Entry\"");
    String readBack = avroRoundTrip(ddl);
    assertEquals(readBack, avroRoundTrip(readBack));
  }

  // A union with no base to derive from, as at the root, keeps its members' names; a union in a
  // map's value derives from the value's.
  @Test
  void testAvroUnionMemberNamesKeptWithoutBase() {
    AvroSchema original =
        new AvroSchema("[{\"type\":\"fixed\",\"name\":\"MD5\",\"size\":16},\"string\"]");
    AvroSchema written = LogicalTypeToAvroConverter.fromLogicalType(
        AvroToLogicalTypeConverter.toLogicalType(original), "Holder");
    assertThat(written.isBackwardCompatible(original)).isEmpty();
    assertWritesBack("{\"type\":\"record\",\"name\":\"Order\",\"fields\":[{\"name\":\"m\","
        + "\"type\":{\"type\":\"array\",\"items\":{\"type\":\"record\",\"name\":\"MapEntry\","
        + "\"namespace\":\"io.confluent.connect.avro\",\"fields\":["
        + "{\"name\":\"key\",\"type\":\"int\"},"
        + "{\"name\":\"value\",\"type\":[{\"type\":\"fixed\",\"name\":\"Tok\",\"size\":2},"
        + "\"string\"]}]}}}]}");
  }

  // A schema reference's types, used or not, keep their names: a generated type yields, and a
  // pinned one is refused, rather than become a reference to the other.
  @Test
  void testAvroNamesOfReferencedTypesKept() {
    assertThat(avroWithReference("STRUCT p (u UNION(b STRUCT(x INT), s STRING));",
        "{\"type\":\"record\",\"name\":\"p_u_b\",\"fields\":["
            + "{\"name\":\"z\",\"type\":\"string\"}]}"))
        .contains("\"name\":\"p_u_b_2\"");
    assertThat(avroWithReference("STRUCT p (h BINARY(4));",
        "{\"type\":\"record\",\"name\":\"E\",\"fields\":[{\"name\":\"z\",\"type\":"
            + "{\"type\":\"fixed\",\"name\":\"p_h\",\"size\":8}}]}"))
        .contains("\"name\":\"p_h_2\"");
    ValidationException e = assertThrows(ValidationException.class, () -> avroWithReference(
        "STRUCT p (f STRUCT(x INT) WITH ('avro.name' = 'E'));",
        "{\"type\":\"record\",\"name\":\"E\",\"fields\":[{\"name\":\"z\",\"type\":\"string\"}]}"));
    assertThat(e.getMessage()).contains("'E'", "at a schema reference and f");
  }

  private static String avroWithReference(String ddl, String reference) {
    LogicalType parsed = parse(ddl);
    LogicalType logicalType = new LogicalType(parsed.getName(), parsed.getNamespace(),
        parsed.getRootSchema(), parsed.getNamedTypes(), new LinkedHashSet<>(),
        new LinkedHashMap<>(), Collections.singletonList(new SchemaReference("ext", "ext", 1)),
        Collections.singletonMap("ext", reference), Collections.emptyMap());
    return LogicalTypeToAvroConverter.fromLogicalType(logicalType, "p").canonicalString();
  }

  // The reader would take an array of records named as Connect's map entries for a map.
  @Test
  void testAvroMapEntryNameOnArrayRefused() {
    ValidationException e = assertThrows(ValidationException.class, () -> avro(
        "STRUCT p (xs ARRAY<STRUCT(key INT, value STRING) NOT NULL> NOT NULL "
            + "WITH ('avro.name' = 'io.confluent.connect.avro.MapEntry'));"));
    assertThat(e.getMessage()).contains("io.confluent.connect.avro.MapEntry");
  }

  @Test
  void testAvroInvalidNameRefused() {
    ValidationException e = assertThrows(ValidationException.class,
        () -> avro("STRUCT p (f0 STRUCT(x INT) WITH ('avro.name' = 'p-f0'));"));
    assertThat(e.getMessage()).contains("avro.name", "'p-f0'");
  }

  @Test
  void testProtoRenameKeepsTypeNames() {
    String ddl = "STRUCT p ("
        + "rf0 STRUCT(x INT) WITH ('protobuf.name' = 'F0Row'), "
        + "rm MAP<INT, STRING> NOT NULL WITH ('protobuf.name' = 'MEntry'), "
        + "ru UNION(b STRUCT(x INT) WITH ('protobuf.name' = 'UBRow'), s STRING));";
    assertThat(proto(ddl)).contains(".p.F0Row rf0", ".p.MEntry rm", ".p.UBRow b");
    assertThat(protoRoundTrip(ddl)).contains("'protobuf.name' = 'F0Row'",
        "'protobuf.name' = 'MEntry'", "'protobuf.name' = 'UBRow'");
  }

  @Test
  void testProtoDerivedNameTakenByPinnedOneGetsSuffix() {
    String ddl = "STRUCT p (rhome STRUCT(city STRING) WITH ('protobuf.name' = 'HomeRow'), "
        + "home STRUCT(street STRING));";
    assertThat(proto(ddl)).contains(".p.HomeRow rhome", ".p.Home2Row home");
    assertThat(protoRoundTrip(ddl)).contains(
        "home STRUCT(street STRING) WITH('protobuf.name' = 'Home2Row'");
  }

  // A oneof member's pin is its own, though it derives its name from <oneof>_<branch>, which can
  // be a field's name.
  @Test
  void testProtoBranchAndFieldPinsKeptApart() {
    assertThat(proto("STRUCT p (f1_b0 STRUCT(y STRING) WITH ('protobuf.name' = 'F1B02Row'), "
        + "f1 UNION(b0 STRUCT(x INT), s STRING));"))
        .contains(".p.F1B02Row f1_b0", ".p.F1B0Row b0");
    assertThat(proto("STRUCT p (f1_b0 STRUCT(y STRING), "
        + "f1 UNION(b0 STRUCT(x INT) WITH ('protobuf.name' = 'F1B02Row'), s STRING));"))
        .contains(".p.F1B0Row f1_b0", ".p.F1B02Row b0");
    for (String ddl : new String[] {
        "STRUCT p (f1 UNION(b0 STRUCT(x INT), s STRING), f1_b0 STRUCT(y STRING));",
        "STRUCT p (f1_b0 STRUCT(y STRING), f1 UNION(b0 STRUCT(x INT), s STRING));"}) {
      String readBack = protoRoundTrip(ddl);
      assertThat(readBack).contains("'protobuf.name' = 'F1B02Row'");
      assertEquals(readBack, protoRoundTrip(readBack));
    }
  }

  @Test
  void testProtoDerivedNamesReadBackWithoutParams() {
    assertThat(protoRoundTrip("STRUCT p (f0 STRUCT(s STRUCT(x INT)), "
        + "u UNION(b STRUCT(x INT), s STRING, a ARRAY<INT>, v UNION(i INT, t STRING)), "
        + "m MAP<INT, STRUCT(y STRING)>, xs ARRAY<STRUCT(q INT)>, ys ARRAY<ARRAY<INT>>, "
        + "zs ARRAY<INT>, e ENUM_KIND);" + "ENUM ENUM_KIND ('A', 'B');"))
        .doesNotContain("protobuf.name");
  }

  // A nested message shared by two fields is emitted once, under its own name.
  @Test
  void testProtoHandWrittenNamesWriteBackCompatibly() {
    ProtobufSchema original = new ProtobufSchema("syntax = \"proto3\";\n"
        + "message Order {\n"
        + "  Address addr = 1;\n"
        + "  Address billing = 2;\n"
        + "  Kind kind = 3;\n"
        + "  message Address { string city = 1; }\n"
        + "  enum Kind { A = 0; B = 1; }\n"
        + "}\n");
    LogicalType logicalType = ProtoToLogicalTypeConverter.toLogicalType(original);
    assertThat(ddl(logicalType)).contains("addr STRUCT(city STRING NOT NULL) "
        + "WITH('protobuf.name' = 'Address')", "WITH('protobuf.name' = 'Kind')");
    ProtobufSchema written = LogicalTypeToProtoConverter.fromLogicalType(logicalType, "Order");
    assertThat(written.canonicalString()).containsOnlyOnce("message Address");
    assertThat(written.isBackwardCompatible(original)).isEmpty();
  }

  // A shared message keeps its name though it is one field's derived name, and a message keeps
  // a name derived as another kind's.
  @Test
  void testProtoNamesLikeDerivedOnesWriteBackCompatibly() {
    assertProtoWritesBack("syntax = \"proto3\";\n"
        + "message p {\n"
        + "  AddrRow addr = 1;\n"
        + "  AddrRow billing = 2;\n"
        + "  message AddrRow { string city = 1; }\n"
        + "}\n");
    assertProtoWritesBack("syntax = \"proto3\";\n"
        + "message p {\n"
        + "  AddrEnum addr = 1;\n"
        + "  message AddrEnum { string city = 1; }\n"
        + "}\n");
  }

  private static void assertProtoWritesBack(String proto) {
    ProtobufSchema original = new ProtobufSchema(proto);
    LogicalType logicalType = ProtoToLogicalTypeConverter.toLogicalType(original);
    ProtobufSchema written = LogicalTypeToProtoConverter.fromLogicalType(logicalType, "p");
    assertThat(written.isBackwardCompatible(original)).isEmpty();
    assertEquals(ddl(logicalType), ddl(ProtoToLogicalTypeConverter.toLogicalType(written)));
  }

  // A oneof member's name is taken in its message, so a generated type yields to it.
  @Test
  void testProtoDerivedNameTakenByMemberGetsSuffix() {
    assertThat(proto("STRUCT p (u UNION(b STRUCT(x INT), UBRow STRING));"))
        .contains(".p.UB2Row b", "string UBRow");
  }

  // A declared type is referred to absolutely, so a message generated under its name in the
  // referring message, as branch u.b's UBRow, does not shadow it.
  @Test
  void testProtoGeneratedMessageDoesNotShadowDeclaredType() {
    ProtobufSchema schema = LogicalTypeToProtoConverter.fromLogicalType(parse(
        "STRUCT Holder (u UNION(b STRUCT(x INT), s STRING), o UBRow); STRUCT UBRow (y STRING);"),
        "Holder");
    assertEquals("UBRow",
        schema.toDescriptor("Holder").findFieldByName("o").getMessageType().getFullName());
    assertEquals("Holder.UBRow",
        schema.toDescriptor("Holder").findFieldByName("b").getMessageType().getFullName());
  }

  // A native map's entry is named by its map<> syntax, so it is not kept.
  @Test
  void testProtoNativeMapEntryNameNotKept() {
    ProtobufSchema original = new ProtobufSchema("syntax = \"proto3\";\n"
        + "message Order {\n  map<string, string> tags = 1;\n}\n");
    assertThat(ddl(ProtoToLogicalTypeConverter.toLogicalType(original)))
        .doesNotContain("protobuf.name");
  }

  @Test
  void testProtoTypeNameParams() {
    LogicalType logicalType = parse("STRUCT p (f0 STRUCT(x INT), m MAP<INT, STRING> NOT NULL, "
        + "u UNION(b STRUCT(x INT), s STRING), i INT);");
    assertEquals(Collections.singletonMap("protobuf.name", "F0Row"),
        LogicalTypeToProtoConverter.typeNameParams(logicalType, "p", "f0"));
    assertEquals(Collections.singletonMap("protobuf.name", "MEntry"),
        LogicalTypeToProtoConverter.typeNameParams(logicalType, "p", "m"));
    assertThat(LogicalTypeToProtoConverter.typeNameParams(logicalType, "p", "i")).isEmpty();
    Map<String, Map<String, Object>> branches =
        LogicalTypeToProtoConverter.branchTypeNameParams(logicalType, "p", "u");
    assertEquals(Collections.singletonMap("b",
        Collections.singletonMap("protobuf.name", "UBRow")), branches);
  }

  // The reader would take key and value elements named ...Entry for a map's entries.
  @Test
  void testProtoEntryPinOnArrayRefused() {
    String array = "STRUCT p (xs ARRAY<STRUCT(key INT, value STRING) NOT NULL> NOT NULL "
        + "WITH ('protobuf.name' = '%s'));";
    ValidationException e = assertThrows(ValidationException.class,
        () -> proto(String.format(array, "CustomEntry")));
    assertThat(e.getMessage()).contains("protobuf.name", "'CustomEntry'");
    assertThat(protoRoundTrip(String.format(array, "Custom")))
        .contains("ARRAY<STRUCT(", "'protobuf.name' = 'Custom'");
  }

  @Test
  void testProtoInvalidNamesRefused() {
    ValidationException e = assertThrows(ValidationException.class,
        () -> proto("STRUCT p (f0 STRUCT(x INT) WITH ('protobuf.name' = 'F0.Row'));"));
    assertThat(e.getMessage()).contains("protobuf.name", "'F0.Row'");
    // The reader knows a map entry by its suffix.
    e = assertThrows(ValidationException.class,
        () -> proto("STRUCT p (m MAP<INT, STRING> NOT NULL WITH ('protobuf.name' = 'MRow'));"));
    assertThat(e.getMessage()).contains("must end with Entry");
  }

  // Each format's params are its own: the others ignore them.
  @Test
  void testNameParamsOfOtherFormatsIgnored() {
    String ddl = "STRUCT p (f0 STRUCT(x INT) WITH ('avro.name' = 'Pinned', "
        + "'protobuf.name' = 'PinnedRow'));";
    assertThat(avro(ddl)).contains("\"name\":\"Pinned\"").doesNotContain("PinnedRow");
    assertThat(proto(ddl)).contains("PinnedRow").doesNotContain("\"Pinned\"");
    assertThat(LogicalTypeToJsonConverter.fromLogicalType(parse(ddl), "p").canonicalString())
        .doesNotContain("Pinned");
  }
}
