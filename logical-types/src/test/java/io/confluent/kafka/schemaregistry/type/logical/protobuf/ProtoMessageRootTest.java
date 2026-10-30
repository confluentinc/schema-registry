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

package io.confluent.kafka.schemaregistry.type.logical.protobuf;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.squareup.wire.schema.internal.parser.TypeElement;
import io.confluent.kafka.schemaregistry.client.rest.entities.Schema;
import io.confluent.kafka.schemaregistry.client.rest.entities.SchemaReference;
import io.confluent.kafka.schemaregistry.protobuf.ProtobufSchema;
import io.confluent.kafka.schemaregistry.type.logical.LogicalType;
import io.confluent.kafka.schemaregistry.type.logical.LogicalTypeConversion;
import io.confluent.kafka.schemaregistry.type.logical.LogicalTypeToDdlConverter;
import io.confluent.kafka.schemaregistry.type.logical.ValidationException;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.junit.jupiter.api.Test;

/**
 * Which message a Protobuf schema is rooted at: the one it names, the one it declares, or, for a
 * file of several naming none, all of them; and the DDL that writes and reads back each.
 */
class ProtoMessageRootTest {

  private static final String STRUCTS = "NAMESPACE p;\n"
      + "STRUCT Address (city STRING NOT NULL);\n"
      + "STRUCT Person (name STRING NOT NULL, home Address);\n";

  private static ProtobufSchema fromDdl(String ddl) {
    Schema submitted = new Schema("people-value", null, null, ProtobufSchema.TYPE,
        Collections.emptyList(), ddl);
    return (ProtobufSchema) LogicalTypeConversion.toNative(
        LogicalTypeConversion.tryParse(ddl).get(), submitted, Collections.emptyMap());
  }

  private static List<String> messages(ProtobufSchema schema) {
    return schema.rawSchema().getTypes().stream().map(TypeElement::getName)
        .collect(Collectors.toList());
  }

  private static List<String> rootFields(LogicalType logicalType) {
    return logicalType.getRootSchema().getFields().stream()
        .map(io.confluent.kafka.schemaregistry.type.logical.Schema.Field::getName)
        .collect(Collectors.toList());
  }

  private static String roundTrip(ProtobufSchema schema) {
    return fromDdl(LogicalTypeToDdlConverter.toDdl(
        ProtoToLogicalTypeConverter.toLogicalType(schema))).canonicalString();
  }

  @Test
  void aScriptNamingNoRootOfSeveralStructsIsAFileOfAllOfThem() {
    ProtobufSchema schema = fromDdl(STRUCTS);
    // In declaration order, though Person, the one no other references, would be the root
    assertEquals(List.of("Address", "Person"), messages(schema));
    assertNull(schema.rootParam());
    assertTrue(schema.readsAsSeveralMessages());
    LogicalType read = ProtoToLogicalTypeConverter.toLogicalType(schema);
    assertTrue(ProtoToLogicalTypeConverter.isMultiMessageRoot(read.getRootSchema()));
    assertEquals(List.of("p.Address", "p.Person"), rootFields(read));
    assertEquals(schema.canonicalString(), roundTrip(schema));
  }

  @Test
  void aScriptNamingItsRootBesideOtherStructsRecordsIt() {
    ProtobufSchema schema = fromDdl(STRUCTS + "TYPE Person NOT NULL;\n");
    assertEquals(List.of("Person", "Address"), messages(schema));
    assertEquals("p.Person", schema.rootParam());
    assertFalse(schema.readsAsSeveralMessages());
    LogicalType read = ProtoToLogicalTypeConverter.toLogicalType(schema);
    assertEquals(List.of("name", "home"), rootFields(read));
    String ddl = LogicalTypeToDdlConverter.toDdl(read);
    assertTrue(ddl.contains("TYPE Person NOT NULL;"), ddl);
    assertEquals(schema.canonicalString(), roundTrip(schema));
  }

  @Test
  void aScriptOfOneStructRecordsNoRoot() {
    ProtobufSchema schema = fromDdl("NAMESPACE p;\nSTRUCT Person (name STRING NOT NULL);\n");
    assertEquals(List.of("Person"), messages(schema));
    assertNull(schema.rootParam());
    assertFalse(schema.canonicalString().contains("file_meta"), schema.canonicalString());
    assertFalse(schema.readsAsSeveralMessages());
  }

  @Test
  void aPrimitiveRootRecordsItsWrapperEvenWhenItIsTheFirst() {
    // DoubleValue is the first message of wrappers.proto, which declares several
    for (String type : List.of("DOUBLE", "FLOAT", "BIGINT")) {
      ProtobufSchema schema = fromDdl("TYPE " + type + " NOT NULL;\n");
      assertTrue(schema.rootParam() != null, type);
      assertFalse(schema.readsAsSeveralMessages(), type);
      assertEquals(type, ProtoToLogicalTypeConverter.toLogicalType(schema).getRootSchema()
          .getType().name(), type);
    }
  }

  @Test
  void aWrapperNamingNoRootIsReadAsTheFileItReExports() {
    ProtobufSchema wrappers = new ProtobufSchema(
        "syntax = \"proto3\";\nimport public \"google/protobuf/wrappers.proto\";\n");
    assertTrue(wrappers.readsAsSeveralMessages());
    LogicalType read = ProtoToLogicalTypeConverter.toLogicalType(wrappers);
    assertTrue(rootFields(read).contains("google.protobuf.Int64Value"), read.toString());

    // The messages are the re-exported file's, so they are not written back as the schema's own
    String two = "syntax = \"proto3\";\npackage q;\n"
        + "message A { int32 a = 1; }\nmessage B { int32 b = 1; }\n";
    ProtobufSchema several = new ProtobufSchema(
        "syntax = \"proto3\";\nimport public \"two.proto\";\n",
        List.of(new SchemaReference("two.proto", "two", 1)), Map.of("two.proto", two), null,
        null);
    LogicalType reExported = ProtoToLogicalTypeConverter.toLogicalType(several);
    assertEquals(List.of("q.A", "q.B"), rootFields(reExported));
    assertTrue(reExported.getExternalTypes().containsAll(List.of("q.A", "q.B")),
        reExported.getExternalTypes().toString());
    assertThrows(ValidationException.class,
        () -> LogicalTypeToProtoConverter.fromLogicalType(reExported, "ignored"));
    assertThrows(ValidationException.class, () -> LogicalTypeToDdlConverter.toDdl(reExported));

    String one = "syntax = \"proto3\";\npackage q;\nmessage Only { int32 a = 1; }\n";
    ProtobufSchema single = new ProtobufSchema(
        "syntax = \"proto3\";\nimport public \"one.proto\";\n",
        List.of(new SchemaReference("one.proto", "one", 1)), Map.of("one.proto", one), null,
        null);
    assertFalse(single.readsAsSeveralMessages());
  }

  private static final String META = "import \"confluent/meta.proto\";\n";

  private static String rootOption(String root) {
    return "option (confluent.file_meta) = { params: [ { key: \"root\", value: \"" + root
        + "\" } ] };\n";
  }

  private static ProtobufSchema wrapperOf(String file, String text, String root) {
    return new ProtobufSchema("syntax = \"proto3\";\n" + (root != null ? META : "")
        + "import public \"" + file + "\";\n" + (root != null ? rootOption(root) : ""),
        List.of(new SchemaReference(file, file, 1)), Map.of(file, text), null, null);
  }

  // The messages a wrapper is read as: its one re-exported root, or each of several
  private static List<String> readAs(ProtobufSchema wrapper) {
    wrapper.validate(false);
    LogicalType read = ProtoToLogicalTypeConverter.toLogicalType(wrapper);
    io.confluent.kafka.schemaregistry.type.logical.Schema root = read.getRootSchema();
    return ProtoToLogicalTypeConverter.isMultiMessageRoot(root)
        ? rootFields(read) : List.of(root.getQualifiedName());
  }

  @Test
  void aWrapperIsRootedByItsOwnRootParamAlone() {
    // A root names its own subject's value, so the re-exported file's doesn't carry over: over
    // either file a wrapper may name either message, or none, which is all of them
    String rooted = "syntax = \"proto3\";\npackage p;\n" + META + rootOption("p.B")
        + "message A { int32 a = 1; }\nmessage B { int32 b = 1; }\n";
    String unrooted = "syntax = \"proto3\";\npackage p;\n"
        + "message C { int32 c = 1; }\nmessage D { int32 d = 1; }\n";
    assertEquals("p.B", new ProtobufSchema(rooted).name());
    assertFalse(new ProtobufSchema(rooted).readsAsSeveralMessages());

    assertEquals(List.of("p.A", "p.B"), readAs(wrapperOf("f1.proto", rooted, null)));
    assertEquals(List.of("p.C", "p.D"), readAs(wrapperOf("f2.proto", unrooted, null)));
    assertEquals(List.of("p.C"), readAs(wrapperOf("f2.proto", unrooted, "p.C")));
    assertEquals(List.of("p.A"), readAs(wrapperOf("f1.proto", rooted, "p.A")));
  }

  @Test
  void aSchemaGivenANameIsStillReadAsAllItsMessages() {
    // A name picks the message the schema denotes; the stored file, which has none, decides
    ProtobufSchema schema = fromDdl(STRUCTS).copy("p.Person");
    assertTrue(schema.readsAsSeveralMessages());
    assertEquals(List.of("p.Address", "p.Person"),
        rootFields(ProtoToLogicalTypeConverter.toLogicalType(schema)));
    // Read as one message on purpose, it is that message
    assertEquals(List.of("name", "home"),
        rootFields(ProtoToLogicalTypeConverter.toLogicalType(schema, "p.Person")));
  }

  @Test
  void aCycleAmongSeveralStructsIsAFileOfAllOfThem() {
    // No root is inferred, so a cycle no inference could break is fine, and round-trips
    String cycle = "NAMESPACE p;\nSTRUCT A (b B);\nSTRUCT B (a A);\n";
    ProtobufSchema schema = fromDdl(cycle);
    assertEquals(List.of("A", "B"), messages(schema));
    assertTrue(schema.readsAsSeveralMessages());
    assertEquals(schema.canonicalString(), roundTrip(schema));
    // Other formats still infer one root, which a cycle leaves none of
    Schema avro = new Schema("people-value", null, null, "AVRO", Collections.emptyList(), cycle);
    ValidationException e = assertThrows(ValidationException.class,
        () -> LogicalTypeConversion.toNative(LogicalTypeConversion.tryParse(cycle).get(), avro,
            Collections.emptyMap()));
    assertTrue(e.getMessage().contains("cycle"), e.getMessage());
  }

  @Test
  void aUnionRootIsNoLongerAFileOfSeveralMessages() {
    ValidationException e = assertThrows(ValidationException.class,
        () -> fromDdl(STRUCTS + "TYPE UNION(a Address, p Person) NOT NULL;\n"));
    assertTrue(e.getMessage().contains("must be STRUCT"), e.getMessage());
  }

  private static LogicalType rootedAt(LogicalType read, List<String> messages) {
    return new LogicalType(null, read.getNamespace(),
        ProtoToLogicalTypeConverter.multiMessageRoot(messages), read.getNamedTypes(),
        read.getExternalTypes(), Map.of(), read.getReferences(), read.getResolvedReferences(),
        read.getDefaultValues());
  }

  @Test
  void aRootOverSeveralMessagesMustListEveryOneOnce() {
    LogicalType read = ProtoToLogicalTypeConverter.toLogicalType(fromDdl(STRUCTS));
    LogicalType partial = rootedAt(read, List.of("p.Person"));
    assertThrows(ValidationException.class,
        () -> LogicalTypeToProtoConverter.fromLogicalType(partial, "ignored"));
    assertThrows(ValidationException.class, () -> LogicalTypeToDdlConverter.toDdl(partial));
  }

  @Test
  void aRootOverSeveralMessagesIsWrittenAsDdlInItsOwnOrder() {
    // Declared as the root lists them, so the file the DDL writes keeps their indexes
    LogicalType reversed = rootedAt(ProtoToLogicalTypeConverter.toLogicalType(fromDdl(STRUCTS)),
        List.of("p.Person", "p.Address"));
    assertEquals(List.of("Person", "Address"),
        messages(fromDdl(LogicalTypeToDdlConverter.toDdl(reversed))));
    assertEquals(List.of("Person", "Address"),
        messages(LogicalTypeToProtoConverter.fromLogicalType(reversed, "ignored")));
  }
}
