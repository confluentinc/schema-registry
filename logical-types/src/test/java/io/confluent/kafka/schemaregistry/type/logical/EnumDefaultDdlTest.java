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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.confluent.kafka.schemaregistry.ParsedSchema;
import io.confluent.kafka.schemaregistry.avro.AvroSchema;
import io.confluent.kafka.schemaregistry.client.rest.entities.Schema;
import io.confluent.kafka.schemaregistry.client.rest.entities.SchemaReference;
import io.confluent.kafka.schemaregistry.json.JsonSchema;
import io.confluent.kafka.schemaregistry.protobuf.ProtobufSchema;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;

/**
 * A field of an enum type keeps its default through the DDL {@code format=logical} shows: read
 * back and registered, in each format, whether the enum is declared inline, in the script or by
 * a reference.
 */
class EnumDefaultDdlTest {

  private static final String COLOR_AVRO =
      "{\"type\":\"enum\",\"name\":\"Color\",\"namespace\":\"q\",\"symbols\":[\"RED\",\"BLUE\"]}";
  private static final String COLOR_PROTO =
      "syntax = \"proto3\";\npackage q;\nenum Color {\n  RED = 0;\n  BLUE = 1;\n}\n";

  private static ParsedSchema registered(String type, String ddl,
      List<SchemaReference> references, Map<String, String> resolved) {
    Schema submitted = new Schema("paint-value", null, null, type, references, ddl);
    return LogicalTypeConversion.toNative(
        LogicalTypeConversion.tryParse(ddl).get(), submitted, resolved);
  }

  private static ParsedSchema registered(String type, String ddl) {
    return registered(type, ddl, Collections.emptyList(), Collections.emptyMap());
  }

  private static String shown(ParsedSchema schema) {
    return LogicalTypeToDdlConverter.toDdl(LogicalTypeConversion.toLogicalType(schema));
  }

  private static Object defaultOf(ParsedSchema schema) {
    return LogicalTypeConversion.toLogicalType(schema).getRootSchema().getFields().get(0)
        .getDefaultValue();
  }

  @Test
  void anAvroEnumFieldsDefaultIsRegisteredFromTheDdlShown() {
    AvroSchema avro = new AvroSchema("{\"type\":\"record\",\"name\":\"Paint\",\"namespace\":\"p\","
        + "\"fields\":[{\"name\":\"c\",\"type\":{\"type\":\"enum\",\"name\":\"Color\","
        + "\"symbols\":[\"RED\",\"BLUE\"]},\"default\":\"BLUE\"}]}");
    ParsedSchema back = registered(AvroSchema.TYPE, shown(avro));
    assertEquals("BLUE", defaultOf(back));
    assertEquals(shown(avro), shown(back));
  }

  @Test
  void aProtobufEnumFieldsDefaultIsRegisteredFromTheDdlShown() {
    ProtobufSchema proto = new ProtobufSchema("syntax = \"proto3\";\npackage p;\n"
        + "import \"confluent/meta.proto\";\nenum Color {\n  RED = 0;\n  BLUE = 1;\n}\n"
        + "message Paint {\n  Color c = 1 [(confluent.field_meta) = { params: [ "
        + "{ key: \"logical.default\", value: \"BLUE\" } ] }];\n}\n");
    ParsedSchema back = registered(ProtobufSchema.TYPE, shown(proto));
    assertEquals("BLUE", defaultOf(back));
    assertEquals(shown(proto), shown(back));
  }

  @Test
  void aJsonEnumFieldsDefaultIsRegisteredFromTheDdlShown() {
    JsonSchema json = new JsonSchema("{\"type\":\"object\",\"title\":\"Paint\",\"properties\":"
        + "{\"c\":{\"type\":\"string\",\"enum\":[\"RED\",\"BLUE\"],\"default\":\"BLUE\"}}}");
    ParsedSchema back = registered(JsonSchema.TYPE, shown(json));
    assertEquals("BLUE", defaultOf(back));
    assertEquals(shown(json), shown(back));
  }

  @Test
  void anEnumDeclaredAfterTheFieldStillChecksTheDefault() {
    // The DDL writer declares a struct before the enum it uses
    String ddl = "NAMESPACE p;\nSTRUCT Paint (c Color NOT NULL DEFAULT '%s');\n"
        + "ENUM Color ('RED', 'BLUE');\n";
    for (String type : List.of(AvroSchema.TYPE, ProtobufSchema.TYPE, JsonSchema.TYPE)) {
      assertEquals("BLUE", defaultOf(registered(type, String.format(ddl, "BLUE"))), type);
      ValidationException e = assertThrows(ValidationException.class,
          () -> registered(type, String.format(ddl, "GREEN")), type);
      assertTrue(e.getMessage().contains("'GREEN' is not one of its symbols"), e.getMessage());
    }
  }

  @Test
  void aUnionsEnumBranchDefaultIsRegisteredFromTheDdlShown() {
    // A union's default is a value of its first branch, here a named enum
    AvroSchema avro = new AvroSchema("{\"type\":\"record\",\"name\":\"Paint\",\"namespace\":\"p\","
        + "\"fields\":[{\"name\":\"c\",\"type\":[{\"type\":\"enum\",\"name\":\"Color\","
        + "\"symbols\":[\"RED\",\"BLUE\"]},\"int\"],\"default\":\"BLUE\"}]}");
    JsonSchema json = new JsonSchema("{\"type\":\"object\",\"title\":\"Paint\",\"properties\":"
        + "{\"c\":{\"oneOf\":[{\"$ref\":\"#/$defs/Color\"},{\"type\":\"integer\"}],"
        + "\"default\":\"BLUE\"}},\"$defs\":{\"Color\":{\"type\":\"string\","
        + "\"enum\":[\"RED\",\"BLUE\"]}}}");
    for (ParsedSchema schema : List.of(avro, json)) {
      String ddl = shown(schema);
      assertTrue(ddl.contains("DEFAULT 'BLUE'"), ddl);
      ParsedSchema back = registered(schema.schemaType(), ddl);
      assertEquals("BLUE", defaultOf(back), schema.schemaType());
      assertEquals(ddl, shown(back));
    }
    String ddl = "NAMESPACE p;\nSTRUCT Paint (c UNION(color Color NOT NULL, number INT NOT NULL) "
        + "NOT NULL DEFAULT '%s');\nENUM Color ('RED', 'BLUE');\n";
    // Protobuf writes a union as a oneof, which holds no default of any type
    for (String type : List.of(AvroSchema.TYPE, JsonSchema.TYPE)) {
      assertEquals("BLUE", defaultOf(registered(type, String.format(ddl, "BLUE"))), type);
    }
    for (String type : List.of(AvroSchema.TYPE, ProtobufSchema.TYPE, JsonSchema.TYPE)) {
      assertThrows(ValidationException.class,
          () -> registered(type, String.format(ddl, "GREEN")), type);
    }
  }

  @Test
  void anEnumDefaultIsOneOfItsSymbolsAsAString() {
    ValidationException wrong = assertThrows(ValidationException.class, () -> registered(
        AvroSchema.TYPE, "STRUCT Paint (c ENUM('RED', 'BLUE') DEFAULT 'GREEN');\n"));
    assertTrue(wrong.getMessage().contains("'GREEN' is not one of its symbols"),
        wrong.getMessage());
    assertThrows(ValidationException.class, () -> registered(
        AvroSchema.TYPE, "STRUCT Paint (c ENUM('RED', 'BLUE') DEFAULT 1);\n"));
    assertThrows(ValidationException.class, () -> registered(AvroSchema.TYPE,
        "NAMESPACE p;\nSTRUCT Paint (c Color NOT NULL DEFAULT 1);\nENUM Color ('RED');\n"));
    // A struct still takes no default
    assertThrows(ValidationException.class, () -> registered(AvroSchema.TYPE,
        "NAMESPACE p;\nSTRUCT Paint (s Shade DEFAULT 'x');\nSTRUCT Shade (a INT);\n"));
  }

  @Test
  void aReferencedAvroEnumsDefaultIsCheckedByItsReference() {
    List<SchemaReference> references = List.of(new SchemaReference("q.Color", "color", 1));
    Map<String, String> resolved = Map.of("q.Color", COLOR_AVRO);
    AvroSchema avro = new AvroSchema("{\"type\":\"record\",\"name\":\"Paint\",\"namespace\":\"p\","
        + "\"fields\":[{\"name\":\"c\",\"type\":\"q.Color\",\"default\":\"BLUE\"}]}",
        references, resolved, null);
    String ddl = shown(avro);
    assertEquals("BLUE", defaultOf(registered(AvroSchema.TYPE, ddl, references, resolved)));
    // Avro itself takes any string as an enum's default
    ValidationException e = assertThrows(ValidationException.class, () -> registered(
        AvroSchema.TYPE, ddl.replace("'BLUE'", "'GREEN'"), references, resolved));
    assertTrue(e.getMessage().contains("not a symbol of enum q.Color"), e.getMessage());
    // So too as a union's first branch
    AvroSchema union = new AvroSchema("{\"type\":\"record\",\"name\":\"Paint\","
        + "\"namespace\":\"p\",\"fields\":[{\"name\":\"c\",\"type\":[\"q.Color\",\"int\"],"
        + "\"default\":\"BLUE\"}]}", references, resolved, null);
    String unionDdl = shown(union);
    assertEquals("BLUE",
        defaultOf(registered(AvroSchema.TYPE, unionDdl, references, resolved)));
    assertThrows(ValidationException.class, () -> registered(AvroSchema.TYPE,
        unionDdl.replace("'BLUE'", "'GREEN'"), references, resolved));
  }

  @Test
  void aReferencedProtobufEnumsDefaultIsCheckedByItsReference() {
    List<SchemaReference> references = List.of(new SchemaReference("color.proto", "color", 1));
    Map<String, String> resolved = Map.of("color.proto", COLOR_PROTO);
    ProtobufSchema proto = new ProtobufSchema("syntax = \"proto3\";\npackage p;\n"
        + "import \"confluent/meta.proto\";\nimport \"color.proto\";\n"
        + "message Paint {\n  q.Color c = 1 [(confluent.field_meta) = { params: [ "
        + "{ key: \"logical.default\", value: \"BLUE\" } ] }];\n}\n",
        references, resolved, null, null);
    String ddl = shown(proto);
    assertEquals("BLUE", defaultOf(registered(ProtobufSchema.TYPE, ddl, references, resolved)));
    ValidationException e = assertThrows(ValidationException.class, () -> registered(
        ProtobufSchema.TYPE, ddl.replace("'BLUE'", "'GREEN'"), references, resolved));
    assertTrue(e.getMessage().contains("not a value of enum q.Color"), e.getMessage());
  }
}
