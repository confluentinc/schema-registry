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

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.Descriptors.FieldDescriptor;
import com.google.protobuf.DynamicMessage;
import io.confluent.kafka.schemaregistry.client.SchemaMetadata;
import io.confluent.kafka.schemaregistry.client.rest.entities.Config;
import io.confluent.kafka.schemaregistry.client.rest.entities.SchemaReference;
import io.confluent.kafka.schemaregistry.json.JsonSchema;
import io.confluent.kafka.schemaregistry.protobuf.ProtobufSchema;
import io.confluent.kafka.schemaregistry.type.logical.LogicalType;
import io.confluent.kafka.schemaregistry.type.logical.Schema;
import io.confluent.kafka.schemaregistry.type.logical.SchemaType;
import io.confluent.kafka.schemaregistry.type.logical.json.JsonToLogicalTypeConverter;
import io.confluent.kafka.schemaregistry.type.logical.protobuf.LogicalTypeToProtoConverter;
import io.confluent.kafka.schemaregistry.type.logical.statements.AlterAction.AddAttribute;
import io.confluent.kafka.schemaregistry.type.logical.statements.AlterAction.AlterAttributeType;
import io.confluent.kafka.schemaregistry.type.logical.statements.AlterAction.DropAttribute;
import io.confluent.kafka.schemaregistry.type.logical.statements.AlterAction.MoveAttribute;
import io.confluent.kafka.schemaregistry.type.logical.statements.AlterAction.Position;
import io.confluent.kafka.schemaregistry.type.logical.statements.AlterAction.RenameAttribute;
import io.confluent.kafka.schemaregistry.type.logical.statements.AlterAction.SetAttributeWith;
import io.confluent.kafka.schemaregistry.type.logical.statements.AlterAction.SetDataType;
import io.confluent.kafka.schemaregistry.type.logical.statements.TypeScripts.BodyKind;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.everit.json.schema.ValidationException;
import org.junit.jupiter.api.Test;

/**
 * More edges of what ALTER TYPE and CREATE TYPE refuse and leave, as those of
 * {@link TypeAlterEdgeCasesTest}: each a rule that a change of one word of the code would
 * break with no other test to tell it.
 */
class TypeAlterMoreEdgeCasesTest {

  private static final String WRAPPED = "[(confluent.field_meta) = {\n    params: [{\n      "
      + "key: \"flink.wrapped\",\n      value: \"true\"\n    }]\n  }]";
  private static final String P3 = "syntax = \"proto3\";\n\nimport \"confluent/meta.proto\";\n";
  private static final String JSON_HEAD = "{\"type\":\"object\",\"title\":\"%s\","
      + "\"properties\":{\"id\":{\"type\":\"integer\"},";
  private static final String WRAPPED_ONEOF = P3 + "\nmessage T {\n  int32 id = 5;\n"
      + "  WOneofWrapper w = 2 " + WRAPPED + ";\n  int32 z = 6;\n%s\n  message WOneofWrapper {\n"
      + "    oneof value {\n      int32 a = 1;\n      string b = 3;\n    }\n  }\n}\n";
  private static final String IS_WRAPPED = "is a oneof wrapped in a message";
  private static final String LATER = "\"$schema\":\"https://json-schema.org/draft/2020-12/"
      + "schema\",";
  private static final String P = "{\"type\":\"object\",\"properties\":{\"p\":{\"type\":"
      + "\"integer\"}},\"required\":[\"p\"],\"additionalProperties\":false}";
  private static final String Q = "{\"type\":\"object\",\"properties\":{\"q\":{\"type\":"
      + "\"string\"}},\"required\":[\"q\"],\"additionalProperties\":false}";
  private static final String LIB = "{\"type\":\"object\",\"title\":\"lib\",\"properties\":"
      + "{\"k\":{\"type\":\"integer\"}},\"additionalProperties\":false,\"$defs\":{\"Loc\":" + P
      + "}}";

  private final TestRegistry registry = new TestRegistry();

  private void create(String subject, SchemaType format, String body) {
    registry.create(subject, format, BodyKind.STRUCT, body);
  }

  private int version(String subject) {
    return registry.latest(subject).getVersion();
  }

  private Descriptor message(String subject) {
    return ((ProtobufSchema) registry.schema(subject)).toDescriptor();
  }

  private String fields(String subject) {
    return message(subject).getFields().stream().map(f -> f.getName() + "=" + f.getNumber())
        .sorted().collect(Collectors.joining(" "));
  }

  private void proto(String subject, String text) throws Exception {
    registry.client.register(subject, new ProtobufSchema(text));
  }

  private void json(String subject, String text, String... refs) throws Exception {
    List<SchemaReference> references = new ArrayList<>();
    Map<String, String> resolved = new LinkedHashMap<>();
    for (String ref : refs) {
      SchemaMetadata md = registry.latest(ref);
      references.add(new SchemaReference(ref, ref, md.getVersion()));
      resolved.put(ref, md.getSchema());
    }
    registry.client.register(subject, new JsonSchema(text, references, resolved, null));
  }

  private List<String> references(String subject) {
    List<SchemaReference> refs = registry.latest(subject).getReferences();
    return refs == null ? List.of()
        : refs.stream().map(SchemaReference::getName).collect(Collectors.toList());
  }

  private String head(String type) {
    return String.format(JSON_HEAD, type);
  }

  // A plain field given a union type is no wrapped oneof where no schema can be made.
  @Test
  void aFieldGivenAUnionTypeIsNoWrappedOneofWhereNoSchemaCanBeMade() {
    create("w1", SchemaType.PROTOBUF, "(id INT, a INT, u UNION(c INT, q STRING))");
    assertThatThrownBy(() -> registry.alter("w1",
        new AlterAttributeType("a", "UNION(x INT, y STRING)"), new AddAttribute("c", "INT")))
        .isInstanceOf(TypeRefusedException.class)
        .hasMessageContaining("keeps its field name 'c'")
        .hasMessageNotContaining("wrapped in a message");
  }

  // A union added under the name the wrapped oneof was stored by, which is by then an
  // INT of another name, is no continuation of it.
  @Test
  void aUnionAddedUnderTheWrappedOneofsOldNameIsNotIt() throws Exception {
    proto("u1", String.format(WRAPPED_ONEOF, ""));
    registry.alter("u1", new AlterAttributeType("w", "INT"), new RenameAttribute("w", "x"),
        new AddAttribute("w", "UNION(p INT, q STRING)"));
    assertThat(version("u1")).isEqualTo(2);
    assertThat(message("u1").findFieldByNumber(2)).isNull();
    assertThat(message("u1").getRealOneofs()).extracting(o -> o.getName()).containsExactly("w");
  }

  // A wrapped oneof of a nested message is told by what is written, whatever unions
  // the root keeps.
  @Test
  void aWrappedOneofOfANestedMessageIsRefused() throws Exception {
    proto("o5", P3 + "\nmessage T {\n  int32 id = 1;\n  N n = 2;\n  message N {\n    W w = 2 "
        + WRAPPED + ";\n    message W {\n      oneof value {\n        int32 a = 1;\n        "
        + "string b = 3;\n      }\n    }\n  }\n}\n");
    assertThatThrownBy(() -> registry.alter("o5", new AddAttribute("k", "INT")))
        .isInstanceOf(TypeRefusedException.class)
        .hasMessageContaining("field w of message N " + IS_WRAPPED);
    assertThat(version("o5")).isEqualTo(1);
  }

  // A Protobuf type that is no row, written as a wrapper the file imports, has no
  // attributes to tell unions of, and is given another type.
  @Test
  void aProtobufTypeThatIsNoRowIsGivenAnotherType() {
    registry.create("ot", SchemaType.PROTOBUF, BodyKind.OTHER, "INT");
    registry.alter("ot", new AlterAction.SetDataType("BIGINT"));
    assertThat(version("ot")).isEqualTo(2);
  }

  private static final String ROOT = "option (confluent.file_meta) = {\n  params: {\n    key: "
      + "\"logical.root\",\n    value: \"T\"\n  }\n};\n";

  // A wrapped oneof of a message beside the root is told by what is written, whatever
  // unions the root keeps.
  @Test
  void aWrappedOneofOfAMessageBesideTheRootIsRefused() throws Exception {
    proto("o4", P3 + "\n" + ROOT + "\nmessage T {\n  int32 id = 1;\n}\nmessage M {\n  W w = 2 "
        + WRAPPED + ";\n  message W {\n    oneof value {\n      int32 a = 1;\n      string b = "
        + "3;\n    }\n  }\n}\n");
    assertThatThrownBy(() -> registry.alter("o4", new AddAttribute("k", "INT")))
        .isInstanceOf(TypeRefusedException.class)
        .hasMessageContaining("field w of message M " + IS_WRAPPED);
    assertThat(version("o4")).isEqualTo(1);
  }

  private static final String ROW = "{\"type\":\"object\",\"title\":\"%s\",\"properties\":"
      + "{\"%s\":%s},\"required\":[\"%2$s\"],\"additionalProperties\":false%4$s}";

  // References name@version, in the order given.
  private void jsonAt(String subject, String text, String... refs) throws Exception {
    List<SchemaReference> references = new ArrayList<>();
    Map<String, String> resolved = new LinkedHashMap<>();
    for (String ref : refs) {
      String[] at = ref.split("@");
      SchemaMetadata md = registry.version(at[0], Integer.parseInt(at[1]));
      references.add(new SchemaReference(at[0], at[0], md.getVersion()));
      resolved.putIfAbsent(at[0], md.getSchema());
    }
    registry.client.register(subject, new JsonSchema(text, references, resolved, null));
  }

  private void twoVersionsOfBv() throws Exception {
    json("bv", String.format(ROW, "bv", "k", "{\"type\":\"integer\"}", ""));
    json("abv", String.format(ROW, "abv", "in", "{\"$ref\":\"bv\"}", ""), "bv");
    json("libx", String.format(ROW, "libx", "in", "{\"$ref\":\"bv\"}", ",\"$defs\":{\"Loc\":"
        + P + "}"), "bv");
    json("bv", String.format(ROW, "bv", "p", "{\"type\":\"integer\"}", ""));
  }

  // A reference of no use binds a name below one that is of use by a definition alone.
  @Test
  void aReferenceBindsANameBelowOneInUseByADefinitionAlone() throws Exception {
    twoVersionsOfBv();
    jsonAt("r5", head("r5") + "\"a\":{\"$ref\":\"libx#/$defs/Loc\"}},"
        + "\"additionalProperties\":false}", "bv@2", "libx@1");
    assertThatThrownBy(() -> registry.alter("r5", new AddAttribute("z", "INT")))
        .isInstanceOf(TypeRefusedException.class)
        .hasMessageContaining("it would refer to two versions of type bv, version 2 (named "
            + "directly) and version 1 (through version 1 of type libx)");
    assertThat(version("r5")).isEqualTo(1);
  }

  // A wrapper of one value whose value is not field 1 is told as a wrapper of a value.
  @Test
  void aWrapperOfAValueUnderAnotherNumberIsToldAsAWrapperOfAValue() throws Exception {
    proto("x9", P3 + "\nmessage T {\n  int32 id = 5;\n  W w = 2 " + WRAPPED + ";\n\n"
        + "  message W {\n    int32 value = 5;\n  }\n}\n");
    assertThatThrownBy(() -> registry.alter("x9", new AddAttribute("n", "INT")))
        .isInstanceOf(TypeRefusedException.class)
        .hasMessageContaining("field w of message T is a message that wraps a value, which "
            + "would be written as the value itself");
  }

  // Resolved as name=subject@version, first met binds.
  private void jsonRefs(String subject, String text, List<SchemaReference> references,
      String... resolved) throws Exception {
    Map<String, String> texts = new LinkedHashMap<>();
    for (String r : resolved) {
      String[] at = r.split("[=@]");
      texts.putIfAbsent(at[0], registry.version(at[1], Integer.parseInt(at[2])).getSchema());
    }
    registry.client.register(subject, new JsonSchema(text, references, texts, null));
  }

  // A reference of no use binds a name below another to its type, though the version
  // numbers are the same.
  @Test
  void aReferenceBindsANameBelowAnotherToItsTypeAtTheSameVersion() throws Exception {
    json("bv", String.format(ROW, "bv", "k", "{\"type\":\"integer\"}", ""));
    json("bva", String.format(ROW, "bva", "p", "{\"type\":\"integer\"}", ""));
    json("ab", String.format(ROW, "ab", "in", "{\"$ref\":\"bv\"}", ""), "bv");
    jsonRefs("b4", head("b4") + "\"a\":{\"$ref\":\"ab\"}},\"additionalProperties\":false}",
        List.of(new SchemaReference("bv", "bva", 1), new SchemaReference("ab", "ab", 1)),
        "bv=bva@1", "ab=ab@1");
    assertThatThrownBy(() -> registry.alter("b4", new AddAttribute("z", "INT")))
        .isInstanceOf(TypeRefusedException.class)
        .hasMessage("Types bva and bv would both be referred to as bv, which can name only "
            + "one of them");
    assertThat(version("b4")).isEqualTo(1);
  }

  private static final String CLOSED = "},\"additionalProperties\":false}";

  private static final String NESTED = P3 + "\nmessage T {\n  int32 id = 1;\n  %s r = 2;\n"
      + "  int32 z = 3;\n  message R {\n    int32 k = 1;\n    WOneofWrapper w = 4 " + WRAPPED
      + ";\n    message WOneofWrapper {\n      oneof value {\n        int32 a = 2;\n"
      + "        string b = 3;\n      }\n    }\n  }\n}\n";

  private int numberInRow(String subject, String field) {
    return message(subject).findFieldByName("r").getMessageType().findFieldByName(field)
        .getNumber();
  }

  // The number of a wrapped oneof's field in a nested row is no new attribute's of the
  // row: where it is dropped, given another kind of type, in a row that is an element,
  // and in a later statement with its version soft-deleted.
  @Test
  void theNumberOfANestedRowsWrappedOneofIsNoNewAttributes() throws Exception {
    proto("n1", String.format(NESTED, "R"));
    registry.alter("n1", new AlterAttributeType("r", "STRUCT<k INT, n STRING>"));
    assertThat(numberInRow("n1", "n")).isEqualTo(5);
    proto("n2", String.format(NESTED, "R"));
    registry.alter("n2", new AlterAttributeType("r", "STRUCT<k INT, w STRING>"));
    assertThat(numberInRow("n2", "w")).isEqualTo(5);
    proto("n3", String.format(NESTED, "repeated R"));
    registry.alter("n3", new AlterAttributeType("r",
        "ARRAY<STRUCT<k INT, n STRING> NOT NULL> NOT NULL"));
    assertThat(numberInRow("n3", "n")).isEqualTo(5);
    proto("n4", String.format(NESTED, "R"));
    registry.alter("n4", new AlterAttributeType("r", "STRUCT<k INT>"));
    registry.softDelete("n4", 1);
    registry.alter("n4", new AlterAttributeType("r", "STRUCT<k INT, n STRING>"));
    assertThat(numberInRow("n4", "n")).isEqualTo(5);
  }

  // A type created again over its dropped versions gives a new attribute no number a
  // wrapped oneof's field had in one of them.
  @Test
  void aTypeCreatedAgainGivesNoAttributeAWrappedOneofsNumber() throws Exception {
    proto("ag", P3 + "\nmessage ag {\n  int32 id = 1;\n  int32 z = 2;\n  WOneofWrapper w = 5 "
        + WRAPPED + ";\n  message WOneofWrapper {\n    oneof value {\n      int32 a = 3;\n"
        + "      string b = 4;\n    }\n  }\n}\n");
    registry.alter("ag", new DropAttribute("w", false));
    registry.drop("ag");
    create("ag", SchemaType.PROTOBUF, "(id INT, z INT, n STRING)");
    assertThat(message("ag").findFieldByName("n").getNumber()).isEqualTo(6);
  }

  // A oneof wrapped in a message, its members numbered in the wrapper as the fields of
  // the message that holds it are in theirs: a field w of the number given.
  private static String wrapped(int number, String members) {
    return "WOneofWrapper w = " + number + " " + WRAPPED + ";\nmessage WOneofWrapper {\n"
        + "oneof value {\n" + members + "}\n}\n";
  }

  private static final String A_B = "int32 a = 1;\nstring b = 2;\n";

  private void level(String subject, String level) throws Exception {
    Config config = new Config((String) null);
    config.setCompatibilityLevel(level);
    registry.client.updateConfig(subject, config);
  }

  // The members of a wrapped oneof are the wrapper's, and none of the row that holds it:
  // once it is dropped, an attribute of a member's name is new to the row, and one the
  // row keeps of a member's name keeps its number.
  @Test
  void aMemberOfAWrappedOneofIsNoAttributeOfTheRowThatHoldsIt() throws Exception {
    String row = P3 + "\nmessage T {\nint32 id = 1;\nR r = 2;\nmessage R {\n%s}\n}\n";
    proto("m1", String.format(row, "int32 k = 1;\n" + wrapped(2, A_B)));
    level("m1", "FULL_TRANSITIVE");
    Descriptor first = message("m1");
    Descriptor r = first.findFieldByName("r").getMessageType();
    Descriptor wrapper = r.findFieldByName("w").getMessageType();
    byte[] stored = DynamicMessage.newBuilder(first).setField(first.findFieldByName("r"),
        DynamicMessage.newBuilder(r).setField(r.findFieldByName("w"),
            DynamicMessage.newBuilder(wrapper).setField(wrapper.findFieldByName("b"), "hello")
                .build()).build()).build().toByteArray();
    registry.alter("m1", new AlterAttributeType("r", "STRUCT<k INT NOT NULL, b STRING>"));
    assertThat(shape(message("m1"))).isEqualTo("id=1 r=2{k=1 b=3}");
    Descriptor now = message("m1");
    DynamicMessage read = (DynamicMessage) DynamicMessage.parseFrom(now, stored)
        .getField(now.findFieldByName("r"));
    assertThat(read.hasField(read.getDescriptorForType().findFieldByName("b"))).isFalse();
    // Both members' names, which no statement gave a number.
    proto("m2", String.format(row, "int32 k = 1;\n" + wrapped(2, A_B)));
    registry.alter("m2",
        new AlterAttributeType("r", "STRUCT<k INT NOT NULL, a INT, b STRING>"));
    assertThat(shape(message("m2"))).isEqualTo("id=1 r=2{k=1 a=3 b=4}");
    // An attribute the row keeps, of the name of a member declared before it.
    proto("m3", String.format(row, wrapped(1, "int32 k = 1;\nstring b = 9;\n")
        + "int32 k = 2;\n"));
    registry.alter("m3", new AlterAttributeType("r", "STRUCT<k INT NOT NULL>"));
    assertThat(shape(message("m3"))).isEqualTo("id=1 r=2{k=2}");
    // In a row that is an element.
    proto("m4", String.format(row, "int32 k = 1;\n" + wrapped(2, A_B))
        .replace("R r = 2;", "repeated R r = 2;"));
    registry.alter("m4", new AlterAttributeType("r",
        "ARRAY<STRUCT<k INT NOT NULL, b STRING> NOT NULL> NOT NULL"));
    assertThat(shape(message("m4"))).isEqualTo("id=1 r=2{k=1 b=3}");
  }

  // Nor is a member taken for the field of its number: a row declared after the wrapped
  // oneof, of a member's number, is followed through its versions, as a field and as a
  // member of a oneof.
  @Test
  void aMemberOfAWrappedOneofIsNotTakenForTheRowOfItsNumber() throws Exception {
    String row = "message R {\nint32 k = 1;\nint32 x = 2;\n}\n";
    proto("m5", P3 + "\nmessage m5 {\n" + wrapped(1, A_B) + "R r = 2;\n" + row + "}\n");
    level("m5", "FULL_TRANSITIVE");
    registry.alter("m5", new DropAttribute("w", false),
        new AlterAttributeType("r", "STRUCT<k INT NOT NULL>"));
    registry.alter("m5", new AlterAttributeType("r", "STRUCT<k INT NOT NULL, n INT>"));
    assertThat(shape(message("m5"))).isEqualTo("r=2{k=1 n=3}");
    proto("m6", P3 + "\nmessage m6 {\n" + wrapped(1, A_B) + "R r = 2;\n" + row + "}\n");
    registry.alter("m6", new DropAttribute("w", false),
        new AlterAttributeType("r", "STRUCT<k INT NOT NULL, x INT NOT NULL, n STRING>"));
    assertThat(shape(message("m6"))).isEqualTo("r=2{k=1 x=2 n=3}");
    proto("m7", P3 + "\nmessage m7 {\n" + wrapped(1, A_B)
        + "oneof u {\nR r = 2;\nint32 c = 3;\n}\n" + row + "}\n");
    level("m7", "FULL_TRANSITIVE");
    registry.alter("m7", new AlterAttributeType("u",
        "UNION(r STRUCT<x INT NOT NULL, n INT>, c INT)"), new DropAttribute("w", false));
    assertThat(shape(message("m7"))).isEqualTo("r=2{x=2 n=3} c=3");
  }

  // A type created again takes no member of a wrapped oneof of a dropped version for an
  // attribute of its name; and no member of a new union takes a number one had.
  @Test
  void aTypeCreatedAgainTakesNoMemberOfAWrappedOneofForAnAttribute() throws Exception {
    proto("m8", P3 + "\nmessage m8 {\nint32 k = 1;\n" + wrapped(2, A_B) + "}\n");
    level("m8", "NONE");
    registry.drop("m8");
    create("m8", SchemaType.PROTOBUF, "(k INT NOT NULL, b STRING)");
    assertThat(shape(message("m8"))).isEqualTo("k=1 b=3");
    proto("m9", P3 + "\nmessage m9 {\n" + wrapped(1, "int32 k = 1;\nstring b = 2;\n")
        + "int32 k = 2;\n}\n");
    level("m9", "NONE");
    registry.drop("m9");
    create("m9", SchemaType.PROTOBUF, "(k INT NOT NULL)");
    assertThat(shape(message("m9"))).isEqualTo("k=2");
    proto("m10", P3 + "\nmessage m10 {\nint32 id = 1;\n"
        + wrapped(2, "int32 a = 4;\nstring b = 5;\n") + "int32 z = 3;\n}\n");
    level("m10", "NONE");
    registry.drop("m10");
    create("m10", SchemaType.PROTOBUF, "(id INT NOT NULL, x UNION(a INT, b STRING), "
        + "z INT NOT NULL)");
    assertThat(fields("m10")).isEqualTo("a=6 b=7 id=1 z=3");
  }

  // A message's fields with their numbers, and those of the messages of its file that
  // they are of.
  private static String shape(Descriptor message) {
    return message.getFields().stream().map(f -> f.getName() + "=" + f.getNumber()
        + (f.getJavaType() == FieldDescriptor.JavaType.MESSAGE
          && f.getMessageType().getFile() == message.getFile()
          ? "{" + shape(f.getMessageType()) + "}" : "")).collect(Collectors.joining(" "));
  }

  // A row another type declares, given in the type itself, is numbered as that type has
  // it: an attribute it keeps has its number, and a new one a number above them, so that
  // none reads what is stored of another.
  @Test
  void aRowOfAnotherTypeGivenInTheTypeKeepsItsNumbers() throws Exception {
    create("lib", SchemaType.PROTOBUF, "(k INT NOT NULL, x INT)");
    registry.alter("lib", new DropAttribute("k", false));
    create("o1", SchemaType.PROTOBUF, "(id INT NOT NULL, r lib)");
    Descriptor first = message("o1");
    Descriptor row = first.findFieldByName("r").getMessageType();
    byte[] stored = DynamicMessage.newBuilder(first).setField(first.findFieldByName("r"),
        DynamicMessage.newBuilder(row).setField(row.findFieldByName("x"), 99).build()).build()
        .toByteArray();
    registry.alter("o1", new AlterAttributeType("r", "STRUCT<x INT, n INT>"));
    assertThat(shape(message("o1"))).isEqualTo("id=1 r=2{x=2 n=3}");
    Descriptor now = message("o1");
    DynamicMessage read = (DynamicMessage) DynamicMessage.parseFrom(now, stored)
        .getField(now.findFieldByName("r"));
    assertThat(read.getField(read.getDescriptorForType().findFieldByName("x"))).isEqualTo(99);
    assertThat(read.hasField(read.getDescriptorForType().findFieldByName("n"))).isFalse();
    // As the element of an array, the value of a map, and a row within a row.
    create("o2", SchemaType.PROTOBUF, "(id INT NOT NULL, a ARRAY<lib NOT NULL> NOT NULL, "
        + "m MAP<STRING, lib NOT NULL> NOT NULL, o STRUCT<i lib> NOT NULL)");
    String given = "STRUCT<x INT, n INT>";
    registry.alter("o2",
        new AlterAttributeType("a", "ARRAY<" + given + " NOT NULL> NOT NULL"),
        new AlterAttributeType("m", "MAP<STRING, " + given + " NOT NULL> NOT NULL"),
        new AlterAttributeType("o", "STRUCT<i " + given + "> NOT NULL"));
    assertThat(shape(message("o2"))).isEqualTo("id=1 a=2{x=2 n=3} "
        + "m=3{key=1 value=2{x=2 n=3}} o=4{i=1{x=2 n=3}}");
    assertThat(references("o2")).isEmpty();
  }

  // The numbers are those of every version of the other type that a version of the type
  // referred to, and of the files that one imports: by the names a file of a package and
  // of several messages has for them.
  @Test
  void aRowOfAnotherTypeIsNumberedAboveEveryVersionReferredTo() throws Exception {
    create("libv", SchemaType.PROTOBUF, "(k INT NOT NULL, x INT, y INT)");
    create("o3", SchemaType.PROTOBUF, "(id INT NOT NULL, r libv)");
    registry.alter("libv", new DropAttribute("y", false));
    registry.alter("o3", new AlterAttributeType("r", "libv"));
    assertThat(registry.latest("o3").getReferences())
        .containsExactly(new SchemaReference("libv", "libv", 2));
    registry.alter("o3", new AlterAttributeType("r", "STRUCT<k INT NOT NULL, n INT>"));
    assertThat(shape(message("o3"))).isEqualTo("id=1 r=2{k=1 n=4}");
    // A file of a package with two messages, and one with a single message.
    proto("two.proto", "syntax = \"proto3\";\npackage p.q;\n\nmessage R {\n  int32 k = 1;\n"
        + "  int32 x = 2;\n}\nmessage S {\n  int32 z = 1;\n}\n");
    proto("one.proto", "syntax = \"proto3\";\npackage p;\n\nmessage R {\n  int32 k = 1;\n"
        + "  int32 x = 2;\n}\n");
    for (String file : List.of("two.proto", "one.proto")) {
      String type = file.startsWith("two") ? "p.q.R" : "p.R";
      registry.client.register("o4" + file, new ProtobufSchema("syntax = \"proto3\";\n\n"
          + "import \"" + file + "\";\n\nmessage T {\n  int32 id = 1;\n  " + type + " r = 2;\n}\n",
          List.of(new SchemaReference(file, file, 1)),
          Map.of(file, registry.latest(file).getSchema()), null, null));
      registry.alter("o4" + file, new AlterAttributeType("r", "STRUCT<k INT NOT NULL, n INT>"));
      assertThat(shape(message("o4" + file))).as(file).isEqualTo("id=1 r=2{k=1 n=3}");
    }
  }

  // A type created again over dropped versions that referred to a row gives it in the
  // type with the numbers it had.
  @Test
  void aTypeCreatedAgainGivesARowOfAnotherTypeItsNumbers() throws Exception {
    create("libc", SchemaType.PROTOBUF, "(k INT NOT NULL, x INT)");
    registry.alter("libc", new DropAttribute("k", false));
    create("o5", SchemaType.PROTOBUF, "(id INT NOT NULL, r libc)");
    level("o5", "NONE");
    registry.drop("o5");
    create("o5", SchemaType.PROTOBUF, "(id INT NOT NULL, r STRUCT<x INT, n INT>)");
    assertThat(shape(message("o5"))).isEqualTo("id=1 r=2{x=2 n=3}");
  }

  // In Avro and JSON an attribute is read by its name. A JSON row of another type given
  // in the type itself has the attributes said, the reference gone; an Avro one is
  // refused, as what is stored of it would not be read.
  @Test
  void aRowOfAnotherTypeGivenInAJsonTypeIsReadByNameAndInAnAvroTypeRefused() {
    for (SchemaType format : List.of(SchemaType.AVRO, SchemaType.JSON)) {
      create("lib" + format, format, "(k INT NOT NULL, x INT)");
      registry.alter("lib" + format, new DropAttribute("k", false));
      create("o6" + format, format, "(id INT NOT NULL, r lib" + format + ")");
    }
    AlterAction given = new AlterAttributeType("r", "STRUCT<x INT, n INT>");
    refused("Attribute r.x would not read its own values, those of r.x of version 1 of type "
        + "o6AVRO", "o6AVRO", given);
    assertThat(references("o6AVRO")).containsExactly("libAVRO");
    registry.alter("o6JSON", given);
    assertThat(references("o6JSON")).isEmpty();
    assertThat(registry.schema("o6JSON").canonicalString())
        .contains("\"n\":{\"connect.index\":1,", "\"x\":{\"connect.index\":0,",
            "\"title\":\"o6JSON_r\"");
  }

  // bv at version 1 below abv, and at version 2 below cbv.
  private void bvBelowTwo() throws Exception {
    twoVersionsOfBv();
    json("cbv", String.format(ROW, "cbv", "cin", "{\"$ref\":\"bv\"}", ""), "bv");
  }

  // A type an attribute is of, read through a reference that goes with the statement, is
  // not left to another version; named again it has a reference of its own.
  @Test
  void aTypeAnAttributeIsOfIsNotLeftToAnotherVersionByAReferenceThatGoes() throws Exception {
    bvBelowTwo();
    jsonRefs("m5", head("m5") + "\"a\":{\"$ref\":\"bv\"},\"c\":{\"$ref\":\"cbv\"}" + CLOSED,
        List.of(new SchemaReference("cbv", "cbv", 1), new SchemaReference("abv", "abv", 1)),
        "bv=bv@2", "cbv=cbv@1", "abv=abv@1");
    refused("Type m5 cannot be changed so: it would no longer refer to version 2 of type bv, "
        + "which it has through a type it would refer to no more; name the type again",
        "m5", new DropAttribute("c", false));
    assertThat(version("m5")).isEqualTo(1);
    registry.alter("m5", new AlterAttributeType("a", "bv"), new DropAttribute("c", false));
    assertThat(registry.latest("m5").getReferences())
        .containsExactly(new SchemaReference("bv", "bv", 2));
  }

  // A reference the statement leaves no attribute of goes though it binds a name below
  // another that stays: the way out of two versions.
  @Test
  void aReferenceTheStatementFreesGoesThoughItBindsANameBelowAnother() throws Exception {
    bvBelowTwo();
    jsonRefs("fr", head("fr") + "\"a\":{\"$ref\":\"abv\"},\"c\":{\"$ref\":\"cbv\"}" + CLOSED,
        List.of(new SchemaReference("abv", "abv", 1), new SchemaReference("cbv", "cbv", 1)),
        "bv=bv@1", "abv=abv@1", "cbv=cbv@1");
    registry.alter("fr", new DropAttribute("a", false));
    assertThat(version("fr")).isEqualTo(2);
    assertThat(references("fr")).containsExactly("cbv");
  }

  // The document of a definition an attribute is of is kept at the version a reference of
  // no use binds it to: the two versions are told.
  @Test
  void theDocumentOfADefinitionInUseIsKeptAtItsVersion() throws Exception {
    String lq = "{\"type\":\"object\",\"title\":\"lq\",\"additionalProperties\":false,"
        + "\"$defs\":{\"Loc\":%s}}";
    json("lq", String.format(lq, P));
    json("alq", String.format(ROW, "alq", "in", "{\"$ref\":\"lq#/$defs/Loc\"}", ""), "lq");
    json("lq", String.format(lq, Q));
    jsonAt("d6", head("d6") + "\"a\":{\"$ref\":\"lq#/$defs/Loc\"}" + CLOSED, "alq@1", "lq@2");
    refused("Type d6 cannot be changed so: it would refer to two versions of type lq", "d6",
        new AddAttribute("z", "INT"));
    assertThat(version("d6")).isEqualTo(1);
  }

  // A type the statement names again is its own to move: a reference of no use that binds
  // its old version goes.
  @Test
  void aTypeTheStatementNamesAgainIsNotKeptByAReferenceOfNoUse() throws Exception {
    bvBelowTwo();
    jsonAt("o2", head("o2") + "\"a\":{\"$ref\":\"bv\"}" + CLOSED, "abv@1", "bv@1");
    registry.alter("o2", new AddAttribute("zx", "bv"));
    assertThat(registry.latest("o2").getReferences())
        .containsExactly(new SchemaReference("bv", "bv", 2));
  }

  // A reference of no use kept for another of no use that then goes is tried again.
  @Test
  void aReferenceKeptForAnotherThatGoesIsTriedAgain() throws Exception {
    bvBelowTwo();
    jsonAt("g3", head("g3") + "\"c\":{\"$ref\":\"cbv\"}" + CLOSED, "bv@2", "abv@1", "cbv@1");
    registry.alter("g3", new AddAttribute("z", "INT"));
    assertThat(references("g3")).containsExactly("cbv");
  }

  private static final String LIBS = "{\"type\":\"object\",\"title\":\"%s\",\"properties\":"
      + "{\"v\":{\"$ref\":\"#/$defs/o5s\"},\"l\":{\"$ref\":\"#/$defs/Loc\"}},"
      + "\"additionalProperties\":false,\"$defs\":{\"o5s\":{\"enum\":[\"X\"]},\"Loc\":%s}}";

  private String later(String type) {
    return "{" + LATER + head(type).substring(1);
  }

  // A name for a document that says more of a value beside its $ref is refused, as a
  // later draft reads the more; one that says no more is altered.
  @Test
  void aNameForADocumentThatSaysMoreIsRefused() throws Exception {
    json("o5", String.format(ROW, "o5", "o", "{\"type\":\"string\"}", ""));
    String rest = "\"x\":{\"$ref\":\"#/$defs/Y\"}},\"additionalProperties\":false,\"$defs\":"
        + "{\"Y\":{\"$ref\":\"o5\",\"logical.ref\":true%s}}}";
    json("sm", later("sm") + String.format(rest, ",\"minProperties\":2"), "o5");
    refused("Type sm cannot be altered: its definition Y is a name for o5 that says more of "
        + "a value beside it (minProperties)", "sm", new AddAttribute("z", "INT"));
    json("sn", later("sn") + String.format(rest, ",\"title\":\"t\""), "o5");
    registry.alter("sn", new AddAttribute("z", "INT"));
    assertThat(version("sn")).isEqualTo(2);
  }

  // A definition a document has and uses itself, named as another document the type
  // refers to, is the document's: the type names neither, and is altered.
  @Test
  void aDocumentsOwnDefinitionNamedAsAReferenceIsNotTheTypes() throws Exception {
    json("o5s", String.format(ROW, "o5s", "o", "{\"type\":\"string\"}", ""));
    json("lib", String.format(LIBS, "lib", P));
    json("n3", later("n3") + "\"a\":{\"$ref\":\"lib\"},\"w\":{\"$ref\":\"o5s\"}},"
        + "\"additionalProperties\":false}", "lib", "o5s");
    registry.alter("n3", new AddAttribute("z", "INT"));
    assertThat(references("n3")).containsExactly("lib", "o5s");
    assertThat(registry.schema("n3").canonicalString())
        .contains("{\"$ref\":\"lib\"}").contains("{\"$ref\":\"o5s\"}");
  }

  private static final String ENTRY = "\"x\":{\"$ref\":\"#/$defs/Loc\"},\"y\":{\"$ref\":\"%s\"}},"
      + "\"additionalProperties\":false,\"$defs\":{\"Loc\":{\"$ref\":\"%s#/$defs/Loc\","
      + "\"logical.ref\":true}}}";

  // A name the type has for a document's definition, and that definition named
  // directly, are one definition.
  @Test
  void aNameForADocumentsDefinitionAndTheDefinitionAreOne() throws Exception {
    json("lib", String.format(LIBS, "lib", P));
    json("q2", later("q2") + String.format(ENTRY, "lib#/$defs/Loc", "lib"), "lib");
    registry.alter("q2", new AddAttribute("z", "INT"));
    assertThat(version("q2")).isEqualTo(2);
  }

  // A schema whose $id is the reference's name is not altered: a $ref by that name is to
  // either, as the loader comes to them.
  @Test
  void aSchemaWhoseIdIsAReferencesNameIsNotAltered() throws Exception {
    json("lib", String.format(LIBS, "lib", P));
    json("n9", "{\"$id\":\"lib\"," + later("n9").substring(1)
        + String.format(ENTRY, "lib#/$defs/Loc", "lib"), "lib");
    refused("Type n9 cannot be altered: its $id, lib, is the name it has for a schema it "
        + "refers to as well", "n9", new AddAttribute("z", "INT"));
    // By the word the draft has for a schema's name: id in draft-04, and no name since.
    String named = "\"id\":\"lib\"," + head("n8").substring(1) + "\"y\":{\"$ref\":"
        + "\"lib#/$defs/Loc\"}" + CLOSED;
    json("n8", "{\"$schema\":\"http://json-schema.org/draft-04/schema#\"," + named, "lib");
    refused("Type n8 cannot be altered: its id, lib, is the name", "n8",
        new AddAttribute("z", "INT"));
    json("n7", "{" + named, "lib");
    registry.alter("n7", new AddAttribute("z", "INT"));
    assertThat(version("n7")).isEqualTo(2);
  }

  // A JSON union's branch of a type referred to whose own root may be null takes a null
  // itself: with the union's own a null matches twice, and a NOT NULL union takes one. It
  // is refused; a type whose root takes none is a branch, and a type stored so is altered.
  @Test
  void aJsonUnionsBranchOfATypeThatTakesANullIsRefused() throws Exception {
    registry.create("hu", SchemaType.JSON, BodyKind.OTHER, "INT");
    registry.create("hn", SchemaType.JSON, BodyKind.OTHER, "INT NOT NULL");
    // A name for another type, and a union, take a null too unless they say NOT NULL.
    registry.create("ha", SchemaType.JSON, BodyKind.OTHER, "hn");
    registry.create("hd", SchemaType.JSON, BodyKind.OTHER, "UNION(p hn, q STRING)");
    for (String union : List.of("UNION(a hu, b BOOLEAN)", "UNION(a hu, b BOOLEAN) NOT NULL",
        "UNION(a BOOLEAN, b UNION(c hu, d STRING))", "UNION(a ha, b BOOLEAN)",
        "UNION(a hd, b BOOLEAN)")) {
      assertThatThrownBy(() -> create("nu", SchemaType.JSON, "(id INT, u " + union + ")"))
          .as(union).isInstanceOf(TypeRefusedException.class)
          .hasMessageContaining("A UNION of attribute u in a JSON type cannot have branch")
          .hasMessageContaining("which is of a type that takes a null of its own");
    }
    create("nn", SchemaType.JSON, "(id INT, u UNION(a hn, b BOOLEAN))");
    JsonSchema written = (JsonSchema) registry.schema("nn");
    written.validate(new ObjectMapper().readTree("{\"id\":1,\"u\":null}"));
    written.validate(new ObjectMapper().readTree("{\"id\":1,\"u\":5}"));
    json("ns", head("ns") + "\"u\":{\"oneOf\":[{\"type\":\"null\"},{\"$ref\":\"hu\"},"
        + "{\"type\":\"boolean\"}]}" + CLOSED, "hu");
    registry.alter("ns", new AddAttribute("z", "INT"));
    assertThat(version("ns")).isEqualTo(2);
    refused("A UNION of attribute w in a JSON type cannot have branch a", "ns",
        new AddAttribute("w", "UNION(a hu, b BOOLEAN)"));
  }

  // A Protobuf file that imports one subject by another's name: the reference to that
  // other takes the name the file has for it, and the file still has what it imports by
  // the name, in a type created again as in one created.
  @Test
  void aReferenceNamedAsAFileImportsAnotherKeepsWhatTheFileImports() throws Exception {
    proto("a", "syntax = \"proto3\";\nmessage A {\n  int32 x = 1;\n}\n");
    proto("c", "syntax = \"proto3\";\nmessage C {\n  int32 y = 1;\n}\n");
    Map<String, String> resolved = new LinkedHashMap<>();
    resolved.put("a.proto", registry.latest("a").getSchema());
    resolved.put("a", registry.latest("c").getSchema());
    registry.client.register("pair", new ProtobufSchema("syntax = \"proto3\";\nimport "
        + "\"a.proto\";\nimport \"a\";\n\nmessage Pair {\n  A a = 1;\n  C c = 2;\n}\n",
        List.of(new SchemaReference("a.proto", "a", 1), new SchemaReference("a", "c", 1)),
        resolved, null, null));
    create("pq", SchemaType.PROTOBUF, "(id INT, p pair, q a)");
    registry.drop("pq");
    create("pq", SchemaType.PROTOBUF, "(id INT, p pair, q a, n INT)");
    assertThat(references("pq")).containsExactly("a.proto", "pair");
    assertThat(message("pq").findFieldByName("p").getMessageType().findFieldByName("c")
        .getMessageType().getName()).isEqualTo("C");
  }

  // A Protobuf type created again over its dropped versions keeps the name and package
  // its last version's root message had: what was written by it is read by that name.
  @Test
  void aProtobufTypeCreatedAgainKeepsItsRootMessagesName() throws Exception {
    proto("rn", "syntax = \"proto3\";\npackage acme;\n\nmessage T {\n  int32 id = 1;\n"
        + "  int32 z = 2;\n}\n");
    registry.drop("rn");
    create("rn", SchemaType.PROTOBUF, "(id INT, z INT, n STRING)");
    assertThat(message("rn").getFullName()).isEqualTo("acme.T");
    assertThat(fields("rn")).isEqualTo("id=1 n=3 z=2");
  }

  // A copy of one of the format's own files is that file whatever order it declares its
  // fields in: a type referring to it is created.
  @Test
  void aCopyOfAFormatsFileWithItsFieldsInAnotherOrderIsThatFile() throws Exception {
    proto("mydate", "syntax = \"proto3\";\npackage google.type;\n\nmessage Date {\n"
        + "  int32 day = 3;\n  int32 year = 1;\n  int32 month = 2;\n}\n");
    create("dd", SchemaType.PROTOBUF, "(id INT, d mydate)");
    assertThat(references("dd")).containsExactly("mydate");
  }

  // A document read into an attribute, under allOf, is the schema's own to write: a
  // definition of it named as a name the type has for another, or as a definition of the
  // type's own that no $ref is to, is read by a name of its own and written as it is.
  @Test
  void aDocumentReadIntoTheTypeHasItsDefinitionsReadByNamesOfTheirOwn() throws Exception {
    json("o5", String.format(ROW, "o5", "o", "{\"type\":\"string\"}", ""));
    json("via2", "{\"type\":\"object\",\"title\":\"via2\",\"properties\":{\"v\":{\"$ref\":"
        + "\"#/$defs/Y\"}},\"additionalProperties\":false,\"$defs\":{\"Y\":{\"enum\":[\"A\"]}}}");
    json("ra", later("ra") + "\"a\":{\"$ref\":\"#/$defs/Y\"},\"b\":{\"allOf\":[{\"$ref\":"
        + "\"via2\"}]}},\"additionalProperties\":false,\"$defs\":{\"Y\":{\"$ref\":\"o5\","
        + "\"logical.ref\":true}}}", "o5", "via2");
    registry.alteredTo("ra", "{\"$ref\":\"o5\"}", TestRegistry.alias("Ref1", "via2#/$defs/Y"));
    json("lib2", String.format(LIBS, "lib2", P));
    json("ru", later("ru") + "\"b\":{\"allOf\":[{\"$ref\":\"lib2\"}]}},"
        + "\"additionalProperties\":false,\"$defs\":{\"Loc\":" + Q + "}}", "lib2");
    registry.alteredTo("ru", TestRegistry.alias("Ref1", "lib2#/$defs/Loc"),
        "\"required\":[\"q\"]");
  }

  // A document's definition under definitions is come to by $defs too, and what it names
  // is read: the document's Loc beside the type's own, by a name of its own. One
  // definition named by both words is one, and the type is altered again.
  @Test
  void aDefinitionUnderDefinitionsIsOneByEitherWord() throws Exception {
    json("libd", "{\"type\":\"object\",\"title\":\"libd\",\"additionalProperties\":false,"
        + "\"definitions\":{\"B\":{\"type\":\"object\",\"properties\":{\"in\":{\"$ref\":"
        + "\"#/definitions/Loc\"}},\"additionalProperties\":false},\"Loc\":" + P + "}}");
    json("db", head("db") + "\"x\":{\"$ref\":\"#/$defs/Loc\"},\"y\":{\"allOf\":[{\"$ref\":"
        + "\"libd#/$defs/B\"}]}},\"additionalProperties\":false,\"$defs\":{\"Loc\":" + Q + "}}",
        "libd");
    registry.alteredTo("db", TestRegistry.alias("Ref1", "libd#/$defs/Loc"),
        "{\"$ref\":\"#/$defs/Loc\"}");
    json("dt", head("dt") + "\"a\":{\"$ref\":\"libd#/definitions/Loc\"},\"b\":{\"$ref\":"
        + "\"#/$defs/Y\"}},\"additionalProperties\":false,\"$defs\":{\"Y\":{\"type\":\"object\","
        + "\"properties\":{\"v\":{\"$ref\":\"libd#/definitions/Loc\"}}}}}", "libd");
    registry.alter("dt", new AddAttribute("z", "INT"));
    registry.alter("dt", new AddAttribute("z2", "INT"));
    assertThat(version("dt")).isEqualTo(3);
    // One definition, said by either word, is read by one name.
    for (int version = 1; version <= 3; version++) {
      assertThat(JsonToLogicalTypeConverter.toLogicalType((JsonSchema) registry.parse(
          registry.version("dt", version))).getExternalImports()).as("version " + version)
          .isEmpty();
    }
  }

  private static Schema of(LogicalType lt, Schema type) {
    return type.getType() == Schema.Type.NAMED_TYPE_REF
        ? lt.getNamedTypes().get(type.getQualifiedName()) : type;
  }

  // Two documents that each define a name: a type of both reads each by its own, where
  // it is created and where it is altered, and takes the values it took.
  @Test
  void aTypeOfTwoDocumentsThatEachDefineANameReadsEachByItsOwn() throws Exception {
    String doc = "{\"type\":\"object\",\"title\":\"%s\",\"properties\":{\"i\":{\"$ref\":"
        + "\"#/$defs/Inner\"}},\"additionalProperties\":false,\"$defs\":{\"Inner\":%s}}";
    json("t1", String.format(doc, "t1", P));
    json("t2", String.format(doc, "t2", Q));
    create("tw", SchemaType.JSON, "(id INT, a t1, b t2)");
    for (int version = 1; version <= 2; version++) {
      JsonSchema schema = (JsonSchema) registry.schema("tw");
      LogicalType read = JsonToLogicalTypeConverter.toLogicalType(schema);
      Schema row = of(read, read.getRootSchema());
      assertThat(of(read, of(read, row.getField("a").getSchema()).getField("i").getSchema())
          .getField("p")).as("a.i at version " + version).isNotNull();
      assertThat(of(read, of(read, row.getField("b").getSchema()).getField("i").getSchema())
          .getField("q")).as("b.i at version " + version).isNotNull();
      assertThat(takes(schema, "{\"id\":1,\"a\":{\"i\":{\"p\":4}},\"b\":{\"i\":{\"q\":\"s\"}}}"))
          .isTrue();
      assertThat(takes(schema, "{\"id\":1,\"a\":{\"i\":{\"q\":\"s\"}},\"b\":{\"i\":{\"p\":4}}}"))
          .isFalse();
      if (version == 1) {
        registry.alter("tw", new AddAttribute("z", "INT"));
      }
    }
  }

  private static boolean takes(JsonSchema schema, String value) throws Exception {
    try {
      schema.validate(new ObjectMapper().readTree(value));
      return true;
    } catch (ValidationException e) {
      return false;
    }
  }

  // A definition's name is its key, however the $ref to it is escaped: the type has one
  // definition of it, of its own or a document's, and is altered. A $ref says a plus as
  // %2B: a plus in it is a space to the loader.
  @Test
  void aDefinitionsNameIsItsKeyHoweverTheRefEscapesIt() throws Exception {
    String[][] keys = {{"Stra\u00dfe", "Stra%C3%9Fe"}, {"a/b", "a~1b"}, {"a~b", "a~0b"},
        {"a%b", "a%25b"}, {"a b", "a%20b"}, {"a b", "a+b"}, {"a+b", "a%2Bb"},
        {"a#b", "a%23b"}, {"a^b", "a%5Eb"}, {"a{b}", "a%7Bb%7D"}};
    for (int i = 0; i < keys.length; i++) {
      String key = keys[i][0];
      json("el" + i, "{\"type\":\"object\",\"title\":\"el" + i + "\",\"additionalProperties\":"
          + "false,\"$defs\":{\"" + key + "\":" + P + "}}");
      for (String document : List.of("", "el" + i)) {
        String type = (document.isEmpty() ? "es" : "ed") + i;
        String[] refs = document.isEmpty() ? new String[0] : new String[] {document};
        json(type, later(type) + "\"x\":{\"$ref\":\"" + document + "#/$defs/" + keys[i][1]
            + "\"}},\"additionalProperties\":false,\"$defs\":{\"" + key + "\":" + Q + "}}", refs);
        registry.alter(type, new AddAttribute("z", "INT"));
        JsonSchema written = (JsonSchema) registry.schema(type);
        assertThat(written.toJsonNode().path("$defs").has(key)).as(type + " " + key).isTrue();
        assertThat(takes(written, "{\"x\":{\"q\":\"s\"}}")).as(type + " " + key)
            .isEqualTo(document.isEmpty());
        assertThat(takes(written, "{\"x\":{\"p\":1}}")).as(type + " " + key)
            .isEqualTo(!document.isEmpty());
        // And again, by what the first statement wrote.
        registry.alter(type, new AddAttribute("z2", "INT"));
        assertThat(version(type)).as(type + " " + key).isEqualTo(3);
      }
    }
  }

  private LogicalType read(String subject) {
    return JsonToLogicalTypeConverter.toLogicalType((JsonSchema) registry.schema(subject));
  }

  // Under a root $id the loader calls a document by its name resolved against the id: the
  // type has it by the name the schema has for it, and its own definitions by no document.
  @Test
  void aDocumentIsToldByItsNameUnderARootId() throws Exception {
    json("lib", LIB);
    String id = "{\"$id\":\"x-test://ids/ti.json\",";
    json("ri", id + later("ri").substring(1) + "\"x\":{\"$ref\":\"#/$defs/Loc\"},\"y\":{\"$ref\":"
        + "\"lib#/$defs/Loc\"}},\"additionalProperties\":false,\"$defs\":{\"Loc\":" + Q + "}}",
        "lib");
    assertThat(read("ri").getExternalImports()).containsEntry("Ref1", "lib#/$defs/Loc");
    registry.alteredTo("ri", TestRegistry.alias("Ref1", "lib#/$defs/Loc"));
    // A definition of the document's own name is the document's.
    json("self", "{\"type\":\"object\",\"title\":\"self\",\"additionalProperties\":false,"
        + "\"$defs\":{\"self\":" + P + "}}");
    json("rs", id + head("rs").substring(1) + "\"y\":{\"$ref\":\"self#/$defs/self\"}" + CLOSED,
        "self");
    registry.alteredTo("rs", "{\"$ref\":\"self#/$defs/self\"}");
    // And a definition of the schema's own that another names is its own.
    json("rn", id + later("rn").substring(1) + "\"x\":{\"$ref\":\"#/$defs/A\"}},"
        + "\"additionalProperties\":false,\"$defs\":{\"A\":{\"type\":\"object\",\"properties\":"
        + "{\"b\":{\"$ref\":\"#/$defs/B\"}}},\"B\":" + Q + "}}");
    assertThat(read("rn").getExternalTypes()).isEmpty();
    registry.alteredTo("rn", "{\"$ref\":\"#/$defs/B\"}");
  }

  // A document with one key under $defs and under definitions has two definitions of it:
  // a type of either cannot say which, and is refused.
  @Test
  void aDocumentsKeyUnderBothWordsIsTwoDefinitions() throws Exception {
    json("bw", "{\"type\":\"object\",\"title\":\"bw\",\"additionalProperties\":false,"
        + "\"$defs\":{\"Loc\":" + Q + "},\"definitions\":{\"Loc\":" + P + "}}");
    json("tb", head("tb") + "\"x\":{\"$ref\":\"bw#/definitions/Loc\"}" + CLOSED, "bw");
    refused("Type tb cannot be changed so: no JSON schema can be made of it: Duplicate named "
        + "type: Loc", "tb", new AddAttribute("z", "INT"));
  }

  // In an older draft a part of a document that is no definition is said by the same $ref
  // in every document: each is read as its own document's.
  @Test
  void partsOfTwoDocumentsSaidAlikeAreEachTheirOwn() throws Exception {
    String part = "{\"type\":\"object\",\"title\":\"%s\",\"properties\":{\"a\":%s,\"b\":"
        + "{\"$ref\":\"#/properties/a\"}},\"additionalProperties\":false}";
    json("pa1", String.format(part, "pa1", P));
    json("pa2", String.format(part, "pa2", Q));
    json("pp", head("pp") + "\"x\":{\"$ref\":\"pa1\"},\"y\":{\"$ref\":\"pa2\"}" + CLOSED,
        "pa1", "pa2");
    LogicalType read = read("pp");
    Schema row = of(read, read.getRootSchema());
    assertThat(of(read, of(read, row.getField("x").getSchema()).getField("b").getSchema())
        .getField("p")).isNotNull();
    assertThat(of(read, of(read, row.getField("y").getSchema()).getField("b").getSchema())
        .getField("q")).isNotNull();
  }

  // A reference that moves to a version whose root takes a null gives a stored union's
  // branch a null of its own: refused, as adding such a branch is.
  @Test
  void aReferenceMovedToAVersionThatTakesANullIsRefusedForAUnionsBranch() throws Exception {
    registry.create("g", SchemaType.JSON, BodyKind.OTHER, "INT NOT NULL");
    create("tg", SchemaType.JSON, "(id INT, u UNION(a g, b BOOLEAN))");
    registry.alter("g", new SetDataType("INT"));
    refused("Type tg cannot be changed so: its reference would move, type g from version 1 "
        + "to version 2, and at that version branch a of the UNION of attribute u is of a "
        + "type that takes a null of its own", "tg", new AddAttribute("w", "g"));
    assertThat(version("tg")).isEqualTo(1);
  }

  // A definition of a document an attribute is of moves with the document's version: one
  // read through a reference that goes with the statement is refused, and altered once the
  // statement names the document's type itself, as the refusal says.
  @Test
  void aDefinitionInUseIsNotLeftToAnotherVersionByAReferenceThatGoes() throws Exception {
    String lq = "{\"type\":\"object\",\"title\":\"lq\",\"additionalProperties\":false,"
        + "\"$defs\":{\"Loc\":%s}}";
    json("lq", String.format(lq, P));
    json("alq", String.format(ROW, "alq", "in", "{\"$ref\":\"lq#/$defs/Loc\"}", ""), "lq");
    json("lq", String.format(lq, Q));
    jsonAt("dm", head("dm") + "\"a\":{\"$ref\":\"lq#/$defs/Loc\"},\"c\":{\"$ref\":\"alq\"}"
        + CLOSED, "alq@1", "lq@2");
    refused("Type dm cannot be changed so: it would no longer refer to version 1 of type lq",
        "dm", new DropAttribute("c", false));
    jsonAt("dw", head("dw") + "\"a\":{\"$ref\":\"lq\"},\"c\":{\"$ref\":\"alq\"}" + CLOSED,
        "alq@1", "lq@2");
    registry.alter("dw", new AlterAttributeType("a", "lq"), new DropAttribute("c", false));
    assertThat(registry.latest("dw").getReferences())
        .containsExactly(new SchemaReference("lq", "lq", 2));
  }

  // A definition of the type's own named as a type below a reference that goes is no type
  // the type refers to: the statement is taken.
  @Test
  void anOwnDefinitionNamedAsATypeBelowAReferenceThatGoesIsItsOwn() throws Exception {
    bvBelowTwo();
    jsonRefs("ob", head("ob") + "\"a\":{\"$ref\":\"abv\"},\"b\":{\"$ref\":\"#/$defs/bv\"}},"
        + "\"additionalProperties\":false,\"$defs\":{\"bv\":" + Q + "}}",
        List.of(new SchemaReference("abv", "abv", 1)), "bv=bv@1", "abv=abv@1");
    registry.alter("ob", new DropAttribute("a", false));
    assertThat(references("ob")).isEmpty();
  }

  // In a later draft what is beside a $ref is read and written again: a place there is
  // one the type may refer to.
  @Test
  void aPlaceBesideARefThatALaterDraftReadsIsAltered() throws Exception {
    json("pb", later("pb") + "\"x\":{\"$ref\":\"#/$defs/A/properties/e\"}},"
        + "\"additionalProperties\":false,\"$defs\":{\"A\":{\"$ref\":\"#/$defs/Loc\","
        + "\"properties\":{\"e\":" + P + "}},\"Loc\":{\"type\":\"object\",\"properties\":"
        + "{\"q\":{\"type\":\"string\"}}}}}");
    JsonSchema stored = (JsonSchema) registry.schema("pb");
    registry.alter("pb", new AddAttribute("z", "INT"));
    for (String value : List.of("{\"x\":{\"p\":1}}", "{\"x\":{\"q\":\"s\"}}")) {
      assertThat(takes((JsonSchema) registry.schema("pb"), value)).as(value)
          .isEqualTo(takes(stored, value));
    }
  }

  // A $schema the loader knows for no draft is read as an older one, where what is beside
  // a $ref is not read: no second definition is come to, and the type is altered.
  @Test
  void aSchemaOfNoDraftTheLoaderKnowsIsReadAsAnOlderOne() throws Exception {
    json("lib", LIB);
    json("nd", "{\"$schema\":\"x-test://meta/draft/2020-12/schema\"," + head("nd").substring(1)
        + "\"x\":{\"$ref\":\"#/$defs/Loc\",\"properties\":{\"zz\":{\"$ref\":\"lib#/$defs/Loc\"}}}},"
        + "\"additionalProperties\":false,\"$defs\":{\"Loc\":" + Q + "}}", "lib");
    registry.alter("nd", new AddAttribute("z", "INT"));
    assertThat(version("nd")).isEqualTo(2);
  }

  // The document of a definition the statement leaves no attribute of goes with it,
  // though it binds a name below another that stays: the way out of two versions.
  @Test
  void theDocumentOfADefinitionTheStatementFreesGoes() throws Exception {
    bvBelowTwo();
    jsonRefs("fd", head("fd") + "\"a\":{\"$ref\":\"libx#/$defs/Loc\"},\"c\":{\"$ref\":\"cbv\"}"
        + CLOSED, List.of(new SchemaReference("libx", "libx", 1),
        new SchemaReference("cbv", "cbv", 1)), "bv=bv@1", "libx=libx@1", "cbv=cbv@1");
    registry.alter("fd", new DropAttribute("a", false));
    assertThat(version("fd")).isEqualTo(2);
    assertThat(references("fd")).containsExactly("cbv");
  }

  // References as name=subject, at the latest version.
  private void jsonNamed(String subject, String text, String... refs) throws Exception {
    List<SchemaReference> references = new ArrayList<>();
    Map<String, String> resolved = new LinkedHashMap<>();
    for (String ref : refs) {
      String[] named = ref.split("=");
      SchemaMetadata md = registry.latest(named[1]);
      references.add(new SchemaReference(named[0], named[1], md.getVersion()));
      resolved.put(named[0], md.getSchema());
    }
    registry.client.register(subject, new JsonSchema(text, references, resolved, null));
  }

  private static final String OWN_LOC = "},\"additionalProperties\":false,\"$defs\":{\"Loc\":" + Q
      + "}}";

  // An $id written with a # after it names the schema all the same.
  @Test
  void anIdWithAHashAfterItNamesTheSchema() throws Exception {
    json("l7", "{\"$id\":\"x-test://l7/l7.json#\"," + later("l7").substring(1)
        + "\"x\":{\"$ref\":\"#/$defs/Loc\"},\"y\":{\"$ref\":\"x-test://l7/l7.json#/$defs/Loc\"}"
        + OWN_LOC);
    registry.alter("l7", new AddAttribute("z", "INT"));
    assertThat(version("l7")).isEqualTo(2);
  }

  // A document whose whole name begins as the $id does is told by the name the
  // schema refers to it by.
  @Test
  void aDocumentWhoseWholeNameBeginsAsTheIdIsToldByItsName() throws Exception {
    json("lib", String.format(LIBS, "lib", P));
    jsonNamed("l9", "{\"$id\":\"x-test://q/a.json\"," + later("l9").substring(1)
        + "\"x\":{\"$ref\":\"#/$defs/Loc\"},\"y\":{\"$ref\":\"a.json2#/$defs/Loc\"}" + OWN_LOC,
        "x-test://q/a.json2=lib");
    registry.alteredTo("l9",
        TestRegistry.alias("Ref1", "x-test://q/a.json2#/$defs/Loc"),
        "{\"$ref\":\"#/$defs/Loc\"}");
  }

  // A definition under definitions that no $ref is to is read by a later draft too.
  @Test
  void aDefinitionUnderDefinitionsReadThoughNoRefIsToItIsOneOfItsName() throws Exception {
    json("lib", String.format(LIBS, "lib", P));
    json("y2", later("y2") + "\"y\":{\"$ref\":\"lib#/$defs/Loc\"}},\"additionalProperties\":"
        + "false,\"definitions\":{\"Loc\":" + Q + "}}", "lib");
    registry.alteredTo("y2", TestRegistry.alias("Ref1", "lib#/$defs/Loc"));
  }

  // A name a document has for another's definition is that document's to read: the type
  // writes its own of that name and the document whole, and is altered.
  @Test
  void aDocumentsNameForAnothersDefinitionIsTheDocuments() throws Exception {
    json("other", String.format(LIBS, "other", Q));
    json("libe", "{" + LATER + "\"type\":\"object\",\"title\":\"libe\",\"properties\":{\"l\":"
        + "{\"$ref\":\"#/$defs/A\"}},\"additionalProperties\":false,\"$defs\":{\"A\":{\"$ref\":"
        + "\"other#/$defs/Loc\",\"logical.ref\":true}}}", "other");
    json("z6", later("z6") + "\"x\":{\"$ref\":\"#/$defs/Loc\"},\"a\":{\"$ref\":\"libe\"}},"
        + "\"additionalProperties\":false,\"$defs\":{\"Loc\":" + P + "}}", "libe", "other");
    registry.alter("z6", new AddAttribute("z", "INT"));
    assertThat(registry.schema("z6").canonicalString()).contains("{\"$ref\":\"#/$defs/Loc\"}")
        .contains("{\"$ref\":\"libe\"}").contains("\"required\":[\"p\"]");
  }

  // A document that names its own definition by its $id is told by the name the
  // schema has for the document.
  @Test
  void aDocumentsDefinitionNamedByTheDocumentsIdIsItsOwn() throws Exception {
    json("libi", "{\"$id\":\"x-test://l/libi.json\",\"type\":\"object\",\"title\":\"libi\","
        + "\"properties\":{\"l\":{\"$ref\":\"x-test://l/libi.json#/$defs/Loc\"}},"
        + "\"additionalProperties\":false,\"$defs\":{\"Loc\":" + P + "}}");
    json("z17", later("z17") + "\"y\":{\"$ref\":\"libi#/$defs/Loc\"},\"a\":{\"$ref\":\"libi\"}},"
        + "\"additionalProperties\":false}", "libi");
    registry.alter("z17", new AddAttribute("z", "INT"));
    assertThat(version("z17")).isEqualTo(2);
  }

  // In a later draft the type's own definitions are read before what any of them names,
  // in whatever order they are: a document's of the name of one is read apart from it.
  @Test
  void aDocumentsDefinitionIsReadApartWhateverTheOrderOfTheTypesOwn() throws Exception {
    json("lib", String.format(LIBS, "lib", P));
    String row = "{\"type\":\"object\",\"properties\":{\"k\":{\"$ref\":\"%s\"}}}";
    json("z18", later("z18") + "\"x\":{\"$ref\":\"#/$defs/A\"},\"y\":{\"$ref\":\"#/$defs/B\"}},"
        + "\"additionalProperties\":false,\"$defs\":{\"A\":" + String.format(row, "lib#/$defs/Loc")
        + ",\"B\":" + String.format(row, "#/$defs/Loc") + ",\"Loc\":" + Q + "}}", "lib");
    registry.alteredTo("z18", TestRegistry.alias("Ref1", "lib#/$defs/Loc"),
        "{\"$ref\":\"#/$defs/Loc\"}", "{\"$ref\":\"#/$defs/Ref1\"}");
    // And again, with the names the first statement wrote.
    registry.alter("z18", new AddAttribute("z2", "INT"));
    assertThat(version("z18")).isEqualTo(3);
  }

  // A definition written by hand that is only a $ref to another document's is a name for
  // that, as one the writer writes is: read through whichever draft's loader.
  @Test
  void aDefinitionThatIsOnlyARefToAnotherDocumentsIsANameForIt() throws Exception {
    json("lib", String.format(LIBS, "lib", P));
    String named = "\"x\":{\"$ref\":\"#/$defs/A\"}},\"additionalProperties\":false,\"$defs\":"
        + "{\"A\":{\"$ref\":\"lib#/$defs/Loc\"}}}";
    json("n1", later("n1") + named, "lib");
    registry.alteredTo("n1", TestRegistry.alias("A", "lib#/$defs/Loc"));
    json("n2", head("n2") + named, "lib");
    registry.alteredTo("n2", TestRegistry.alias("A", "lib#/$defs/Loc"));
    // A name for the document whole is written as the document; one of no use goes.
    json("n3", head("n3") + "\"x\":{\"$ref\":\"#/$defs/A\"}},\"additionalProperties\":false,"
        + "\"$defs\":{\"A\":{\"$ref\":\"lib\"},\"B\":{\"$ref\":\"lib#/$defs/Loc\"}}}", "lib");
    registry.alteredTo("n3", "\"x\":{\"connect.index\":1,\"oneOf\":[{\"type\":\"null\"},"
        + "{\"$ref\":\"lib\"}]}");
    assertThat(registry.schema("n3").canonicalString()).doesNotContain("$defs");
  }

  // A $ref under not, if, then or else, where no type is read (what
  // else a row may have), is not one the schema is read through.
  @Test
  void aRefUnderNotIfThenOrElseWhereNoTypeIsReadIsNotFollowed() throws Exception {
    json("lib", String.format(LIBS, "lib", P));
    for (String word : List.of("not", "if", "then", "else")) {
      String type = "k" + word;
      json(type, later(type) + "\"x\":{\"$ref\":\"#/$defs/Loc\"},\"y\":{\"type\":\"object\","
          + "\"properties\":{\"k\":{\"type\":\"integer\"}},\"additionalProperties\":{\"" + word
          + "\":{\"$ref\":\"lib#/$defs/Loc\"}}}" + OWN_LOC, "lib");
      registry.alter(type, new AddAttribute("z", "INT"));
      assertThat(version(type)).as(word).isEqualTo(2);
    }
  }

  private void refused(String told, String subject, AlterAction... actions) {
    assertThatThrownBy(() -> registry.alter(subject, actions)).as(told)
        .isInstanceOf(TypeRefusedException.class).hasMessageStartingWith(told);
  }

  // An attribute the type has not is told by its quoted name by a
  // move, a rename, a type given and a property set.
  @Test
  void anAttributeTheTypeHasNotIsToldByItsQuotedName() {
    create("av", SchemaType.AVRO, "(id INT NOT NULL, ex INT)");
    String none = "Type av has no attribute `n.x`";
    refused(none, "av", new MoveAttribute("n.x", Position.first()));
    refused(none, "av", new RenameAttribute("n.x", "y"));
    refused(none, "av", new AlterAttributeType("n.x", "INT"));
    refused(none, "av", new SetAttributeWith("n.x", Map.of("owner", "z")));
  }

  // An attribute the statement adds is told by its
  // quoted name in what the statement is then refused for.
  @Test
  void anAttributeTheStatementAddsIsToldByItsQuotedName() {
    create("av", SchemaType.AVRO, "(id INT NOT NULL, ex INT)");
    refused("Attribute `a.b` is given more than one type and its clauses", "av",
        new AddAttribute("a.b", "INT, c STRING"));
    refused("Type av already has attribute `e.x`", "av", new AddAttribute("e.x", "INT"),
        new RenameAttribute("ex", "e.x"));
    create("pr", SchemaType.PROTOBUF, "(id INT NOT NULL, ex INT, uv UNION(a INT, b STRING))");
    refused("Property flink.wrapped of attribute `e.x` cannot be given in a PROTOBUF type",
        "pr", new AddAttribute("e.x", "INT"),
        new SetAttributeWith("e.x", Map.of("flink.wrapped", "true")));
    refused("Attribute `u.v` cannot be placed first in PROTOBUF type pr", "pr",
        new AddAttribute("u.v", "UNION(p INT, q STRING)", Position.first()));
    refused("Attribute `f.g` cannot be placed after `u.v` in PROTOBUF type pr", "pr",
        new AddAttribute("u.v", "UNION(p INT, q STRING)"),
        new AddAttribute("f.g", "INT", Position.after("u.v")));
    String five = "INT WITH ('protobuf.field.number' = '5')";
    refused("Attribute `i.j` cannot have the Protobuf field number 5: attribute `g.h` has it",
        "pr", new AddAttribute("g.h", "INT"), new AddAttribute("i.j", five));
    refused("Attribute `g.h` cannot have the Protobuf field number 2: attribute ex has it",
        "pr", new AddAttribute("g.h", "INT WITH ('protobuf.field.number' = '2')"));
    refused("Branch p of attribute `n.u` cannot be given a Protobuf field number", "pr",
        new AddAttribute("n.u", "UNION(p INT WITH ('protobuf.field.number' = '9'), q STRING)"));
  }

  // An attribute of a JSON type, which may have a dot in its name, is told by
  // its quoted name: one that holds a map stored as entries, and one given a property of
  // another format.
  @Test
  void aJsonAttributeIsToldByItsQuotedName() throws Exception {
    String entries = "{\"type\":\"array\",\"connect.type\":\"map\",\"items\":{\"type\":"
        + "\"object\",\"properties\":{\"key\":{\"type\":\"string\"},\"value\":{\"type\":"
        + "\"integer\"}}}}";
    json("je", head("je") + "\"e.x\":" + entries + "},\"additionalProperties\":false}");
    refused("Type je cannot be altered: attribute `e.x` has a map stored as an array", "je",
        new AddAttribute("z", "INT"));
    create("js", SchemaType.JSON, "(id INT NOT NULL, `e.x` INT)");
    refused("Property avro.aliases of attribute `e.x` cannot be set", "js",
        new SetAttributeWith("e.x", Map.of("avro.aliases", "z")));
  }

  // In a context, a reference of no use whose name another has below it for the same
  // type and version binds nothing, and is dropped.
  @Test
  void aReferenceOfNoUseInAContextIsDroppedWhereItBindsNothing() throws Exception {
    registry.create(":.c:inner", "inner", SchemaType.JSON, BodyKind.STRUCT, "(x STRING)");
    registry.create(":.c:pair", "pair", SchemaType.JSON, BodyKind.STRUCT, "(a STRING, i inner)");
    assertThat(registry.latest(":.c:pair").getReferences())
        .containsExactly(new SchemaReference("inner", "inner", 1));
    Map<String, String> resolved = new LinkedHashMap<>();
    resolved.put("inner", registry.latest(":.c:inner").getSchema());
    resolved.put("pair", registry.latest(":.c:pair").getSchema());
    registry.client.register(":.c:t", new JsonSchema(head("t") + "\"a\":{\"$ref\":\"pair\"}},"
        + "\"additionalProperties\":false}", List.of(new SchemaReference("inner", "inner", 1),
        new SchemaReference("pair", "pair", 1)), resolved, null));
    registry.alter(":.c:t", new AddAttribute("z", "INT"));
    assertThat(references(":.c:t")).containsExactly("pair");
  }
}
