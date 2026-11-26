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
import static org.junit.jupiter.api.Assertions.assertTimeoutPreemptively;

import com.google.protobuf.Descriptors.Descriptor;
import io.confluent.kafka.schemaregistry.ParsedSchema;
import io.confluent.kafka.schemaregistry.avro.AvroSchema;
import io.confluent.kafka.schemaregistry.client.SchemaMetadata;
import io.confluent.kafka.schemaregistry.client.rest.entities.Config;
import io.confluent.kafka.schemaregistry.client.rest.entities.SchemaReference;
import io.confluent.kafka.schemaregistry.json.JsonSchema;
import io.confluent.kafka.schemaregistry.protobuf.ProtobufSchema;
import io.confluent.kafka.schemaregistry.type.logical.LogicalType;
import io.confluent.kafka.schemaregistry.type.logical.SchemaType;
import io.confluent.kafka.schemaregistry.type.logical.json.LogicalTypeToJsonConverter;
import io.confluent.kafka.schemaregistry.type.logical.statements.AlterAction.AddAttribute;
import io.confluent.kafka.schemaregistry.type.logical.statements.AlterAction.AlterAttributeType;
import io.confluent.kafka.schemaregistry.type.logical.statements.AlterAction.DropAttribute;
import io.confluent.kafka.schemaregistry.type.logical.statements.AlterAction.RenameAttribute;
import io.confluent.kafka.schemaregistry.type.logical.statements.TypeScripts.BodyKind;
import java.time.Duration;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.junit.jupiter.api.Test;

/**
 * Edges of what ALTER TYPE and CREATE TYPE refuse and leave: each a rule that a change of
 * one word of the code would break with no other test to tell it.
 */
class TypeAlterEdgeCasesTest {

  private static final String WRAPPED = "[(confluent.field_meta) = {\n    params: [{\n      "
      + "key: \"flink.wrapped\",\n      value: \"true\"\n    }]\n  }]";
  private static final String P3 = "syntax = \"proto3\";\n\nimport \"confluent/meta.proto\";\n";
  private static final String JSON_HEAD = "{\"type\":\"object\",\"title\":\"%s\","
      + "\"properties\":{\"id\":{\"type\":\"integer\"},";
  // A oneof wrapped by hand in a message, members a=1 and b=3; %s is what else T has.
  private static final String WRAPPED_ONEOF = P3 + "\nmessage T {\n  int32 id = 5;\n"
      + "  WOneofWrapper w = 2 " + WRAPPED + ";\n  int32 z = 6;\n%s\n  message WOneofWrapper {\n"
      + "    oneof value {\n      int32 a = 1;\n      string b = 3;\n    }\n  }\n}\n";
  private static final String IS_WRAPPED = "field w of message T is a oneof wrapped in a "
      + "message";

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

  private void proto(String subject, String text, String... refs) throws Exception {
    List<SchemaReference> references = new ArrayList<>();
    Map<String, String> resolved = new LinkedHashMap<>();
    for (String ref : refs) {
      SchemaMetadata md = registry.latest(ref);
      references.add(new SchemaReference(ref, ref, md.getVersion()));
      resolved.put(ref, md.getSchema());
    }
    registry.client.register(subject,
        new ProtobufSchema(text, references, resolved, null, null));
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

  private void nativeJson(String subject, String script, String... refs) throws Exception {
    Map<String, String> resolved = new LinkedHashMap<>();
    List<SchemaReference> references = new ArrayList<>();
    for (String r : refs) {
      resolved.put(r, registry.latest(r).getSchema());
      references.add(new SchemaReference(r, r, registry.latest(r).getVersion()));
    }
    String text = LogicalTypeToJsonConverter.fromLogicalType(TypeScripts.withReferences(
        TypeScripts.parse(script), references, resolved), subject).canonicalString();
    registry.client.register(subject, new JsonSchema(text, references, resolved, null));
  }

  private List<String> references(String subject) {
    List<SchemaReference> refs = registry.latest(subject).getReferences();
    return refs == null ? List.of()
        : refs.stream().map(SchemaReference::getName).sorted().collect(Collectors.toList());
  }

  private static LogicalType struct(String body) {
    return TypeScripts.parse(TypeScripts.createScript("t", BodyKind.STRUCT, body, Map.of(),
        Map.of()));
  }

  // A wrapper of a message another file declares is refused as the wrapper it is.
  @Test
  void aWrapperOfAMessageOfAnotherFileIsRefused() throws Exception {
    proto("imp", "syntax = \"proto3\";\n\npackage o;\n\nmessage In {\n  int32 k = 1;\n}\n");
    proto("v11", P3 + "import \"imp\";\n\nmessage T {\n  int32 id = 5;\n  WWrapper w = 2 "
        + WRAPPED + ";\n  int32 z = 6;\n\n  message WWrapper {\n    o.In value = 1;\n  }\n}\n",
        "imp");
    assertThatThrownBy(() -> registry.alter("v11", new AddAttribute("n", "INT")))
        .isInstanceOf(TypeRefusedException.class)
        .hasMessageContaining("field w of message T is a message that wraps a value");
    assertThat(version("v11")).isEqualTo(1);
  }

  // A wrapper named as one of the format's own messages is told as a wrapper first.
  @Test
  void aWrapperNamedAsOneOfTheFormatsOwnIsToldAsAWrapper() throws Exception {
    proto("dn", "syntax = \"proto3\";\n\nimport \"confluent/meta.proto\";\n\n"
        + "package confluent.type;\n\nmessage Decimal {\n  int32 value = 1;\n}\n");
    proto("v12", P3 + "import \"dn\";\n\nmessage T {\n  int32 id = 5;\n  confluent.type.Decimal "
        + "w = 2 " + WRAPPED + ";\n  int32 z = 6;\n}\n", "dn");
    assertThatThrownBy(() -> registry.alter("v12", new AddAttribute("n", "INT")))
        .isInstanceOf(TypeRefusedException.class)
        .hasMessageContaining("field w of message T is a message that wraps a value");
  }

  // A wrapped oneof given another union type is written as a oneof of its name, of
  // other members: refused by the name alone. (Pins head; INT, or drop and add, are taken.)
  @Test
  void aWrappedOneofGivenAnotherUnionTypeIsRefusedByItsName() throws Exception {
    proto("w01", String.format(WRAPPED_ONEOF, ""));
    assertThatThrownBy(() -> registry.alter("w01",
        new AlterAttributeType("w", "UNION(x INT, y STRING)")))
        .isInstanceOf(TypeRefusedException.class).hasMessageContaining(IS_WRAPPED);
    assertThat(version("w01")).isEqualTo(1);
  }

  // A oneof the stored message has, given a member more by the statement that drops
  // the wrapped one, is the message's own by its name though not every member was its.
  @Test
  void aOneofOfTheMessageGivenAMemberIsItsOwnByItsName() throws Exception {
    proto("w03", String.format(WRAPPED_ONEOF, "  oneof u {\n    int32 a = 1;\n    string q = 7;\n"
        + "  }\n"));
    registry.alter("w03", new DropAttribute("w", false),
        new AlterAttributeType("u", "UNION(a INT, q STRING, c INT)"));
    assertThat(version("w03")).isEqualTo(2);
    assertThat(message("w03").findFieldByNumber(2)).isNull();
    assertThat(message("w03").getRealOneofs()).extracting(o -> o.getName())
        .containsExactly("u");
  }

  // A union added by the statement that drops the wrapped one is the
  // wrapper's oneof only by a member of both the name and the number of one of its own.
  @Test
  void aUnionAddedIsTheWrappedOneofOnlyByAMembersNameAndNumber() throws Exception {
    proto("w06", String.format(WRAPPED_ONEOF, ""));
    registry.alter("w06", new DropAttribute("w", false),
        new AddAttribute("n", "UNION(a INT, c STRING)"));
    assertThat(version("w06")).isEqualTo(2);
    assertThat(message("w06").findFieldByName("a").getNumber()).isNotEqualTo(1);
    // A oneof of the message renamed and given a member, one of whose members has the
    // number of one of the wrapper's and another name, is the message's.
    proto("w07", String.format(WRAPPED_ONEOF, "  oneof u {\n    int32 c = 1;\n    string q = 7;\n"
        + "  }\n"));
    registry.alter("w07", new DropAttribute("w", false),
        new AlterAttributeType("u", "UNION(c INT, q STRING, d INT)"),
        new RenameAttribute("u", "x"));
    assertThat(version("w07")).isEqualTo(2);
    assertThat(fields("w07")).isEqualTo("c=1 d=8 id=5 q=7 z=6");
  }

  // The wrapped oneof renamed is told by its members beside another oneof of the
  // message too.
  @Test
  void aWrappedOneofRenamedBesideAnotherOneofIsRefused() throws Exception {
    proto("w13", String.format(WRAPPED_ONEOF, "  oneof o {\n    int32 c = 7;\n    string d = 8;\n"
        + "  }\n"));
    assertThatThrownBy(() -> registry.alter("w13", new RenameAttribute("w", "x")))
        .isInstanceOf(TypeRefusedException.class).hasMessageContaining(IS_WRAPPED);
    assertThat(version("w13")).isEqualTo(1);
  }

  // The wrapped oneof renamed and given a member more is told by the members it had.
  @Test
  void aWrappedOneofRenamedAndGivenAMemberIsRefused() throws Exception {
    proto("w15", String.format(WRAPPED_ONEOF, ""));
    assertThatThrownBy(() -> registry.alter("w15", new RenameAttribute("w", "x"),
        new AlterAttributeType("x", "UNION(a INT, b STRING, c INT)")))
        .isInstanceOf(TypeRefusedException.class).hasMessageContaining(IS_WRAPPED);
    assertThat(version("w15")).isEqualTo(1);
  }

  // A oneof of the message that is dropped is the wrapper's renamed only where
  // every member is its own by both name and number; by one of the two alone it is not,
  // and the wrapped oneof renamed is refused.
  @Test
  void aOneofOfTheMessageHasAMemberByItsNameAndNumber() throws Exception {
    proto("i02", String.format(WRAPPED_ONEOF, "  oneof u {\n    int32 a = 8;\n    string b = 9;\n"
        + "  }\n"));
    proto("i03", String.format(WRAPPED_ONEOF, "  oneof u {\n    int32 c = 1;\n    string d = 3;\n"
        + "  }\n"));
    // I01: nor is it where one member alone is its own.
    proto("i01", String.format(WRAPPED_ONEOF, "  oneof u {\n    int32 a = 1;\n    string q = 7;\n"
        + "  }\n"));
    for (String type : List.of("i01", "i02", "i03")) {
      assertThatThrownBy(() -> registry.alter(type, new DropAttribute("u", false),
          new RenameAttribute("w", "x"))).as(type)
          .isInstanceOf(TypeRefusedException.class).hasMessageContaining(IS_WRAPPED);
      assertThat(version(type)).as(type).isEqualTo(1);
    }
  }

  // Where no schema can be made, a wrapped oneof given another union type is told as
  // the wrapped oneof it is by its name, whatever its new members are named.
  @Test
  void aWrappedOneofGivenOtherMembersIsToldByItsNameWhereNoSchemaCanBeMade() throws Exception {
    proto("r04", String.format(WRAPPED_ONEOF, "  oneof u {\n    int32 c = 7;\n    string q = 8;\n"
        + "  }\n"));
    assertThatThrownBy(() -> registry.alter("r04",
        new AlterAttributeType("w", "UNION(x INT, y STRING)"), new AddAttribute("c", "INT")))
        .isInstanceOf(TypeRefusedException.class).hasMessageContaining(IS_WRAPPED);
    assertThat(version("r04")).isEqualTo(1);
  }

  // Where no schema can be made of it for another reason, a wrapped oneof given a
  // type that is no union is no more, and the reason is told.
  @Test
  void aWrappedOneofGivenAnotherKindOfTypeIsNotToldWhereNoSchemaCanBeMade() throws Exception {
    proto("r06", String.format(WRAPPED_ONEOF, "  oneof u {\n    int32 c = 7;\n    string q = 8;\n"
        + "  }\n"));
    assertThatThrownBy(() -> registry.alter("r06", new AlterAttributeType("w", "INT"),
        new AddAttribute("c", "INT"))).isInstanceOf(TypeRefusedException.class)
        .hasMessageContaining("keeps its field name 'c'")
        .hasMessageNotContaining("wrapped in a message");
  }

  // Where no schema can be made, the wrapped oneof renamed and given a member
  // more is told by one member of the wrapper's, in the words of the refusal.
  @Test
  void aWrappedOneofRenamedAndGivenAMemberIsToldWhereNoSchemaCanBeMade() throws Exception {
    proto("r10", String.format(WRAPPED_ONEOF, "  oneof u {\n    int32 c = 7;\n    string q = 8;\n"
        + "  }\n"));
    assertThatThrownBy(() -> registry.alter("r10", new RenameAttribute("w", "x"),
        new AlterAttributeType("x", "UNION(a INT, b STRING, c2 INT)"),
        new AddAttribute("c", "INT"))).isInstanceOf(TypeRefusedException.class)
        .hasMessage("Type r10 cannot be altered: field w of message T is a oneof wrapped in a "
            + "message, which would be written as a oneof of the message itself, so its "
            + "stored values would be read as none. Drop the attribute, or register its new "
            + "version as a Protobuf schema");
  }

  private static final String LIB = "{\"type\":\"object\",\"title\":\"lib\",\"properties\":"
      + "{\"q\":{\"type\":\"integer\"}},\"additionalProperties\":false,\"$defs\":{\"Loc\":"
      + "{\"type\":\"object\",\"properties\":{\"p\":{\"type\":\"integer\"}},\"required\":"
      + "[\"p\"],\"additionalProperties\":false}}}";
  private static final String OWN = "\"Loc\":{\"type\":\"object\",\"properties\":{\"q\":"
      + "{\"type\":\"string\"}},\"required\":[\"q\"],\"additionalProperties\":false}";
  private static final String LOC = "{\"$ref\":\"lib#/$defs/Loc\"}";
  private static final String INTS = "{\"type\":\"array\",\"items\":{\"type\":\"integer\"},";
  private static final String ROW = "{\"type\":\"object\",\"properties\":{\"k\":{\"type\":"
      + "\"integer\"}},";

  // A $ref under a word a name is read through is one of the schema's: the document's Loc
  // beside the type's own is refused. Under allOf the document's is read into the
  // attribute and no name is, and under a word that is not read nothing is.
  @Test
  void aDefinitionIsReferredToUnderEveryWordATypeIsReadThrough() throws Exception {
    json("lib", LIB);
    Map<String, String> under = new LinkedHashMap<>();
    under.put("items", "{\"type\":\"array\",\"items\":" + LOC + "}");
    under.put("anyOf", "{\"anyOf\":[{\"type\":\"null\"}," + LOC + "]}");
    under.put("oneOf", "{\"oneOf\":[{\"type\":\"null\"}," + LOC + "]}");
    under.put("properties", "{\"type\":\"object\",\"properties\":{\"k\":" + LOC + "},"
        + "\"additionalProperties\":false}");
    // Those a type is not read through, and that are not written again.
    Map<String, String> unread = new LinkedHashMap<>();
    unread.put("allOf", "{\"allOf\":[" + LOC + "]}");
    unread.put("prefixItems", INTS + "\"prefixItems\":[" + LOC + "]}");
    unread.put("additionalItems", INTS + "\"additionalItems\":" + LOC + "}");
    unread.put("unevaluatedItems", INTS + "\"unevaluatedItems\":" + LOC + "}");
    unread.put("contains", INTS + "\"contains\":" + LOC + "}");
    unread.put("unevaluatedProperties", ROW + "\"unevaluatedProperties\":" + LOC + "}");
    unread.put("propertyNames", ROW + "\"propertyNames\":" + LOC + "}");
    unread.put("patternProperties", ROW + "\"patternProperties\":{\"^k\":" + LOC + "}}");
    unread.put("dependentSchemas", ROW + "\"dependentSchemas\":{\"k\":" + LOC + "}}");
    unread.put("dependencies", ROW + "\"dependencies\":{\"k\":" + LOC + "}}");
    int n = 0;
    for (Map<String, String> words : List.of(under, unread)) {
      for (Map.Entry<String, String> word : words.entrySet()) {
        String type = "k" + n++;
        json(type, "{\"$defs\":{" + OWN + "}," + String.format(JSON_HEAD, type).substring(1)
            + "\"x\":{\"$ref\":\"#/$defs/Loc\"},\"y\":" + word.getValue() + "},"
            + "\"additionalProperties\":false}", "lib");
        if (words == unread) {
          registry.alter(type, new AddAttribute("z", "INT"));
          assertThat(version(type)).as(word.getKey()).isEqualTo(2);
          continue;
        }
        assertThatThrownBy(() -> registry.alter(type, new AddAttribute("z", "INT")))
            .as(word.getKey()).isInstanceOf(TypeRefusedException.class).hasMessageMatching(
                TestRegistry.readAsOne("altered", "Loc", "#/$defs/Loc", "lib#/$defs/Loc"));
      }
    }
    // And in a definition of the type's own an attribute is of, under either word.
    for (String word : List.of("$defs", "definitions")) {
      String type = "d" + word.length();
      json(type, "{\"" + word + "\":{\"B\":{\"type\":\"object\",\"properties\":{\"k\":" + LOC
          + "},\"additionalProperties\":false}," + OWN + "},"
          + String.format(JSON_HEAD, type).substring(1) + "\"x\":{\"$ref\":\"#/" + word
          + "/Loc\"},\"w\":{\"$ref\":\"#/" + word + "/B\"}},\"additionalProperties\":false}",
          "lib");
      assertThatThrownBy(() -> registry.alter(type, new AddAttribute("z", "INT"))).as(word)
          .isInstanceOf(TypeRefusedException.class)
          .hasMessageMatching(TestRegistry.readAsOne("altered", "Loc", "#/" + word + "/Loc",
              "lib#/$defs/Loc"));
    }
    // What is under a word for data, or one of a tool's own, is no schema.
    for (String data : List.of("{\"const\":" + LOC + "}",
        "{\"type\":\"integer\",\"x-tool\":" + LOC + "}")) {
      String type = "k" + n++;
      json(type, "{\"$defs\":{" + OWN + "}," + String.format(JSON_HEAD, type).substring(1)
          + "\"x\":{\"$ref\":\"#/$defs/Loc\"},\"y\":" + data + "},"
          + "\"additionalProperties\":false}", "lib");
      registry.alter(type, new AddAttribute("z", "INT"));
      assertThat(version(type)).as(data).isEqualTo(2);
    }
  }

  // A definition of the schema's own named through its $id is the one named by
  // none: one definition, and the type is altered.
  @Test
  void aDefinitionNamedThroughTheSchemasIdIsItsOwn() throws Exception {
    json("lib", LIB);
    json("i1", "{\"$id\":\"t9\",\"$defs\":{" + OWN + "},"
        + String.format(JSON_HEAD, "i1").substring(1) + "\"x\":{\"$ref\":\"#/$defs/Loc\"},"
        + "\"y\":{\"$ref\":\"t9#/$defs/Loc\"}},\"additionalProperties\":false}");
    registry.alter("i1", new AddAttribute("z", "INT"));
    assertThat(version("i1")).isEqualTo(2);
  }

  // A $ref that begins as the $id does and goes on is to another document.
  @Test
  void aDocumentWhoseNameBeginsAsTheSchemasIdIsAnother() throws Exception {
    json("lib", LIB);
    json("b", LIB.replace("\"lib\"", "\"b\""));
    json("i4", "{\"$id\":\"li\"," + String.format(JSON_HEAD, "i4").substring(1)
        + "\"x\":{\"$ref\":\"lib#/$defs/Loc\"},\"y\":{\"$ref\":\"b#/$defs/Loc\"}},"
        + "\"additionalProperties\":false}", "lib", "b");
    assertThatThrownBy(() -> registry.alter("i4", new AddAttribute("z", "INT")))
        .isInstanceOf(TypeRefusedException.class)
        .hasMessageMatching(TestRegistry.readAsOne("altered", "Loc",
            "lib#/$defs/Loc", "b#/$defs/Loc"));
  }

  // A lookup that gives a version with no list of references, as a client may.
  private TypeLookup nulls() {
    return new TypeLookup() {
      private SchemaMetadata nulled(SchemaMetadata md) {
        return md == null ? null : new SchemaMetadata(md.getId(), md.getVersion(),
            md.getSchemaType(), null, md.getSchema());
      }

      @Override
      public SchemaMetadata latest(String subject) {
        return nulled(registry.latest(subject));
      }

      @Override
      public SchemaMetadata version(String subject, int version) {
        return nulled(registry.version(subject, version));
      }

      @Override
      public ParsedSchema parse(SchemaMetadata version) {
        return registry.parse(version);
      }

      @Override
      public List<Integer> versions(String subject, boolean deleted) {
        return registry.versions(subject, deleted);
      }

      @Override
      public LogicalType logicalType(String subject, SchemaMetadata version) {
        return registry.logicalType(subject, version);
      }

      @Override
      public Config config(String subject) {
        return registry.config(subject);
      }
    };
  }

  // A JSON type whose version has no list of references is altered as any other,
  // and one with a definition named as a part is called is refused all the same.
  @Test
  void aJsonTypeWithNoListOfReferencesIsAltered() throws Exception {
    json("p0", "{\"$defs\":{" + OWN + "}," + String.format(JSON_HEAD, "p0").substring(1)
        + "\"x\":{\"$ref\":\"#/$defs/Loc\"}},\"additionalProperties\":false}");
    TypeAlter.Result result = TypeAlter.alter(nulls(), "p0",
        List.of(new AddAttribute("z", "INT")), new TypeAlter.Options());
    assertThat(result.request()).isNotNull();
    // Two definitions of one name, the type's own under both words, with no references.
    json("p1", "{\"$defs\":{" + OWN + "},\"definitions\":{" + OWN + "},"
        + String.format(JSON_HEAD, "p1").substring(1) + "\"x\":{\"$ref\":\"#/$defs/Loc\"},"
        + "\"y\":{\"$ref\":\"#/definitions/Loc\"}},\"additionalProperties\":false}");
    assertThatThrownBy(() -> TypeAlter.alter(nulls(), "p1",
        List.of(new AddAttribute("z", "INT")), new TypeAlter.Options()))
        .isInstanceOf(TypeRefusedException.class)
        .hasMessageMatching(TestRegistry.readAsOne("altered", "Loc",
            "#/$defs/Loc", "#/definitions/Loc"));
  }

  // A reference nothing is of is looked for in a JSON type alone: a Protobuf import,
  // and an Avro reference, that no attribute is of stay where no name is dropped.
  @Test
  void aReferenceOfNoUseIsDroppedFromAJsonTypeAlone() throws Exception {
    proto("imp", "syntax = \"proto3\";\n\npackage o;\n\nmessage In {\n  int32 k = 1;\n}\n");
    proto("pu", "syntax = \"proto3\";\n\nimport \"imp\";\n\nmessage T {\n  int32 id = 1;\n}\n",
        "imp");
    registry.alter("pu", new AddAttribute("z", "INT"));
    assertThat(references("pu")).containsExactly("imp");
    create("Other", SchemaType.AVRO, "(k INT)");
    registry.client.register("au", new AvroSchema("{\"type\":\"record\",\"name\":\"au\","
        + "\"fields\":[{\"name\":\"id\",\"type\":\"int\"}]}",
        List.of(new SchemaReference("Other", "Other", 1)),
        Map.of("Other", registry.latest("Other").getSchema()), null));
    registry.alter("au", new AddAttribute("z", "INT"));
    assertThat(references("au")).containsExactly("Other");
  }

  // One reference of no use among those in use is dropped, and the others kept.
  @Test
  void aReferenceOfNoUseIsDroppedBesideOneInUse() throws Exception {
    create("E", SchemaType.JSON, "(k INT)");
    create("o5s", SchemaType.JSON, "(o STRING NOT NULL)");
    json("ou2", String.format(JSON_HEAD, "ou2") + "\"x\":{\"$ref\":\"o5s\"}},"
        + "\"additionalProperties\":false,\"$defs\":{\"Foo\":{\"type\":\"object\","
        + "\"properties\":{\"in\":{\"$ref\":\"E\"}},\"additionalProperties\":false}}}",
        "E", "o5s");
    registry.alter("ou2", new AddAttribute("z", "INT"));
    assertThat(references("ou2")).containsExactly("o5s");
  }

  // A number two attributes share is told at each end of what a field
  // can have: the first and the last, and those beside the ones Protobuf keeps.
  @Test
  void aSharedNumberIsToldAtTheEndsOfWhatAFieldCanHave() {
    for (String number : List.of("1", "18999", "20000", "536870911")) {
      String with = " WITH ('protobuf.field.number' = '" + number + "')";
      assertThatThrownBy(() -> Formats.refuseUnwritable(SchemaType.PROTOBUF,
          struct("(a INT" + with + ", b INT" + with + ")"))).as(number)
          .isInstanceOf(TypeRefusedException.class).hasMessage("Attribute b cannot have the "
              + "Protobuf field number " + number + ": attribute a has it; give it another, "
              + "or none");
    }
  }

  // A name with a dot in it is quoted wherever it is a part of a path:
  // a union's member, a row's attribute whose number is shared, and a union told as newly
  // overlapping in a row and in a member.
  @Test
  void aDottedNameIsQuotedInEveryPath() {
    assertThatThrownBy(() -> Formats.refuseUnwritable(SchemaType.JSON,
        struct("(u UNION(`a.b` STRUCT<v INT DEFAULT 1>, c INT))")))
        .isInstanceOf(TypeRefusedException.class)
        .hasMessageStartingWith("Attribute u.`a.b`.v has a DEFAULT");
    String five = " WITH ('protobuf.field.number' = '5')";
    assertThatThrownBy(() -> Formats.refuseUnwritable(SchemaType.PROTOBUF,
        struct("(`e.x` STRUCT<`x.a` INT" + five + ", `y.b` INT" + five + ">)")))
        .isInstanceOf(TypeRefusedException.class).hasMessage("Attribute `e.x`.`y.b` cannot "
            + "have the Protobuf field number 5: attribute `e.x`.`x.a` has it; give it "
            + "another, or none");
    String union = "UNION(a INT, r1 N, r2 STRUCT<k INT NOT NULL>)";
    String[][] rows = {
        {"STRUCT t (id INT, `e.x` STRUCT<`u.v` " + union + ">); TYPE t;", "`e.x`.`u.v`"},
        {"STRUCT t (id INT, o UNION(p INT, `q.r` STRUCT<w " + union + ">)); TYPE t;",
            "o.`q.r`.w"}};
    for (String[] row : rows) {
      LogicalType before = TypeScripts.parse("STRUCT N (k STRING NOT NULL); " + row[0]);
      LogicalType after = TypeScripts.parse("STRUCT N (k INT NOT NULL); " + row[0]);
      assertThat(Formats.newlyOverlapping(before.getRootSchema(),
          KnownTypes.of(before.getNamedTypes()), KnownTypes.of(after.getNamedTypes()), null))
          .as(row[1]).containsExactly(row[1], "r1", "r2");
    }
  }

  // A subject named as the type as read calls another it refers to is no clash where
  // that name is itself given back its reference's: Ref1 is Ref2, and Ref2 is o5s.
  @Test
  void aSubjectNamedAsTheNextReferenceIsCalledIsAltered() {
    create("Ref2", SchemaType.JSON, "(k INT NOT NULL)");
    create("o5s", SchemaType.JSON, "(o STRING NOT NULL)");
    create("s12", SchemaType.JSON, "(id INT, a Ref2, b o5s)");
    registry.alter("s12", new AddAttribute("z", "INT"));
    assertThat(version("s12")).isEqualTo(2);
    assertThat(registry.schema("s12").canonicalString())
        .contains("\"a\":{\"connect.index\":1,\"oneOf\":[{\"type\":\"null\"},{\"$ref\":\"Ref2\"}]}")
        .contains("\"b\":{\"connect.index\":2,\"oneOf\":[{\"type\":\"null\"},{\"$ref\":\"o5s\"}]}");
    assertThat(references("s12")).containsExactly("Ref2", "o5s");
  }

  // An attribute given a type anew is told by its quoted name in what is refused.
  @Test
  void aDottedAttributeGivenATypeIsToldByItsQuotedName() {
    create("dj", SchemaType.JSON, "(id INT, `e.x` STRUCT<c INT>)");
    assertThatThrownBy(() -> registry.alter("dj",
        new AlterAttributeType("e.x", "STRUCT<`a.b` INT DEFAULT 1, c INT>")))
        .isInstanceOf(TypeRefusedException.class)
        .hasMessageStartingWith("Attribute `e.x`.`a.b` has a DEFAULT");
  }

  // A member new to a Protobuf type given anew is told by its path, a name with
  // a dot in it quoted: in a row, and in a union's member.
  @Test
  void aNewMembersNumberIsToldByItsQuotedPath() {
    create("pn", SchemaType.PROTOBUF, "(id INT, r STRUCT<x INT, y INT>, u UNION(a INT, "
        + "b STRING))");
    String one = " WITH ('protobuf.field.number' = '1')";
    String told = " cannot be given the Protobuf field number 1: a member new to a type "
        + "given anew takes the next number its message has free; give it none";
    assertThatThrownBy(() -> registry.alter("pn", new AlterAttributeType("r",
        "STRUCT<x INT, y INT, `z.w` INT" + one + ">")))
        .isInstanceOf(TypeRefusedException.class).hasMessage("Attribute r.`z.w`" + told);
    assertThatThrownBy(() -> registry.alter("pn", new AlterAttributeType("u",
        "UNION(a INT, b STRING, `c.d` STRUCT<k INT" + one + ">)")))
        .isInstanceOf(TypeRefusedException.class).hasMessage("Attribute u.`c.d`.k" + told);
  }

  // A default Avro could not read under the new type is told by the path of its
  // attribute, through rows, collections and a union's member.
  @Test
  void anAvroDefaultThatCannotBeReadIsToldByItsPath() {
    String row = "STRUCT<n BIGINT DEFAULT 5>";
    create("ad", SchemaType.AVRO, "(id INT, e " + row + ", a ARRAY<" + row + ">, m MAP<STRING, "
        + row + ">, u UNION(b " + row + ", c INT), ms MULTISET<" + row + ">, mk MAP<" + row
        + ", INT>, ee STRUCT<f " + row + ">)");
    String now = "STRUCT<n INT>";
    Map<String, String[]> given = new LinkedHashMap<>();
    given.put("e", new String[] {now, "e.n"});
    given.put("a", new String[] {"ARRAY<" + now + ">", "a.n"});
    given.put("m", new String[] {"MAP<STRING, " + now + ">", "m.n"});
    given.put("u", new String[] {"UNION(b " + now + ", c INT)", "u.b.n"});
    given.put("ms", new String[] {"MULTISET<" + now + ">", "ms.n"});
    given.put("mk", new String[] {"MAP<" + now + ", INT>", "mk.n"});
    given.put("ee", new String[] {"STRUCT<f " + now + ">", "ee.f.n"});
    for (Map.Entry<String, String[]> type : given.entrySet()) {
      assertThatThrownBy(() -> registry.alter("ad",
          new AlterAttributeType(type.getKey(), type.getValue()[0]))).as(type.getKey())
          .isInstanceOf(TypeRefusedException.class).hasMessage("The default 5 of attribute "
              + type.getValue()[1] + " cannot be read as INT, as Avro reads a BIGINT "
              + "written before the change");
    }
    // K17: a member's name with a dot in it is quoted.
    create("ab", SchemaType.AVRO, "(id INT, u UNION(`b.c` " + row + ", c INT))");
    assertThatThrownBy(() -> registry.alter("ab",
        new AlterAttributeType("u", "UNION(`b.c` " + now + ", c INT)")))
        .isInstanceOf(TypeRefusedException.class)
        .hasMessageStartingWith("The default 5 of attribute u.`b.c`.n cannot be read as INT");
  }

  // A union's member that is a union, wrapped by the writer, is no wrapped oneof where
  // no schema can be made either: a union added with a branch named as one of its members
  // is not told as one, and the reason is.
  @Test
  void aMemberThatIsAUnionIsNoWrappedOneofWhereNoSchemaCanBeMade() {
    create("m5", SchemaType.PROTOBUF, "(id INT, l UNION(w UNION(x INT, y STRING), b INT), "
        + "z INT)");
    assertThatThrownBy(() -> registry.alter("m5",
        new AddAttribute("v", "UNION(x INT, q STRING)"), new AddAttribute("b", "INT")))
        .isInstanceOf(TypeRefusedException.class)
        .hasMessageContaining("keeps its field name 'b'")
        .hasMessageNotContaining("wrapped in a message");
  }

  // A name that would be two versions of a type is told first, though the type has a
  // wrapped oneof whose members' numbers no schema can be made of.
  @Test
  void twoVersionsOfATypeAreToldBeforeAWrappedOneof() throws Exception {
    create("inner", SchemaType.PROTOBUF, "(x STRING, y STRING)");
    create("pair", SchemaType.PROTOBUF, "(a STRING, i inner)");
    create("o", SchemaType.PROTOBUF, "(id INT NOT NULL, p pair)");
    SchemaMetadata md = registry.latest("o");
    registry.client.register("o", new ProtobufSchema(md.getSchema()
        .replace("syntax = \"proto3\";\n", P3)
        .replace("message o {\n", "message o {\n  WOneofWrapper w = 9 " + WRAPPED
          + ";\n\n  message WOneofWrapper {\n    oneof value {\n      int32 a = 1;\n      "
          + "string b = 3;\n    }\n  }\n"), md.getReferences(),
        ((ProtobufSchema) registry.parse(md)).resolvedReferences(), null, null));
    assertThat(version("o")).isEqualTo(2);
    registry.alter("inner", new AddAttribute("z", "INT"));
    assertThatThrownBy(() -> registry.alter("o", new AddAttribute("j", "inner")))
        .isInstanceOf(TypeRefusedException.class)
        .hasMessageContaining("it would refer to two versions of type inner");
    assertThatThrownBy(() -> registry.alter("o", new AddAttribute("n", "INT")))
        .isInstanceOf(TypeRefusedException.class)
        .hasMessageContaining("field w of message o is a oneof wrapped in a message");
  }

  // A lookup of the registry's, for one answer of it to be given otherwise.
  private class Lookup implements TypeLookup {
    @Override
    public SchemaMetadata latest(String subject) {
      return registry.latest(subject);
    }

    @Override
    public SchemaMetadata version(String subject, int version) {
      return registry.version(subject, version);
    }

    @Override
    public ParsedSchema parse(SchemaMetadata version) {
      return registry.parse(version);
    }

    @Override
    public List<Integer> versions(String subject, boolean deleted) {
      return registry.versions(subject, deleted);
    }

    @Override
    public LogicalType logicalType(String subject, SchemaMetadata version) {
      return registry.logicalType(subject, version);
    }

    @Override
    public Config config(String subject) {
      return registry.config(subject);
    }
  }

  // A referred type that cannot be read as a type is left out, and the others known;
  // so is a version that is not there.
  @Test
  void aReferredTypeThatCannotBeReadIsLeftOut() {
    registry.create("addr", SchemaType.AVRO, BodyKind.STRUCT, "(city STRING)");
    registry.create("geo", SchemaType.AVRO, BodyKind.STRUCT, "(lat DOUBLE)");
    LogicalType order = registry.create("order", SchemaType.AVRO, BodyKind.STRUCT,
        "(id BIGINT NOT NULL, ship_to addr, at geo)");
    TypeLookup unreadable = new Lookup() {
      @Override
      public LogicalType logicalType(String subject, SchemaMetadata version) {
        if (subject.equals("geo")) {
          throw new IllegalStateException("No type can be read of it");
        }
        return super.logicalType(subject, version);
      }
    };
    KnownTypes known = TypeReferences.referredTypes(unreadable, "order",
        order.getReferences(), Map.of());
    assertThat(known.get("addr")).isNotNull();
    assertThat(known.get("geo")).isNull();
    KnownTypes none = TypeReferences.referredTypes(registry, "order",
        List.of(new SchemaReference("addr", "addr", 9)), Map.of());
    assertThat(none.get("addr")).isNull();
  }

  // An entry marked logical.ref that names nothing itself is a definition as any
  // other, and what it refers to is read through it.
  @Test
  void aMarkedEntryThatIsNoNameIsADefinition() throws Exception {
    json("lib", LIB);
    json("m5", "{\"$defs\":{\"A\":{\"logical.ref\":true,\"type\":\"object\",\"properties\":"
        + "{\"k\":" + LOC + "},\"additionalProperties\":false}," + OWN + "},"
        + String.format(JSON_HEAD, "m5").substring(1) + "\"x\":{\"$ref\":\"#/$defs/A\"},"
        + "\"y\":{\"$ref\":\"#/$defs/Loc\"}},\"additionalProperties\":false}", "lib");
    assertThatThrownBy(() -> registry.alter("m5", new AddAttribute("z", "INT")))
        .isInstanceOf(TypeRefusedException.class)
        .hasMessageMatching(TestRegistry.readAsOne("altered", "Loc",
            "#/$defs/Loc", "lib#/$defs/Loc"));
  }

  // A $defs that is no object has no entry to leave out, and the type is altered.
  @Test
  void aDefsThatIsNoObjectHasNoEntries() throws Exception {
    json("q1", "{\"$defs\":[]," + String.format(JSON_HEAD, "q1").substring(1)
        + "\"x\":{\"type\":\"integer\"}},\"additionalProperties\":false}");
    registry.alter("q1", new AddAttribute("z", "INT"));
    assertThat(version("q1")).isEqualTo(2);
  }

  // A member's field name that is no name is told as a literal, a quote doubled.
  @Test
  void aFieldNameThatIsNoNameIsToldAsALiteral() {
    assertThatThrownBy(() -> Formats.refuseUnwritable(SchemaType.PROTOBUF, struct("(u UNION("
        + "a INT WITH ('protobuf.field.name' = 'it''s'), b STRING))")))
        .isInstanceOf(TypeRefusedException.class).hasMessage("Property protobuf.field.name of "
            + "branch a of attribute u is not a valid field name: 'it''s'");
  }

  // The rows two branches name are judged once for the union, and not
  // anew for every two of its branches: 400 rows that share a chain of 1000.
  @Test
  void theRowsAUnionsBranchesShareAreJudgedOnce() {
    StringBuilder script = new StringBuilder();
    for (int i = 1; i < 1000; i++) {
      script.append("STRUCT C").append(i).append(" (n C").append(i + 1).append(" NOT NULL); ");
    }
    script.append("STRUCT C1000 (v INT NOT NULL); STRUCT t (u UNION(");
    for (int i = 0; i < 400; i++) {
      script.append(i == 0 ? "" : ", ").append("b").append(i).append(" STRUCT<x C1 NOT NULL, k")
          .append(i).append(" INT NOT NULL>");
    }
    script.append(")); TYPE t;");
    LogicalType type = TypeScripts.parse(script.toString());
    assertTimeoutPreemptively(Duration.ofSeconds(30),
        () -> Formats.refuseUnwritable(SchemaType.JSON, type));
  }
}
