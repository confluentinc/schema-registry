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

import com.google.protobuf.Descriptors.Descriptor;
import io.confluent.kafka.schemaregistry.client.SchemaMetadata;
import io.confluent.kafka.schemaregistry.client.rest.entities.SchemaReference;
import io.confluent.kafka.schemaregistry.json.JsonSchema;
import io.confluent.kafka.schemaregistry.protobuf.ProtobufSchema;
import io.confluent.kafka.schemaregistry.type.logical.SchemaType;
import io.confluent.kafka.schemaregistry.type.logical.protobuf.LogicalTypeToProtoConverter;
import io.confluent.kafka.schemaregistry.type.logical.statements.AlterAction.AddAttribute;
import io.confluent.kafka.schemaregistry.type.logical.statements.AlterAction.AlterAttributeType;
import io.confluent.kafka.schemaregistry.type.logical.statements.AlterAction.DropAttribute;
import io.confluent.kafka.schemaregistry.type.logical.statements.AlterAction.MoveAttribute;
import io.confluent.kafka.schemaregistry.type.logical.statements.AlterAction.Position;
import io.confluent.kafka.schemaregistry.type.logical.statements.AlterAction.RenameAttribute;
import io.confluent.kafka.schemaregistry.type.logical.statements.AlterAction.SetAttributeWith;
import io.confluent.kafka.schemaregistry.type.logical.statements.TypeScripts.BodyKind;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
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

  private static final String LIBS = "{\"type\":\"object\",\"title\":\"%s\",\"properties\":"
      + "{\"v\":{\"$ref\":\"#/$defs/o5s\"},\"l\":{\"$ref\":\"#/$defs/Loc\"}},"
      + "\"additionalProperties\":false,\"$defs\":{\"o5s\":{\"enum\":[\"X\"]},\"Loc\":%s}}";

  private String later(String type) {
    return "{" + LATER + head(type).substring(1);
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

  // As where the schema's $id is the reference's name: the name is the reference's.
  @Test
  void aNameForADocumentsDefinitionIsTheReferencesThoughTheIdIsItsName() throws Exception {
    json("lib", String.format(LIBS, "lib", P));
    json("n9", "{\"$id\":\"lib\"," + later("n9").substring(1)
        + String.format(ENTRY, "lib#/$defs/Loc", "lib"), "lib");
    registry.alter("n9", new AddAttribute("z", "INT"));
    assertThat(version("n9")).isEqualTo(2);
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
    assertThatThrownBy(() -> registry.alter("l9", new AddAttribute("z", "INT")))
        .isInstanceOf(TypeRefusedException.class)
        .hasMessageContaining("it has two definitions named Loc, #/$defs/Loc and "
            + "a.json2#/$defs/Loc.");
  }

  // A definition under definitions that no $ref is to is read by a later draft too.
  @Test
  void aDefinitionUnderDefinitionsReadThoughNoRefIsToItIsOneOfItsName() throws Exception {
    json("lib", String.format(LIBS, "lib", P));
    json("y2", later("y2") + "\"y\":{\"$ref\":\"lib#/$defs/Loc\"}},\"additionalProperties\":"
        + "false,\"definitions\":{\"Loc\":" + Q + "}}", "lib");
    assertThatThrownBy(() -> registry.alter("y2", new AddAttribute("z", "INT")))
        .isInstanceOf(TypeRefusedException.class)
        .hasMessageContaining("it has two definitions named Loc, lib#/$defs/Loc and "
            + "#/definitions/Loc.");
  }

  // A name a document has for another's definition is a use of that definition.
  @Test
  void aDocumentsNameForAnothersDefinitionIsAUseOfIt() throws Exception {
    json("other", String.format(LIBS, "other", Q));
    json("libe", "{" + LATER + "\"type\":\"object\",\"title\":\"libe\",\"properties\":{\"l\":"
        + "{\"$ref\":\"#/$defs/A\"}},\"additionalProperties\":false,\"$defs\":{\"A\":{\"$ref\":"
        + "\"other#/$defs/Loc\",\"logical.ref\":true}}}", "other");
    json("z6", later("z6") + "\"x\":{\"$ref\":\"#/$defs/Loc\"},\"a\":{\"$ref\":\"libe\"}},"
        + "\"additionalProperties\":false,\"$defs\":{\"Loc\":" + P + "}}", "libe", "other");
    assertThatThrownBy(() -> registry.alter("z6", new AddAttribute("z", "INT")))
        .isInstanceOf(TypeRefusedException.class)
        .hasMessageContaining("it has two definitions named Loc, #/$defs/Loc and "
            + "other#/$defs/Loc.");
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

  // The two definitions are told in the order they are come to, level by level.
  @Test
  void twoDefinitionsAreToldInTheOrderTheyAreComeTo() throws Exception {
    json("lib", String.format(LIBS, "lib", P));
    String row = "{\"type\":\"object\",\"properties\":{\"k\":{\"$ref\":\"%s\"}}}";
    json("z18", later("z18") + "\"x\":{\"$ref\":\"#/$defs/A\"},\"y\":{\"$ref\":\"#/$defs/B\"}},"
        + "\"additionalProperties\":false,\"$defs\":{\"A\":" + String.format(row, "lib#/$defs/Loc")
        + ",\"B\":" + String.format(row, "#/$defs/Loc") + ",\"Loc\":" + Q + "}}", "lib");
    assertThatThrownBy(() -> registry.alter("z18", new AddAttribute("z", "INT")))
        .isInstanceOf(TypeRefusedException.class)
        .hasMessageContaining("it has two definitions named Loc, lib#/$defs/Loc and "
            + "#/$defs/Loc.");
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
