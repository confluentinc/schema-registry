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
import static org.assertj.core.api.Assertions.tuple;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.Descriptors.FieldDescriptor;
import com.google.type.Date;
import io.confluent.kafka.schemaregistry.ParsedSchema;
import io.confluent.kafka.schemaregistry.avro.AvroSchema;
import io.confluent.kafka.schemaregistry.client.rest.entities.ProvenanceField;
import io.confluent.kafka.schemaregistry.client.rest.entities.ProvenanceVersion;
import io.confluent.kafka.schemaregistry.client.SchemaMetadata;
import io.confluent.kafka.schemaregistry.client.rest.entities.Config;
import io.confluent.kafka.schemaregistry.client.rest.entities.SchemaReference;
import io.confluent.kafka.schemaregistry.json.JsonSchema;
import io.confluent.kafka.schemaregistry.protobuf.ProtobufSchema;
import io.confluent.kafka.schemaregistry.type.logical.LogicalType;
import io.confluent.kafka.schemaregistry.type.logical.LogicalTypeConversion;
import io.confluent.kafka.schemaregistry.type.logical.LogicalTypeToDdlConverter;
import io.confluent.kafka.schemaregistry.type.logical.Rule;
import io.confluent.kafka.schemaregistry.type.logical.Schema;
import io.confluent.kafka.schemaregistry.type.logical.SchemaType;
import io.confluent.kafka.schemaregistry.type.logical.ValidationException;
import io.confluent.kafka.schemaregistry.type.logical.common.LogicalTypeVersion;
import io.confluent.kafka.schemaregistry.type.logical.json.LogicalTypeToJsonConverter;
import io.confluent.kafka.schemaregistry.type.logical.protobuf.LogicalTypeToProtoConverter;
import io.confluent.kafka.schemaregistry.type.logical.statements.AlterAction.AddAttribute;
import io.confluent.kafka.schemaregistry.type.logical.statements.AlterAction.AddAttributeCheck;
import io.confluent.kafka.schemaregistry.type.logical.statements.AlterAction.AddConstraint;
import io.confluent.kafka.schemaregistry.type.logical.statements.AlterAction.AddValue;
import io.confluent.kafka.schemaregistry.type.logical.statements.AlterAction.AlterAttributeType;
import io.confluent.kafka.schemaregistry.type.logical.statements.AlterAction.Check;
import io.confluent.kafka.schemaregistry.type.logical.statements.AlterAction.DropAttribute;
import io.confluent.kafka.schemaregistry.type.logical.statements.AlterAction.DropConstraint;
import io.confluent.kafka.schemaregistry.type.logical.statements.AlterAction.MoveAttribute;
import io.confluent.kafka.schemaregistry.type.logical.statements.AlterAction.Position;
import io.confluent.kafka.schemaregistry.type.logical.statements.AlterAction.RenameAttribute;
import io.confluent.kafka.schemaregistry.type.logical.statements.AlterAction.SetAttributeComment;
import io.confluent.kafka.schemaregistry.type.logical.statements.AlterAction.SetAttributeWith;
import io.confluent.kafka.schemaregistry.type.logical.statements.AlterAction.SetDataType;
import io.confluent.kafka.schemaregistry.type.logical.statements.AlterAction.SetWith;
import io.confluent.kafka.schemaregistry.type.logical.statements.TypeScripts.BodyKind;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.jupiter.params.provider.ValueSource;

class TypeAlterTest {

  private final TestRegistry registry = new TestRegistry();

  private void create(String subject, SchemaType format, String body) {
    registry.create(subject, format, BodyKind.STRUCT, body);
  }

  private int version(String subject) {
    return registry.latest(subject).getVersion();
  }

  // The fields of the entry message of a Protobuf map or multiset attribute, as registered.
  private String entry(String subject, String attribute) {
    Descriptor d = ((ProtobufSchema) registry.schema(subject)).toDescriptor()
        .findFieldByName(attribute).getMessageType();
    // Through the messages the writer wraps a repeated entry in.
    while (d.getFields().size() == 1
        && d.getFields().get(0).getType() == FieldDescriptor.Type.MESSAGE) {
      d = d.getFields().get(0).getMessageType();
    }
    return numbers(d);
  }

  private static String numbers(Descriptor message) {
    return message.getFields().stream().map(f -> f.getName() + "=" + f.getNumber())
        .collect(Collectors.joining(" "));
  }

  // The first message of a name the type declares, at whatever depth.
  private static Descriptor nested(Descriptor message, String name) {
    for (Descriptor inner : message.getNestedTypes()) {
      Descriptor found = inner.getName().equals(name) ? inner : nested(inner, name);
      if (found != null) {
        return found;
      }
    }
    return null;
  }

  private Descriptor message(String subject) {
    return ((ProtobufSchema) registry.schema(subject)).toDescriptor();
  }

  @Test
  void anAvroTypeIsAlteredWithAliasesAndNullDefaults() {
    create("t", SchemaType.AVRO, "(a INT NOT NULL, b STRING, c BIGINT)");
    TypeAlter.Result result = registry.alter("t", new AddAttribute("d", "DOUBLE"),
        new RenameAttribute("b", "bb"), new DropAttribute("c", false),
        new AlterAttributeType("a", "BIGINT NOT NULL"));

    assertThat(result.format()).isEqualTo(SchemaType.AVRO);
    assertThat(result.base().getVersion()).isEqualTo(1);
    assertThat(result.version()).isEqualTo(2);
    assertThat(result.policy()).isNull();
    assertThat(registry.schema("t").canonicalString())
        .contains("{\"name\":\"a\",\"type\":\"long\"}")
        .contains("\"name\":\"bb\"").contains("\"aliases\":[\"b\"]")
        .contains("{\"name\":\"d\",\"type\":[\"null\",\"double\"],\"default\":null}")
        .doesNotContain("\"name\":\"c\"");
    assertThat(version("t")).isEqualTo(2);

    assertThatThrownBy(() -> registry.alter("t", new DropAttribute("nope", false)))
        .isInstanceOf(TypeRefusedException.class).hasMessage("Type t has no attribute nope");
    // An action that changes nothing makes no version.
    assertThat(registry.alter("t", new DropAttribute("nope", true)).edited()).isNull();
    assertThatThrownBy(() -> registry.alter("t", new SetWith(Map.of("format", "JSON"))))
        .isInstanceOf(TypeRefusedException.class)
        .hasMessage("The format of type t cannot change");
    assertThatThrownBy(() -> registry.alter("nope", new DropAttribute("a", false)))
        .isInstanceOf(TypeRefusedException.class).hasMessage("Unknown type nope");
    assertThat(version("t")).isEqualTo(2);
  }

  @Test
  void aProtobufFieldKeepsItsNumberAndANewOneTakesNoneUsedBefore() {
    create("p", SchemaType.PROTOBUF, "(a INT NOT NULL, b STRING, c BIGINT)");
    registry.alter("p", new DropAttribute("b", false), new AddAttribute("d", "STRING"),
        new RenameAttribute("c", "cc"));

    ProtobufSchema proto = (ProtobufSchema) registry.schema("p");
    assertThat(proto.toDescriptor().findFieldByName("a").getNumber()).isEqualTo(1);
    assertThat(proto.toDescriptor().findFieldByName("cc").getNumber()).isEqualTo(3);
    assertThat(proto.toDescriptor().findFieldByName("d").getNumber()).isEqualTo(4);
    assertThat(proto.toDescriptor().findFieldByName("b")).isNull();

    // Not the number the dropped b had in version 1, though no field of version 2 has it.
    registry.alter("p", new AddAttribute("e", "INT"));
    proto = (ProtobufSchema) registry.schema("p");
    assertThat(proto.toDescriptor().findFieldByName("e").getNumber()).isEqualTo(5);
  }

  @Test
  void aJsonAttributeCannotBeRenamed() {
    create("j", SchemaType.JSON, "(a INT, b INT)");
    assertThatThrownBy(() -> registry.alter("j", new RenameAttribute("a", "c")))
        .isInstanceOf(TypeRefusedException.class)
        .hasMessage("Attributes of JSON type j cannot be renamed, since JSON Schema has no "
            + "alias that keeps the attribute's data");
  }

  @ParameterizedTest
  @EnumSource(value = SchemaType.class, names = {"AVRO", "PROTOBUF"})
  void aReferenceNoLongerUsedIsDropped(SchemaType format) {
    create("com.acme.address", format, "(city STRING)");
    create("o", format, "(id BIGINT NOT NULL, addr com.acme.address, note STRING)");

    registry.alter("o", new DropAttribute("note", false));
    assertThat(registry.schema("o").references()).extracting(SchemaReference::getSubject)
        .containsExactly("com.acme.address");

    registry.alter("o", new DropAttribute("addr", false));
    ParsedSchema o = registry.schema("o");
    assertThat(o.references()).isEmpty();
    assertThat(o.canonicalString()).doesNotContain("address");
  }

  // A statement that names a referenced type moves the reference to its latest version, for
  // every attribute of that type; one that does not keeps the version it has.
  @ParameterizedTest
  @EnumSource(SchemaType.class)
  void namingAReferencedTypeMovesItToItsLatestVersion(SchemaType format) {
    create("addr", format, "(city STRING)");
    create("o1", format, "(id INT NOT NULL, home addr)");
    create("o2", format, "(id INT NOT NULL, home addr)");
    registry.alter("addr", new AddAttribute("zip", "INT"));

    registry.alter("o1", new AddAttribute("note", "STRING"));
    assertThat(registry.schema("o1").references()).extracting(SchemaReference::getVersion)
        .containsExactly(1);
    registry.alter("o1", new AddAttribute("work", "addr"));
    assertThat(registry.schema("o1").references()).extracting(SchemaReference::getVersion)
        .containsExactly(2);
    registry.alter("o2", new AlterAttributeType("home", "addr"));
    assertThat(version("o2")).isEqualTo(2);
    assertThat(registry.schema("o2").references()).extracting(SchemaReference::getVersion)
        .containsExactly(2);
  }

  @Test
  void aJsonTypeKeepsItsDirectRefsThroughAnAlter() {
    create("a", SchemaType.JSON, "(city STRING)");
    create("o", SchemaType.JSON, "(id BIGINT NOT NULL, addr a, note STRING)");
    ParsedSchema v1 = registry.schema("o");

    registry.alter("o", new AddAttribute("extra", "STRING"));
    ParsedSchema v2 = registry.schema("o");
    assertThat(v2.canonicalString()).contains("{\"$ref\":\"a\"}").doesNotContain("$defs");
    assertThat(v2.isBackwardCompatible(v1)).isEmpty();

    registry.alter("o", new DropAttribute("note", false));
    assertThat(registry.schema("o").canonicalString()).contains("{\"$ref\":\"a\"}");
  }

  @Test
  void anAliasThatBecameAnotherAttributesNameIsDropped() {
    create("w", SchemaType.AVRO, "(f0 INT, f1 STRING)");
    ParsedSchema previous = registry.schema("w");
    // A swap through a third name, a statement each.
    for (String[] rename : new String[][] {{"f1", "tf1"}, {"f0", "f1"}, {"tf1", "f0"}}) {
      registry.alter("w", new RenameAttribute(rename[0], rename[1]));
      assertThat(registry.schema("w").isBackwardCompatible(previous)).isEmpty();
      previous = registry.schema("w");
    }
    List<Schema.Field> fields = registry.struct("w").getFields();
    assertThat(fields).extracting(Schema.Field::getName).containsExactly("f1", "f0");
    assertThat(fields.get(1).getAliases()).containsExactly("tf1");
    assertThat(fields.get(0).getAliases()).isEmpty();
  }

  // A shift of names in one statement, which provenance cannot pair, is refused rather than
  // leave the type's records unreadable; made in two statements, it passes.
  @Test
  void aChangeProvenanceCannotPairIsRefused() {
    create("sh", SchemaType.AVRO, "(id INT NOT NULL, a INT, c INT)");
    assertThatThrownBy(() -> registry.alter("sh", new RenameAttribute("c", "z"),
        new RenameAttribute("a", "c")))
        .isInstanceOf(TypeRefusedException.class).hasMessageStartingWith("Type sh cannot be "
            + "changed so: Schema Registry's provenance could not tell which attribute is "
            + "which across its versions, so their records could not be read");
    assertThat(version("sh")).isEqualTo(1);

    registry.alter("sh", new RenameAttribute("c", "z"));
    registry.alter("sh", new RenameAttribute("a", "c"));
    assertThat(version("sh")).isEqualTo(3);
  }

  // An alias is a name the attribute's data was written with: the one it is stored under,
  // not one it had only within the statement.
  @Test
  void anAvroAliasIsOnlyOfANameTheAttributeWasStoredUnder() {
    create("al", SchemaType.AVRO, "(id INT NOT NULL, a INT, b INT, c BIGINT)");
    // Renamed and renamed back, the attribute is as it was.
    assertThat(registry.alter("al", new RenameAttribute("c", "cc"),
        new RenameAttribute("cc", "c")).edited()).isNull();
    assertThat(version("al")).isEqualTo(1);
    // A swap through a third name, a rename in two steps, and an attribute added and renamed.
    registry.alter("al", new RenameAttribute("a", "t"), new RenameAttribute("b", "a"),
        new RenameAttribute("t", "b"), new RenameAttribute("c", "c1"),
        new RenameAttribute("c1", "c2"), new AddAttribute("z", "INT"),
        new RenameAttribute("z", "y"));
    Map<String, List<String>> aliases = new LinkedHashMap<>();
    registry.struct("al").getFields().forEach(f -> aliases.put(f.getName(), f.getAliases()));
    assertThat(aliases).containsExactly(Map.entry("id", List.of()),
        Map.entry("b", List.of("a")), Map.entry("a", List.of("b")),
        Map.entry("c2", List.of("c")), Map.entry("y", List.of()));
  }

  // An alias that is another attribute's stored name is dropped, whatever the statement
  // names that attribute: two attributes of one alias are one field to a reader.
  @Test
  void anAvroAliasOfANameAnotherAttributeIsStoredUnderIsDropped() {
    create("da", SchemaType.AVRO, "(id INT NOT NULL, a INT, b INT)");
    registry.alter("da", new RenameAttribute("a", "zz"), new AddAttribute("a", "INT"));
    AvroSchema v2 = (AvroSchema) registry.schema("da");
    registry.alter("da", new RenameAttribute("a", "t"));
    Map<String, List<String>> aliases = new LinkedHashMap<>();
    registry.struct("da").getFields().forEach(f -> aliases.put(f.getName(), f.getAliases()));
    assertThat(aliases).containsExactly(Map.entry("id", List.of()),
        Map.entry("zz", List.of()), Map.entry("b", List.of()), Map.entry("t", List.of("a")));
    assertThat(registry.schema("da").isBackwardCompatible(v2)).isEmpty();

    // An attribute the statement adds was stored under no name: the alias stays.
    create("db", SchemaType.AVRO, "(id INT NOT NULL, a INT)");
    registry.alter("db", new RenameAttribute("a", "zz"));
    registry.alter("db", new AddAttribute("a", "INT"), new RenameAttribute("a", "t"));
    assertThat(registry.struct("db").getFields()).extracting(Schema.Field::getName,
        Schema.Field::getAliases).containsExactly(tuple("id", List.of()),
            tuple("zz", List.of("a")), tuple("t", List.of()));
  }

  // A swap back in one statement after a swap is refused, as the aliases would read it as no
  // rename; the message gives the three statements that make it.
  @Test
  void aSwapBackIsRefusedWithTheStatementsThatMakeIt() {
    create("sw", SchemaType.AVRO, "(id INT NOT NULL, a INT, b INT)");
    AlterAction[] swap = {new RenameAttribute("a", "t"), new RenameAttribute("b", "a"),
        new RenameAttribute("t", "b")};
    registry.alter("sw", swap);
    assertThatThrownBy(() -> registry.alter("sw", swap))
        .isInstanceOf(TypeRefusedException.class).hasMessage("Attribute a would read another "
            + "attribute's values, those of a of version 2 of type sw: the aliases of a and b "
            + "already swap their names, so they cannot be swapped back in one statement; "
            + "swap them in three: RENAME ATTRIBUTE a TO a_tmp, then RENAME ATTRIBUTE b TO a, "
            + "then RENAME ATTRIBUTE a_tmp TO b");
  }

  @ParameterizedTest
  @EnumSource(SchemaType.class)
  void aDroppedNameIsNotGivenAgainInTheSameStatement(SchemaType format) {
    create("dn", format, "(a INT, b INT)");
    List<AlterAction> reuses = new ArrayList<>(List.of(new AddAttribute("a", "BIGINT")));
    if (format != SchemaType.JSON) {
      reuses.add(new RenameAttribute("b", "a"));
    }
    for (AlterAction reuse : reuses) {
      assertThatThrownBy(() -> registry.alter("dn", new DropAttribute("a", false), reuse))
          .isInstanceOf(TypeRefusedException.class).hasMessageContaining("use two statements");
    }
    assertThat(version("dn")).isEqualTo(1);

    registry.alter("dn", new DropAttribute("a", false));
    registry.alter("dn", reuses.get(reuses.size() - 1));
    assertThat(version("dn")).isEqualTo(3);
  }

  @Test
  void theLastAttributeIsNotDropped() {
    create("one", SchemaType.AVRO, "(a INT)");
    assertThatThrownBy(() -> registry.alter("one", new DropAttribute("a", false)))
        .isInstanceOf(TypeRefusedException.class)
        .hasMessage("Type one would have no attributes; drop the type instead");
    // A topic's own type is its columns, and the tool's words say so.
    assertThatThrownBy(() -> registry.alter("one", new TypeAlter.Options().columns(true),
        new DropAttribute("A", false)))
        .isInstanceOf(TypeRefusedException.class)
        .hasMessage("Type one would have no attributes; drop the topic instead");
  }

  @Test
  void aTypeThatMayReferToNoOtherIsRefusedInTheToolsWords() {
    create("addr", SchemaType.AVRO, "(city STRING)");
    create("own", SchemaType.AVRO, "(id INT)");
    TypeAlter.Options none = new TypeAlter.Options()
        .noReferences(type -> "Type " + type + " cannot be used here");
    assertThatThrownBy(() -> registry.alter("own", none, new AddAttribute("home", "addr")))
        .isInstanceOf(TypeRefusedException.class).hasMessage("Type addr cannot be used here");
    registry.alter("own", none, new AddAttribute("note", "STRING"));

    // Asked of every type named, not only the first.
    create("other", SchemaType.AVRO, "(n INT)");
    TypeAlter.Options onlyAddr = new TypeAlter.Options()
        .noReferences(type -> type.equals("addr") ? null : "Type " + type + " is refused");
    assertThatThrownBy(() -> registry.alter("own", onlyAddr,
        new AddAttribute("both", "STRUCT<x addr, y other>")))
        .isInstanceOf(TypeRefusedException.class).hasMessage("Type other is refused");

    // Asked only when a type is named, and may allow it.
    registry.alter("own", new TypeAlter.Options().noReferences(type -> null),
        new AddAttribute("home", "addr"));
    assertThat(registry.schema("own").references()).hasSize(1);
  }

  @Test
  void checksFollowARenameAndKeepAnAttributeTheyUse() {
    create("t", SchemaType.AVRO, "(a INT CHECK (a > 0), b INT, CHECK (a < b))");
    assertThatThrownBy(() -> registry.alter("t", new DropAttribute("b", false)))
        .isInstanceOf(TypeRefusedException.class).hasMessageContaining("t_check");

    registry.alter("t", new RenameAttribute("b", "c"),
        new AddConstraint(new Check("big", "a < 100", "too big")));
    Schema t = registry.struct("t");
    assertThat(t.getRules()).extracting(Rule::getName).containsExactly("t_check", "big");
    assertThat(t.getRules().get(0).getSql()).isEqualTo("a < c");
    assertThat(t.getRules().get(1).getDoc()).isEqualTo("too big");

    assertThatThrownBy(() -> registry.alter("t",
        new AddConstraint(new Check("BIG", "a > 1", null))))
        .isInstanceOf(TypeRefusedException.class).hasMessageContaining("already exists");
    registry.alter("t", new DropConstraint("t_check", false), new DropAttribute("c", false));
    assertThat(registry.struct("t").getFields()).extracting(Schema.Field::getName)
        .containsExactly("a");
    // A change that breaks a CHECK is refused rather than registered.
    assertThatThrownBy(() -> registry.alter("t", new AlterAttributeType("a", "STRING")))
        .isInstanceOf(TypeRefusedException.class).hasMessageContaining("CHECK");
  }

  // A type that is an enum is no Protobuf schema, and JSON Schema keeps no order of values.
  @ParameterizedTest
  @EnumSource(value = SchemaType.class, names = {"AVRO", "JSON"})
  void anEnumGainsAValueWhereAsked(SchemaType format) {
    registry.create("color", format, BodyKind.ENUM, "('RED', 'GREEN')");
    registry.alter("color", new AddValue("BLUE", null, null),
        new AddValue("AMBER", "GREEN", null), new AddValue("PINK", null, "RED"));

    Schema color = registry.logicalType("color", registry.latest("color")).getRootSchema();
    if (color.getType() == Schema.Type.NAMED_TYPE_REF) {
      color = registry.logicalType("color", registry.latest("color")).getNamedTypes()
          .get(color.getQualifiedName());
    }
    List<String> symbols = new ArrayList<>();
    color.getEnumValues().forEach(v -> symbols.add(v.getSymbol()));
    if (format == SchemaType.AVRO) {
      assertThat(symbols).containsExactly("RED", "PINK", "AMBER", "GREEN", "BLUE");
    } else {
      assertThat(symbols).containsExactlyInAnyOrder("RED", "PINK", "AMBER", "GREEN", "BLUE");
    }
    assertThatThrownBy(() -> registry.alter("color", new AddValue("RED", null, null)))
        .isInstanceOf(TypeRefusedException.class)
        .hasMessage("Enum color already has value RED");
    assertThatThrownBy(() -> registry.alter("color", new AddValue("X", "NOPE", null)))
        .isInstanceOf(TypeRefusedException.class).hasMessage("Enum color has no value NOPE");
  }

  // An enum has no attributes to add or place: it is told so, and what it takes.
  @Test
  void anAttributeOfAnEnumIsRefusedWithWhatAnEnumTakes() {
    registry.create("hue", SchemaType.AVRO, BodyKind.ENUM, "('RED', 'GREEN')");
    assertThatThrownBy(() -> registry.alter("hue", new AddAttribute("a", "INT")))
        .isInstanceOf(TypeRefusedException.class).hasMessage("Type hue is an enum, which has "
            + "values and no attributes; use ADD VALUE");
  }

  @Test
  void aTypeThatIsOneValueTakesANewDataTypeAndAStructDoesNot() {
    registry.create("id", SchemaType.AVRO, BodyKind.OTHER, "INT NOT NULL");
    registry.alter("id", new SetDataType("BIGINT NOT NULL"));
    assertThat(registry.schema("id").canonicalString()).isEqualTo("\"long\"");

    create("s", SchemaType.AVRO, "(a INT)");
    assertThatThrownBy(() -> registry.alter("s", new SetDataType("BIGINT")))
        .isInstanceOf(TypeRefusedException.class)
        .hasMessage("Type s is a struct; use ALTER ATTRIBUTE to change its attributes");
    assertThatThrownBy(() -> registry.alter("s", new AddValue("X", null, null)))
        .isInstanceOf(TypeRefusedException.class).hasMessage("Type s is not an enum");
  }

  @Test
  void aPolicyIsTheCallersToSetAndMakesNoVersion() {
    create("pol", SchemaType.AVRO, "(a INT)");
    TypeAlter.Result result = registry.alter("pol",
        new SetWith(Map.of("Compatibility.Policy", "logical")));

    assertThat(result.policy()).isEqualTo("LOGICAL");
    assertThat(result.edited()).isNull();
    assertThat(version("pol")).isEqualTo(1);
    assertThat(registry.config("pol").getCompatibilityPolicy()).isEqualTo("LOGICAL");
    assertThatThrownBy(() -> registry.alter("pol",
        new SetWith(Map.of("compatibility.policy", "loose"))))
        .isInstanceOf(TypeRefusedException.class)
        .hasMessage("Unknown compatibility.policy 'loose': it is LOGICAL, STRICT or LENIENT");
  }

  // No logical type records the number of a map's key or value that is no union: the writer
  // gives it the lowest one the other's branches leave free. A new branch is numbered above it.
  @Test
  void aMapsKeyOrValueKeepsItsNumberAsTheUnionBesideItGainsABranch() {
    create("k", SchemaType.PROTOBUF, "(id INT, mp MAP<UNION(x INT, y STRING), STRING>)");
    assertThat(entry("k", "mp")).isEqualTo("x=1 y=2 value=3");
    registry.alter("k", new AlterAttributeType("mp",
        "MAP<UNION(x INT, y STRING, z BIGINT), STRING>"));
    assertThat(entry("k", "mp")).isEqualTo("x=1 y=2 z=4 value=3");

    create("v", SchemaType.PROTOBUF, "(id INT, mp MAP<STRING, UNION(c INT, d STRING)>)");
    assertThat(entry("v", "mp")).isEqualTo("c=2 d=3 key=1");
    registry.alter("v", new AlterAttributeType("mp",
        "MAP<STRING, UNION(c INT, d STRING, e BIGINT)>"));
    assertThat(entry("v", "mp")).isEqualTo("c=2 d=3 e=4 key=1");
  }

  // A key and a value that are both unions are oneofs of one entry message.
  @Test
  void theUnionsOfAMapsKeyAndValueAreNumberedTogether() {
    create("both", SchemaType.PROTOBUF,
        "(id INT, mp MAP<UNION(a INT, b STRING), UNION(c INT, d STRING)>)");
    assertThat(entry("both", "mp")).isEqualTo("a=1 b=2 c=3 d=4");
    registry.alter("both", new AlterAttributeType("mp",
        "MAP<UNION(a INT, b STRING, e BIGINT), UNION(c INT, d STRING, f BIGINT)>"));
    assertThat(entry("both", "mp")).isEqualTo("a=1 b=2 e=5 c=3 d=4 f=6");
  }

  // Dropping a branch numbered below a map's value, or a multiset's count, would free a lower
  // number for it, and the writer would move it there.
  @Test
  void aBranchBelowAMapsValueOrAMultisetsCountIsNotDropped() {
    create("kd", SchemaType.PROTOBUF,
        "(id INT, mp MAP<UNION(x INT, y STRING, z BIGINT), STRING>)");
    assertThat(entry("kd", "mp")).isEqualTo("x=1 y=2 z=3 value=4");
    assertThatThrownBy(() -> registry.alter("kd", new AlterAttributeType("mp",
        "MAP<UNION(y STRING, z BIGINT), STRING>")))
        .isInstanceOf(TypeRefusedException.class)
        .hasMessage("A map's value would move from Protobuf field number 4 to 1, and what is "
            + "stored under 4 would not be read: it takes the lowest number the branches of "
            + "the union beside it leave free, so a branch numbered below it cannot be dropped");

    create("ms", SchemaType.PROTOBUF,
        "(id INT, mp MULTISET<UNION(x INT, y STRING, z BIGINT)>)");
    assertThat(entry("ms", "mp")).isEqualTo("x=1 y=2 z=3 value=4");
    assertThatThrownBy(() -> registry.alter("ms", new AlterAttributeType("mp",
        "MULTISET<UNION(x INT, y STRING)>")))
        .isInstanceOf(TypeRefusedException.class)
        .hasMessageStartingWith("A multiset's count would move from Protobuf field number 4 "
            + "to 3,");
    assertThat(version("kd")).isEqualTo(1);
    assertThat(version("ms")).isEqualTo(1);

    // A branch added since is numbered above the value, and can go.
    registry.alter("kd", new AlterAttributeType("mp",
        "MAP<UNION(x INT, y STRING, z BIGINT, w INT, v INT), STRING>"));
    assertThat(entry("kd", "mp")).isEqualTo("x=1 y=2 z=3 w=5 v=6 value=4");
    registry.alter("kd", new AlterAttributeType("mp",
        "MAP<UNION(x INT, y STRING, z BIGINT, v INT), STRING>"));
    assertThat(entry("kd", "mp")).isEqualTo("x=1 y=2 z=3 v=6 value=4");
  }

  // A type that referred to itself, or to one that refers to it, would lose its own definition.
  @ParameterizedTest
  @EnumSource(SchemaType.class)
  void aTypeRefersToNoTypeThatLeadsBackToIt(SchemaType format) {
    create("self", format, "(id INT NOT NULL)");
    assertThatThrownBy(() -> registry.alter("self", new AddAttribute("next", "self")))
        .isInstanceOf(TypeRefusedException.class)
        .hasMessage("Type self cannot refer to itself");

    create("b", format, "(id INT NOT NULL)");
    create("a", format, "(id INT NOT NULL, b b)");
    create("c", format, "(id INT NOT NULL, a a)");
    assertThatThrownBy(() -> registry.alter("b", new AddAttribute("a", "a")))
        .isInstanceOf(TypeRefusedException.class)
        .hasMessage("Type b cannot refer to type a, which refers to b");
    // Through another type, too.
    assertThatThrownBy(() -> registry.alter("b", new AddAttribute("c", "c")))
        .isInstanceOf(TypeRefusedException.class)
        .hasMessage("Type b cannot refer to type c, which refers to b");
    assertThat(version("self")).isEqualTo(1);
    assertThat(version("b")).isEqualTo(1);
  }

  // Protobuf takes an enum only with its first value numbered 0, and a value keeps its number.
  @Test
  void aProtobufEnumKeepsItsZeroValueFirst() {
    create("z", SchemaType.PROTOBUF, "(id INT, c ENUM('R', 'G', 'B'))");
    assertThatThrownBy(() -> registry.alter("z", new AlterAttributeType("c",
        "ENUM('G', 'R', 'B')")))
        .isInstanceOf(TypeRefusedException.class)
        .hasMessage("A Protobuf enum's first value is the one numbered 0: list R first");
    assertThatThrownBy(() -> registry.alter("z", new AlterAttributeType("c", "ENUM('B', 'G')")))
        .isInstanceOf(TypeRefusedException.class)
        .hasMessage("A Protobuf enum's first value is the one numbered 0: R, which is numbered "
            + "0 in the data written so far, cannot be dropped");
    assertThat(version("z")).isEqualTo(1);

    registry.alter("z", new AlterAttributeType("c", "ENUM('R', 'B', 'G')"));
    assertThat(registry.schema("z").canonicalString()).contains("R = 0;", "B = 2;", "G = 1;");
  }

  // A number given to one new value is not taken by another, given or not.
  @Test
  void newEnumValuesAreNumberedApart() {
    create("en", SchemaType.PROTOBUF, "(id INT, c ENUM('RED', 'GREEN'))");
    registry.alter("en", new AlterAttributeType("c",
        "ENUM('RED', 'GREEN', 'BLUE' WITH ('protobuf.enum.number' = '2'), 'YELLOW')"));
    assertThat(registry.schema("en").canonicalString())
        .contains("BLUE = 2;", "YELLOW = 3;").doesNotContain("allow_alias");

    assertThatThrownBy(() -> registry.alter("en", new AlterAttributeType("c", "ENUM('RED', "
        + "'GREEN', 'BLUE', 'YELLOW', 'A' WITH ('protobuf.enum.number' = '9'), "
        + "'B' WITH ('protobuf.enum.number' = '9'))")))
        .isInstanceOf(TypeRefusedException.class)
        .hasMessage("Values A and B cannot both be numbered 9: they would be one value in "
            + "the data");
  }

  // A field inside an attribute keeps the default Avro reads earlier records by, as an
  // attribute does, when its struct is given anew without one.
  @Test
  void aNestedFieldKeepsItsDefaultThroughARetype() {
    create("nd", SchemaType.AVRO, "(id INT, s STRUCT<y INT>)");
    registry.alter("nd", new AlterAttributeType("s",
        "STRUCT<y INT, x INT NOT NULL DEFAULT 5>"));
    registry.alter("nd", new AlterAttributeType("s", "STRUCT<y INT, x BIGINT NOT NULL>"));
    assertThat(registry.schema("nd").canonicalString())
        .contains("{\"name\":\"x\",\"type\":\"long\",\"default\":5}");
    // So a record written before x was is still read.
    assertThat(registry.schema("nd").isBackwardCompatible(
        registry.parse(registry.version("nd", 1)))).isEmpty();

    // A default the new text gives is the one it has.
    registry.alter("nd", new AlterAttributeType("s",
        "STRUCT<y INT, x BIGINT NOT NULL DEFAULT 7>"));
    assertThat(registry.schema("nd").canonicalString())
        .contains("{\"name\":\"x\",\"type\":\"long\",\"default\":7}");
    // One that the new type could not read is refused.
    assertThatThrownBy(() -> registry.alter("nd", new AlterAttributeType("s",
        "STRUCT<y INT, x BOOLEAN NOT NULL>")))
        .isInstanceOf(TypeRefusedException.class)
        .hasMessageStartingWith("Attribute x's default 7 cannot be read as BOOLEAN");

    // A type that is no struct keeps a nested default likewise, through SET DATA TYPE.
    registry.create("rd", SchemaType.AVRO, BodyKind.OTHER,
        "ARRAY<STRUCT<y INT, x INT NOT NULL DEFAULT 5>>");
    registry.alter("rd", new SetDataType("ARRAY<STRUCT<y INT, x BIGINT NOT NULL>>"));
    assertThat(registry.schema("rd").canonicalString())
        .contains("{\"name\":\"x\",\"type\":\"long\",\"default\":5}");
  }

  // A Protobuf schema written by hand may have what the logical type an ALTER edits does not
  // carry. Rewriting it would lose that, so it is refused.
  @Test
  void aProtobufSchemaAnAlterCouldNotKeepIsRefused() throws Exception {
    registry.client.register("res", new ProtobufSchema("syntax = \"proto3\";\n"
        + "message R { reserved 3, 4; reserved \"old\"; int32 a = 1; int32 b = 2; }"));
    assertThatThrownBy(() -> registry.alter("res", new AddAttribute("c", "INT")))
        .isInstanceOf(TypeRefusedException.class)
        .hasMessage("Type res cannot be altered: it reserves field numbers or names, which "
            + "would be dropped, and a new field could then take one. Register its new "
            + "version as a Protobuf schema");

    registry.client.register("two", new ProtobufSchema("syntax = \"proto2\";\n"
        + "message T { required int32 a = 1; optional int32 b = 2 [default = 7]; }"));
    assertThatThrownBy(() -> registry.alter("two", new AddAttribute("c", "INT")))
        .isInstanceOf(TypeRefusedException.class)
        .hasMessageStartingWith("Type two cannot be altered: it is not a proto3 file");
    assertThat(version("res")).isEqualTo(1);
    assertThat(version("two")).isEqualTo(1);
    // A statement that makes no new version loses nothing.
    assertThat(registry.alter("res", new SetWith(Map.of("compatibility.policy", "STRICT")))
        .edited()).isNull();

    // An integer's encoding is not carried either, in a nested message or a map's entry.
    registry.client.register("enc", new ProtobufSchema("syntax = \"proto3\";\n"
        + "message E { int32 a = 1; message N { map<string, sint64> m = 1; } N n = 2; }"));
    assertThatThrownBy(() -> registry.alter("enc", new AddAttribute("c", "INT")))
        .isInstanceOf(TypeRefusedException.class)
        .hasMessage("Type enc cannot be altered: field value of message mEntry is a sint64, "
            + "which would be written as int64, so its stored values would be read wrongly. "
            + "Register its new version as a Protobuf schema");
    registry.client.register("fix", new ProtobufSchema("syntax = \"proto3\";\n"
        + "message F { sfixed32 z = 1; }"));
    assertThatThrownBy(() -> registry.alter("fix", new AddAttribute("c", "INT")))
        .isInstanceOf(TypeRefusedException.class)
        .hasMessageContaining("field z of message F is a sfixed32, which would be written as "
            + "int32");

    // Nor is a map the file declares as one: it would be written as a repeated field of an
    // entry message, which is another kind of field to the registry and to generated code.
    registry.client.register("nm", new ProtobufSchema("syntax = \"proto3\";\n"
        + "message M { int32 z = 1; message In { int32 x = 1; } In p = 2; "
        + "message N { map<string, In> m = 1; } N n = 3; }"));
    assertThat(registry.struct("nm").getField("n").getSchema().getField("m").getSchema()
        .getType()).isEqualTo(Schema.Type.MAP);
    for (AlterAction action : List.of(new AddAttribute("c", "INT"),
        new MoveAttribute("p", Position.first()))) {
      assertThatThrownBy(() -> registry.alter("nm", action))
          .isInstanceOf(TypeRefusedException.class)
          .hasMessage("Type nm cannot be altered: field m of message N is a Protobuf map, "
              + "which would be written as a repeated field of an entry message, another kind "
              + "of field. Drop the attribute, or register its new version as a Protobuf "
              + "schema");
    }
    assertThat(version("nm")).isEqualTo(1);
    // A version that has no field of the map's number is written: the map dropped, or the
    // attribute whose message has it.
    registry.alter("nm", new DropAttribute("n", false));
    assertThat(names("nm")).containsExactly("z", "p");
    String flat = "syntax = \"proto3\";\nmessage T { int32 a = 1; map<string, int32> m = 2; }";
    registry.client.register("nt", new ProtobufSchema(flat));
    assertThatThrownBy(() -> registry.alter("nt", new RenameAttribute("m", "n")))
        .isInstanceOf(TypeRefusedException.class).hasMessageContaining("is a Protobuf map");
    registry.alter("nt", new DropAttribute("m", false), new AddAttribute("c", "INT"));
    assertThat(numbers(message("nt"))).isEqualTo("a=1 c=3");
    // And a type created again over a dropped version that had one, as an ALTER of it.
    registry.client.register("nd", new ProtobufSchema(flat.replace(" T ", " nd ")));
    registry.drop("nd");
    create("nd", SchemaType.PROTOBUF, "(a INT NOT NULL, b STRING)");
    assertThat(numbers(message("nd"))).isEqualTo("a=1 b=3");
    registry.client.register("ne", new ProtobufSchema(flat.replace(" T ", " ne ")));
    registry.drop("ne");
    assertThatThrownBy(() -> create("ne", SchemaType.PROTOBUF,
        "(a INT NOT NULL, m MAP<STRING, INT NOT NULL> NOT NULL)"))
        .isInstanceOf(TypeRefusedException.class).hasMessageContaining("is a Protobuf map");

    // Nor is a google.protobuf wrapper, which is read as the scalar it wraps and would be
    // written as that scalar, in other bytes: by a move as by any other statement.
    String wrapped = "syntax = \"proto3\";\n\nimport \"google/protobuf/wrappers.proto\";\n\n"
        + "message W {\n  string name = 1;\n  google.protobuf.StringValue sv = 2;\n"
        + "  repeated google.protobuf.Int32Value w = 3;\n}\n";
    registry.client.register("gw", new ProtobufSchema(wrapped));
    for (AlterAction action : List.of(new AddAttribute("c", "INT"),
        new MoveAttribute("name", Position.after("sv")))) {
      assertThatThrownBy(() -> registry.alter("gw", action))
          .isInstanceOf(TypeRefusedException.class)
          .hasMessage("Type gw cannot be altered: field sv of message W is a "
              + "google.protobuf.StringValue, which would be written as the value it wraps "
              + "and not as that message, so its stored values would be read wrongly. Drop "
              + "the attribute, or register its new version as a Protobuf schema");
    }
    assertThatThrownBy(() -> registry.alter("gw", new DropAttribute("sv", false)))
        .isInstanceOf(TypeRefusedException.class)
        .hasMessageContaining("field w of message W is a google.protobuf.Int32Value");
    assertThat(version("gw")).isEqualTo(1);
    registry.alter("gw", new DropAttribute("sv", false), new DropAttribute("w", false),
        new AddAttribute("c", "INT"));
    assertThat(numbers(message("gw"))).isEqualTo("name=1 c=4");

    // Each of the wrappers is one.
    for (String wrapper : List.of("DoubleValue", "FloatValue", "Int64Value", "UInt64Value",
        "Int32Value", "UInt32Value", "BoolValue", "StringValue", "BytesValue")) {
      String type = "gw" + wrapper;
      registry.client.register(type, new ProtobufSchema("syntax = \"proto3\";\n\n"
          + "import \"google/protobuf/wrappers.proto\";\n\nmessage R {\n  int32 id = 1;\n"
          + "  google.protobuf." + wrapper + " w = 2;\n}\n"));
      assertThatThrownBy(() -> registry.alter(type, new AddAttribute("c", "INT"))).as(wrapper)
          .isInstanceOf(TypeRefusedException.class)
          .hasMessageContaining("field w of message R is a google.protobuf." + wrapper);
    }
    // And one in a message beside the root that no field of the root comes to, which is
    // written as the root is.
    registry.client.register("gu", new ProtobufSchema("syntax = \"proto3\";\n\n"
        + "import \"confluent/meta.proto\";\nimport \"google/protobuf/wrappers.proto\";\n\n"
        + "option (confluent.file_meta) = { params: [ { key: \"logical.root\", value: "
        + "\"Root\" } ] };\n\nmessage Root {\n  int32 id = 1;\n}\nmessage Unused {\n"
        + "  google.protobuf.FloatValue w = 5;\n}\n"));
    assertThatThrownBy(() -> registry.alter("gu", new AddAttribute("c", "INT")))
        .isInstanceOf(TypeRefusedException.class).hasMessageContaining("field w of message "
            + "Unused is a google.protobuf.FloatValue");

    // A wrapper in an entry another file declares, which is written as an entry of this
    // file's, is one too; one in a message that stays another file's is not written at all.
    String imports = "syntax = \"proto3\";\n\nimport \"google/protobuf/wrappers.proto\";\n\n"
        + "package o;\n\n";
    String pair = imports + "message PairEntry {\n  string key = 1;\n"
        + "  google.protobuf.StringValue value = 2;\n}\n";
    String in = imports + "message In {\n  google.protobuf.StringValue sv = 1;\n}\n";
    registry.client.register("gp", new ProtobufSchema(pair));
    registry.client.register("gi", new ProtobufSchema(in));
    String uses = "syntax = \"proto3\";\n\nimport \"%s\";\n\nmessage R {\n  int32 id = 1;\n"
        + "  %s a = 2;\n}\n";
    registry.client.register("ge", new ProtobufSchema(String.format(uses, "gp",
        "repeated o.PairEntry"), List.of(new SchemaReference("gp", "gp", 1)),
        Map.of("gp", pair), null, null));
    assertThatThrownBy(() -> registry.alter("ge", new AddAttribute("c", "INT")))
        .isInstanceOf(TypeRefusedException.class).hasMessageContaining("field value of "
            + "message PairEntry is a google.protobuf.StringValue");
    registry.client.register("gr", new ProtobufSchema(String.format(uses, "gi", "o.In"),
        List.of(new SchemaReference("gi", "gi", 1)), Map.of("gi", in), null, null));
    registry.alter("gr", new AddAttribute("c", "INT"));
    assertThat(numbers(message("gr"))).isEqualTo("id=1 a=2 c=3");
    // Nor is one that another file's message takes the place of: neither is written.
    registry.client.register("go", new ProtobufSchema("syntax = \"proto3\";\n\npackage o;\n\n"
        + "message Other {\n  string name = 1;\n}\n"));
    registry.alter("gr", new AlterAttributeType("a", "go"));
    assertThat(registry.latest("gr").getReferences()).extracting(SchemaReference::getSubject)
        .containsExactly("go");
    // A message two fields have is still written for the one that keeps it.
    registry.client.register("g2", new ProtobufSchema("syntax = \"proto3\";\n\n"
        + "import \"google/protobuf/wrappers.proto\";\n\nmessage R {\n  int32 id = 1;\n"
        + "  S a = 2;\n  S b = 3;\n\n  message S {\n    int32 x = 1;\n"
        + "    google.protobuf.StringValue w = 2;\n  }\n}\n"));
    assertThatThrownBy(() -> registry.alter("g2", new AlterAttributeType("a", "go")))
        .isInstanceOf(TypeRefusedException.class)
        .hasMessageContaining("field w of message S is a google.protobuf.StringValue");
    // A message that has a field of its own type is walked once.
    registry.client.register("gt", new ProtobufSchema("syntax = \"proto3\";\n\n"
        + "message Tree {\n  int32 v = 1;\n  Tree left = 2;\n  Tree right = 3;\n}\n"));
    registry.alter("gt", new AddAttribute("c", "INT"));
    assertThat(version("gt")).isEqualTo(2);

    // One the logical type carries whole is altered as any other; a uint32 is read as a
    // BIGINT, whose int64 is the same encoding.
    registry.client.register("plain", new ProtobufSchema("syntax = \"proto3\";\n"
        + "message P { int32 a = 1; uint32 b = 2; }"));
    registry.alter("plain", new AddAttribute("c", "INT"));
    assertThat(numbers(message("plain"))).isEqualTo("a=1 b=2 c=3");
  }

  // A topic's key and value types share their constraints' names, whichever is altered.
  @Test
  void aTopicsKeyAndValueShareTheirConstraintNames() {
    registry.create("t-key", "t_key", SchemaType.AVRO, BodyKind.STRUCT,
        "(k INT, k2 INT, CONSTRAINT kk CHECK (k > 0))");
    registry.create("t-value", "t_value", SchemaType.AVRO, BodyKind.STRUCT,
        "(v INT, CONSTRAINT vv CHECK (v > 0))");
    TypeAlter.Options columns = new TypeAlter.Options().columns(true);
    assertThatThrownBy(() -> registry.alter("t-value", columns,
        new AddConstraint(new Check("kk", "v < 9", null))))
        .isInstanceOf(TypeRefusedException.class).hasMessageContaining("already exists");
    assertThatThrownBy(() -> registry.alter("t-key", columns,
        new AddConstraint(new Check("VV", "k < 9", null))))
        .isInstanceOf(TypeRefusedException.class).hasMessageContaining("already exists");

    // Types that are no topic's have their names to themselves.
    registry.alter("t-key", new AddConstraint(new Check("vv", "k < 9", null)));
    assertThat(registry.struct("t-key").getRules()).extracting(Rule::getName)
        .containsExactly("kk", "vv");
  }

  // The text of an added attribute is one attribute's.
  @Test
  void anAddedAttributeIsOneAttribute() {
    create("one", SchemaType.AVRO, "(a INT)");
    for (String text : List.of("INT, c STRING", "INT) WITH ('owner' = 'zz'",
        "INT, CHECK (b > 0)", "INT); TYPE STRUCT(x STRING", "INT); STRUCT other (y INT")) {
      assertThatThrownBy(() -> registry.alter("one", new AddAttribute("b", text)))
          .isInstanceOf(TypeRefusedException.class)
          .hasMessage("Attribute b is given more than one attribute's type and clauses: "
              + text);
    }
    assertThat(version("one")).isEqualTo(1);

    registry.alter("one", new AddAttribute("b", "INT CHECK (b > 0) COMMENT 'its own'"));
    assertThat(registry.struct("one").getFields()).extracting(Schema.Field::getName)
        .containsExactly("a", "b");
  }

  @Test
  void aToolsCheckIsGivenWhenItIsSet() {
    assertThatThrownBy(() -> new TypeAlter.Options().check(null))
        .isInstanceOf(NullPointerException.class);
  }

  // Protobuf keeps the numbers 19000 to 19999 for itself, and refuses a schema that has one.
  @Test
  void aNewProtobufNumberSkipsTheOnesProtobufKeeps() {
    // An added attribute, and the branches of an added union.
    create("r1", SchemaType.PROTOBUF, "(a INT WITH ('protobuf.field.number' = '18999'))");
    registry.alter("r1", new AddAttribute("b", "INT"));
    assertThat(numbers(message("r1"))).isEqualTo("a=18999 b=20000");
    create("r2", SchemaType.PROTOBUF, "(a INT WITH ('protobuf.field.number' = '18998'))");
    registry.alter("r2", new AddAttribute("u", "UNION(p INT, q STRING)"));
    assertThat(numbers(message("r2"))).isEqualTo("p=20000 q=20001 a=18998");

    // A field added to a struct inside the type, and a branch added to a union.
    create("r3", SchemaType.PROTOBUF,
        "(id INT, s STRUCT<n INT WITH ('protobuf.field.number' = '18999')>)");
    registry.alter("r3", new AlterAttributeType("s", "STRUCT<n INT, m INT>"));
    assertThat(numbers(message("r3").findFieldByName("s").getMessageType()))
        .isEqualTo("n=18999 m=20000");
    create("r4", SchemaType.PROTOBUF, "(id INT, u UNION(p INT WITH ('protobuf.field.number' = "
        + "'18998'), q STRING WITH ('protobuf.field.number' = '18999')))");
    registry.alter("r4", new AlterAttributeType("u", "UNION(p INT, q STRING, r BIGINT)"));
    assertThat(numbers(message("r4"))).isEqualTo("p=18998 q=18999 r=20000 id=1");
  }

  // A field number, an alias or a pinned type name is how the format reads an attribute's data.
  @Test
  void anAttributesFormatNativePropertiesCannotBeChanged() {
    create("p", SchemaType.PROTOBUF, "(a INT, b STRING)");
    assertThatThrownBy(() -> registry.alter("p",
        new SetAttributeWith("a", Map.of("protobuf.field.number", "3"))))
        .isInstanceOf(TypeRefusedException.class)
        .hasMessage("Property protobuf.field.number of attribute a cannot be set: it is how the "
            + "PROTOBUF format reads the attribute's existing data");
    create("v", SchemaType.AVRO, "(a INT)");
    assertThatThrownBy(() -> registry.alter("v",
        new SetAttributeWith("a", Map.of("avro.aliases", "z"))))
        .isInstanceOf(TypeRefusedException.class).hasMessageContaining("avro.aliases");
    assertThat(version("p")).isEqualTo(1);
    assertThat(version("v")).isEqualTo(1);

    // Other properties are the type's to set.
    registry.alter("v", new SetAttributeWith("a", Map.of("owner", "x")));
    assertThat(registry.struct("v").getFields().get(0).getParams()).containsEntry("owner", "x");
  }

  // A type's own name and aliases are how the format reads its data, as an attribute's are.
  @Test
  void aTypesFormatNativePropertiesCannotBeChanged() throws Exception {
    registry.client.register("old", new AvroSchema("{\"type\":\"record\",\"name\":\"Now\","
        + "\"aliases\":[\"Before\"],\"fields\":[{\"name\":\"a\",\"type\":\"int\"}]}"));
    assertThatThrownBy(() -> registry.alter("old", new SetWith(Map.of("avro.aliases", "Else"))))
        .isInstanceOf(TypeRefusedException.class)
        .hasMessage("Property avro.aliases of type old cannot be set: it is how the AVRO "
            + "format reads the type's existing data");
    assertThatThrownBy(() -> registry.alter("old", new SetWith(Map.of("avro.aliases", ""))))
        .isInstanceOf(TypeRefusedException.class).hasMessageContaining("avro.aliases");
    assertThatThrownBy(() -> registry.alter("old", new SetWith(Map.of("avro.name", "Other"))))
        .isInstanceOf(TypeRefusedException.class).hasMessageContaining("avro.name");
    create("pm", SchemaType.PROTOBUF, "(a INT)");
    assertThatThrownBy(() -> registry.alter("pm", new SetWith(Map.of("protobuf.name", "Other"))))
        .isInstanceOf(TypeRefusedException.class).hasMessageContaining("protobuf.name");
    assertThat(version("old")).isEqualTo(1);
    assertThat(version("pm")).isEqualTo(1);

    // One given as it is changes nothing, and another property is set beside it.
    registry.alter("old", new SetWith(Map.of("avro.aliases", "Before", "owner", "me")));
    assertThat(registry.schema("old").canonicalString())
        .contains("\"aliases\":[\"Before\"]").contains("\"owner\":\"me\"");
  }

  // A field or enum number that is none is refused by name, not by the registry without one.
  @Test
  void aProtobufNumberThatIsNoneIsRefused() {
    create("bn", SchemaType.PROTOBUF, "(a INT, c ENUM('RED', 'GREEN'))");
    assertThatThrownBy(() -> registry.alter("bn", new AlterAttributeType("c",
        "ENUM('RED', 'GREEN', 'BLUE' WITH ('protobuf.enum.number' = 'x'))")))
        .isInstanceOf(TypeRefusedException.class)
        .hasMessage("Property protobuf.enum.number of value BLUE is not a valid number: 'x'");
    assertThatThrownBy(() -> registry.alter("bn", new AddAttribute("e",
        "ENUM('RED', 'GREEN' WITH ('protobuf.enum.number' = '99999999999'))")))
        .isInstanceOf(TypeRefusedException.class)
        .hasMessageContaining("of value GREEN is not a valid number: '99999999999'");
    assertThatThrownBy(() -> registry.alter("bn", new AddAttribute("s",
        "STRUCT<x INT WITH ('protobuf.field.number' = 'x')>")))
        .isInstanceOf(TypeRefusedException.class)
        .hasMessage("Property protobuf.field.number of attribute x is not a valid number: 'x'");
    assertThatThrownBy(() -> registry.alter("bn", new AddAttribute("u",
        "UNION(p INT WITH ('protobuf.field.number' = ''), q STRING)")))
        .isInstanceOf(TypeRefusedException.class)
        .hasMessage("Property protobuf.field.number of branch p is not a valid number: ''");
    assertThat(version("bn")).isEqualTo(1);
  }

  // An enum value keeps the number it has had, and a number a value had is no other's.
  @Test
  void anEnumValueCannotBeGivenAnotherNumber() {
    create("e", SchemaType.PROTOBUF, "(id INT, c ENUM('RED', 'GREEN'))");
    assertThatThrownBy(() -> registry.alter("e", new AlterAttributeType("c",
        "ENUM('RED', 'GREEN' WITH ('protobuf.enum.number' = '5'))")))
        .isInstanceOf(TypeRefusedException.class)
        .hasMessage("Value GREEN cannot be numbered 5: it is numbered 1 in the data written "
            + "so far");
    assertThatThrownBy(() -> registry.alter("e", new AlterAttributeType("c",
        "ENUM('RED', 'BLUE' WITH ('protobuf.enum.number' = '1'))")))
        .isInstanceOf(TypeRefusedException.class)
        .hasMessage("Value BLUE cannot be numbered 1: that is the number of value GREEN in the "
            + "data written so far");

    // The number a value has, or one no value had, is accepted.
    registry.alter("e", new AlterAttributeType("c", "ENUM('RED', "
        + "'GREEN' WITH ('protobuf.enum.number' = '1'), 'BLUE' WITH ('protobuf.enum.number' = '7'))"));
    assertThat(registry.schema("e").canonicalString()).contains("BLUE = 7;");
  }

  // A topic's columns are one name whatever their case.
  @Test
  void aColumnNameDifferingOnlyInCaseIsRefused() {
    create("cols", SchemaType.AVRO, "(a INT, b INT)");
    TypeAlter.Options columns = new TypeAlter.Options().columns(true);
    assertThatThrownBy(() -> registry.alter("cols", columns, new AddAttribute("A", "INT")))
        .isInstanceOf(TypeRefusedException.class)
        .hasMessage("Type cols already has attribute a");
    assertThatThrownBy(() -> registry.alter("cols", columns, new RenameAttribute("b", "A")))
        .isInstanceOf(TypeRefusedException.class)
        .hasMessage("Type cols already has attribute a");
    assertThat(version("cols")).isEqualTo(1);

    // A column may take its own name in another case; a type's attributes keep their case.
    registry.alter("cols", columns, new RenameAttribute("a", "A"));
    registry.alter("cols", new AddAttribute("B", "INT"));
    assertThat(registry.struct("cols").getFields()).extracting(Schema.Field::getName)
        .containsExactly("A", "b", "B");
  }

  // A constraint names a column in the column's own case, so it is found however a statement
  // spells the column.
  @Test
  void aColumnOfAConstraintIsFoundInAnyCase() {
    create("cc", SchemaType.AVRO, "(A INT, b INT, CONSTRAINT pos CHECK (A > 0))");
    TypeAlter.Options columns = new TypeAlter.Options().columns(true);
    assertThatThrownBy(() -> registry.alter("cc", columns, new DropAttribute("a", false)))
        .isInstanceOf(TypeRefusedException.class)
        .hasMessage("Attribute a is used by constraint pos; drop the constraint first");
    assertThatThrownBy(() -> registry.alter("cc", columns,
        new AddConstraint(new Check("low", "a < 9", null))))
        .isInstanceOf(TypeRefusedException.class)
        .hasMessageContaining("Unknown column in CHECK: 'a'");
    assertThat(version("cc")).isEqualTo(1);

    registry.alter("cc", columns, new RenameAttribute("a", "z"));
    assertThat(registry.struct("cc").getRules()).extracting(Rule::getSql)
        .containsExactly("z > 0");
  }

  // The registration carries the version, so the registry refuses it after another.
  @Test
  void theRequestCarriesTheVersionTheAlterTakes() {
    create("rq", SchemaType.AVRO, "(a INT)");
    TypeAlter.Result result = TypeAlter.alter(registry, "rq",
        List.of(new AddAttribute("b", "INT")), new TypeAlter.Options());
    assertThat(result.request().getVersion()).isEqualTo(2);
    assertThat(result.request().getSchemaType()).isEqualTo("AVRO");
    assertThat(TypeAlter.alter(registry, "rq", List.of(new SetWith(
        Map.of("compatibility.policy", "LOGICAL"))), new TypeAlter.Options()).request())
        .isNull();
  }

  // A change back to the live version's type would leave the soft-deleted version's records
  // unreadable. The registry checks dropped versions only under LOGICAL, so it is refused here.
  @ParameterizedTest
  @EnumSource(value = SchemaType.class, names = {"AVRO", "JSON"})
  void anAlterOverASoftDeletedVersionIncompatiblyIsRefused(SchemaType format) {
    create("o", format, "(id INT NOT NULL, a INT)");
    registry.alter("o", new AlterAttributeType("a",
        format == SchemaType.AVRO ? "BIGINT" : "STRING"));
    registry.softDelete("o", 2);

    assertThatThrownBy(() -> registry.alter("o", new AlterAttributeType("a", "INT"),
        new AddAttribute("n", "INT")))
        .isInstanceOf(TypeRefusedException.class).hasMessageStartingWith("Type o cannot be "
            + "changed so: it is incompatible with version 2 of type o, whose records it "
            + "reads, at BACKWARD compatibility: {errorType:\"" + (format == SchemaType.AVRO
            ? "MISSING_UNION_BRANCH" : "COMBINED_TYPE_SUBSCHEMAS_CHANGED"));
    assertThat(registry.versions("o", true)).containsExactly(1, 2);
  }

  // A reference moved back is refused, unless the policy is LOGICAL, whose check reads
  // soft-deleted versions too.
  @Test
  void aReferenceMovedBackIsRefusedUnlessThePolicyIsLogical() {
    create("com.acme.pair", SchemaType.AVRO, "(a STRING, b STRING)");
    registry.alter("com.acme.pair", new AddAttribute("c", "INT"));
    create("o", SchemaType.AVRO, "(id INT NOT NULL)");
    registry.alter("o", new AddAttribute("p", "com.acme.pair"));
    registry.softDelete("o", 2);
    registry.softDelete("com.acme.pair", 2);

    assertThatThrownBy(() -> registry.alter("o", new AddAttribute("n", "INT")))
        .isInstanceOf(TypeRefusedException.class).hasMessage("Type o cannot be changed so: its "
            + "reference to type com.acme.pair would move back from version 2 to version 1, "
            + "the latest live one, because version 2 is soft-deleted; a version cannot refer "
            + "to an earlier version of a type than the one it follows; drop the attributes "
            + "of type com.acme.pair, or register a new version of it carrying the changes of "
            + "version 2");
    assertThat(registry.versions("o", true)).containsExactly(1, 2);

    registry.alter("o", new SetWith(Map.of("compatibility.policy", "LOGICAL")),
        new AddAttribute("n", "INT"));
    assertThat(registry.latest("o").getReferences().get(0).getVersion()).isEqualTo(1);
  }

  // A schema holds one version of a name, the first its references are read to, and they are
  // stored in another order than the statement gives them: the version would be checked as
  // one schema and stored as another.
  @ParameterizedTest
  @EnumSource(SchemaType.class)
  void aVersionReferringToTwoVersionsOfATypeIsRefused(SchemaType format) {
    create("inner", format, "(x STRING, y STRING)");
    create("pair", format, "(a STRING, i inner)");
    create("o", format, "(id INT NOT NULL, p pair)");
    // By two ways at one version, a type is one type.
    create("d", format, "(id INT NOT NULL, p pair, j inner)");
    registry.alter("inner", new AddAttribute("z", "INT"));

    assertThatThrownBy(() -> registry.alter("o", new AddAttribute("j", "inner")))
        .isInstanceOf(TypeRefusedException.class).hasMessage("Type o cannot be changed so: it "
            + "would refer to two versions of type inner, version 1 (through version 1 of "
            + "type pair) and version 2 (named directly); a schema has one version of a type, "
            + "so name the type again to move it to its latest version, or first move the "
            + "types that refer to it");
    assertThat(version("o")).isEqualTo(1);

    // The type that refers to it is moved first, and named again where it is used.
    registry.alter("pair", new AlterAttributeType("i", "inner"));
    assertThatThrownBy(() -> registry.alter("d", new AlterAttributeType("p", "pair")))
        .isInstanceOf(TypeRefusedException.class).hasMessageContaining("it would refer to two "
            + "versions of type inner, version 1 (named directly) and version 2 (through "
            + "version 2 of type pair)");
    for (String type : List.of("o", "d")) {
      List<AlterAction> actions = new ArrayList<>(List.of(new AlterAttributeType("p", "pair")));
      actions.add(type.equals("o") ? new AddAttribute("j", "inner")
          : new AlterAttributeType("j", "inner"));
      registry.alter(type, actions.toArray(new AlterAction[0]));
      assertThat(registry.latest(type).getReferences())
          .extracting(SchemaReference::getSubject, SchemaReference::getVersion)
          .containsExactlyInAnyOrder(tuple("inner", 2), tuple("pair", 2));
    }
  }

  // One name is one type too: a type of another context refers to its own inner by the
  // name this type's inner has, and the registry would read the first of the two for both.
  @ParameterizedTest
  @EnumSource(SchemaType.class)
  void aVersionReferringToTwoTypesByOneNameIsRefused(SchemaType format) {
    create("inner", format, "(q BIGINT, s STRING)");
    registry.create(":.c:inner", "inner", format, BodyKind.STRUCT, "(q BIGINT)");
    registry.create(":.c:pair", "pair", format, BodyKind.STRUCT, "(a STRING, i inner)");
    create("o", format, "(id INT NOT NULL, d inner)");

    String both = "would both be referred to as inner, which can name only one of them";
    assertThatThrownBy(() -> registry.alter("o", new AddAttribute("p", "`:.c:pair`")))
        .isInstanceOf(TypeRefusedException.class).hasMessageEndingWith(both)
        .hasMessageContaining("inner and :.c:inner");
    assertThatThrownBy(() -> create("ob", format, "(id INT NOT NULL, d inner, p `:.c:pair`)"))
        .isInstanceOf(TypeRefusedException.class).hasMessageEndingWith(both);
    assertThat(version("o")).isEqualTo(1);
    assertThat(registry.versions("ob", true)).isEmpty();

    // Without the attribute of the one, the other is added.
    registry.alter("o", new DropAttribute("d", false), new AddAttribute("p", "`:.c:pair`"));
    assertThat(version("o")).isEqualTo(2);
  }

  // A type of another context refers to its own inner as inner, and from here that type is
  // named with its context: named both ways, a Protobuf file would be imported twice. The one
  // type is referred to by the one name, and its two versions are told as such.
  @ParameterizedTest
  @EnumSource(SchemaType.class)
  void aTypeNamedWithATypeThatRefersToItIsReferredToByOneName(SchemaType format) {
    registry.create(":.c:inner", "inner", format, BodyKind.STRUCT, "(x STRING)");
    registry.create(":.c:pair", "pair", format, BodyKind.STRUCT, "(a STRING, i inner)");
    // In either order, and added to a type that has the other.
    create("o1", format, "(id INT NOT NULL, p `:.c:pair`, k `:.c:inner`)");
    create("o2", format, "(id INT NOT NULL, k `:.c:inner`, p `:.c:pair`)");
    create("o3", format, "(id INT NOT NULL, p `:.c:pair`)");
    create("o4", format, "(id INT NOT NULL, p `:.c:pair`)");
    registry.alter("o3", new AddAttribute("k", "`:.c:inner`", Position.first()));
    // Whichever a type had first, and both added by one statement to a type with another.
    create("o5", format, "(id INT NOT NULL, k `:.c:inner`)");
    registry.alter("o5", new AddAttribute("p", "`:.c:pair`"));
    create("other", format, "(v INT)");
    create("o6", format, "(id INT NOT NULL, x other)");
    registry.alter("o6", new AddAttribute("p", "`:.c:pair`"), new AddAttribute("k", "`:.c:inner`"),
        new DropAttribute("x", false));
    for (String type : List.of("o1", "o2", "o3", "o5", "o6")) {
      List<SchemaReference> references = registry.latest(type).getReferences();
      assertThat(references).as(type).extracting(SchemaReference::getSubject)
          .containsExactlyInAnyOrder(":.c:inner", ":.c:pair");
      if (format != SchemaType.JSON) {
        assertThat(references).as(type).extracting(SchemaReference::getName).contains("inner");
      }
    }

    registry.alter(":.c:inner", new AddAttribute("z", "INT"));
    if (format == SchemaType.JSON) {
      // A JSON $ref names a document, and two names are two documents, each read as it is.
      registry.alter("o4", new AddAttribute("k", "`:.c:inner`"));
      assertThat(version("o4")).isEqualTo(2);
      return;
    }
    assertThatThrownBy(() -> registry.alter("o4", new AddAttribute("k", "`:.c:inner`")))
        .isInstanceOf(TypeRefusedException.class).hasMessageContaining(
            "it would refer to two versions of type :.c:inner, version 1 (through version 1 of "
                + "type :.c:pair) and version 2 (named directly)");
  }

  // A type the statement drops is no longer referred to, and its name for a type is not this
  // one's: the type keeps the name it is given here, which another type may then not have.
  @ParameterizedTest
  @EnumSource(value = SchemaType.class, names = {"AVRO", "PROTOBUF"})
  void aReferenceIsNotNamedAsATypeTheStatementDropsNamesIt(SchemaType format) {
    registry.create(":.c:inner", "inner", format, BodyKind.STRUCT, "(x STRING)");
    registry.create(":.c:pair", "pair", format, BodyKind.STRUCT, "(a STRING, i inner)");
    registry.create("inner", "dflt.inner", format, BodyKind.STRUCT, "(q BIGINT, s STRING)");
    create("o", format, "(id INT NOT NULL, p `:.c:pair`)");

    registry.alter("o", new DropAttribute("p", false), new AddAttribute("k", "`:.c:inner`"));
    assertThat(registry.latest("o").getReferences()).extracting(SchemaReference::getName,
        SchemaReference::getSubject).containsExactly(tuple(":.c:inner", ":.c:inner"));
    registry.alter("o", new AddAttribute("d", "inner"));
    assertThat(registry.latest("o").getReferences()).extracting(SchemaReference::getSubject)
        .containsExactlyInAnyOrder(":.c:inner", "inner");

    // And one type named two ways in a statement, with its context and without, is one.
    registry.create(":.c:two", "two", format, BodyKind.STRUCT,
        "(id INT NOT NULL, k inner, k2 `:.c:inner`)");
    assertThat(registry.latest(":.c:two").getReferences())
        .extracting(SchemaReference::getSubject).hasSize(1);
  }

  // A reference the statement drops and names again is the one it was, by the name another
  // tool gave it: a file of that tool imports the type by that name.
  @Test
  void aReferenceDroppedAndNamedAgainKeepsItsName() throws Exception {
    String common = "syntax = \"proto3\";\n\nmessage PostalAddress {\n  string street = 1;\n}\n";
    registry.client.register("common", new ProtobufSchema(common));
    registry.client.register("cust", new ProtobufSchema("syntax = \"proto3\";\n\n"
        + "import \"common.proto\";\n\nmessage Cust {\n  string id = 1;\n"
        + "  optional .PostalAddress billing = 2;\n  optional .PostalAddress home = 3;\n}\n",
        List.of(new SchemaReference("common.proto", "common", 1)),
        Map.of("common.proto", common), null, null));
    registry.alter("cust", new DropAttribute("billing", false), new DropAttribute("home", false),
        new AddAttribute("shipping", "common"));
    assertThat(registry.latest("cust").getReferences()).extracting(SchemaReference::getName,
        SchemaReference::getSubject).containsExactly(tuple("common.proto", "common"));
    assertThat(registry.schema("cust").canonicalString()).contains("import \"common.proto\";");

    // It refers to the latest version of its subject, by the subject as it was spelt.
    String cust = "syntax = \"proto3\";\n\nimport \"common.proto\";\n\nmessage Cust {\n"
        + "  string id = 1;\n  optional .PostalAddress billing = 2;\n}\n";
    registry.client.register("cust2", new ProtobufSchema(cust,
        List.of(new SchemaReference("common.proto", ":.:common", 1)),
        Map.of("common.proto", common), null, null));
    registry.client.register("common", new ProtobufSchema(common.replace("}\n",
        "  string city = 2;\n}\n")));
    registry.alter("cust2", new DropAttribute("billing", false),
        new AddAttribute("shipping", "common"));
    assertThat(registry.latest("cust2").getReferences()).extracting(SchemaReference::getName,
        SchemaReference::getSubject, SchemaReference::getVersion)
        .containsExactly(tuple("common.proto", ":.:common", 2));

    // A reference to another subject takes no name of the one dropped.
    registry.client.register("other", new ProtobufSchema(common.replace("PostalAddress", "O")));
    registry.client.register("cust3", new ProtobufSchema(cust,
        List.of(new SchemaReference("common.proto", "common", 1)),
        Map.of("common.proto", common), null, null));
    registry.alter("cust3", new DropAttribute("billing", false), new AddAttribute("o", "other"));
    assertThat(registry.latest("cust3").getReferences()).extracting(SchemaReference::getName,
        SchemaReference::getSubject).containsExactly(tuple("other", "other"));

    // Two types under one name are told as that, not as a type the schema does not know.
    registry.client.register("cust4", new ProtobufSchema(cust.replace("common.proto", "other"),
        List.of(new SchemaReference("other", "common", 1)), Map.of("other", common), null,
        null));
    assertThatThrownBy(() -> registry.alter("cust4", new AddAttribute("o", "other")))
        .isInstanceOf(TypeRefusedException.class)
        .hasMessageContaining("would both be referred to as other");
  }

  // A type is named in a statement otherwise than its reference is: by its subject where
  // another tool gave an import path, with its context or without, or after the reference
  // took the name the types beside it use. Named again, it is the type already referred to.
  @ParameterizedTest
  @EnumSource(value = SchemaType.class, names = {"AVRO", "PROTOBUF"})
  void aTypeNamedAgainOtherwiseThanItsReferenceIsTheOneReferredTo(SchemaType format)
      throws Exception {
    registry.create(":.c:inner", "inner", format, BodyKind.STRUCT, "(x STRING)");
    registry.create(":.c:pair", "pair", format, BodyKind.STRUCT, "(a STRING, i inner)");
    create("o1", format, "(id INT NOT NULL, p `:.c:pair`, k `:.c:inner`)");
    registry.create(":.c:t", "t", format, BodyKind.STRUCT, "(id INT NOT NULL, k inner)");
    create("o5", format, "(id INT NOT NULL, k `:.c:inner`)");
    registry.alter("o5", new AddAttribute("p", "`:.c:pair`"));

    registry.alter("o1", new AddAttribute("k2", "`:.c:inner`"));
    registry.alter(":.c:t", new AddAttribute("k2", "`:.c:inner`"));
    registry.alter("o5", new AddAttribute("k2", "`:.c:inner`"));
    // Said to be the type it is, an attribute is as it was.
    assertThat(registry.alter("o1", new AlterAttributeType("k", "`:.c:inner`")).edited())
        .isNull();
    for (String type : List.of("o1", ":.c:t", "o5")) {
      assertThat(registry.latest(type).getReferences()).as(type)
          .extracting(SchemaReference::getSubject).doesNotHaveDuplicates();
      assertThat(registry.struct(type).getFields()).as(type)
          .extracting(Schema.Field::getName).contains("k", "k2");
    }
    if (format != SchemaType.PROTOBUF) {
      return;
    }
    // A file another tool registered, imported by a path of its own choosing.
    String common = "syntax = \"proto3\";\n\nmessage PostalAddress {\n  string street = 1;\n}\n";
    registry.client.register("common", new ProtobufSchema(common));
    SchemaReference reference = new SchemaReference("common.proto", "common", 1);
    registry.client.register("cust", new ProtobufSchema("syntax = \"proto3\";\n\n"
        + "import \"common.proto\";\n\nmessage Cust {\n  string id = 1;\n"
        + "  optional .PostalAddress billing = 2;\n}\n", List.of(reference),
        Map.of("common.proto", common), null, null));
    registry.alter("cust", new AddAttribute("shipping", "common"));
    assertThat(version("cust")).isEqualTo(2);
    assertThat(registry.latest("cust").getReferences()).containsExactly(reference);
  }

  // A type's own name is how the types that refer to it name it: a type it refers to is not
  // given that name, and a type whose name is met again below it, as another type's, is one
  // no type can refer to.
  @ParameterizedTest
  @EnumSource(value = SchemaType.class, names = {"AVRO", "PROTOBUF"})
  void aReferenceIsNotGivenTheNameOfTheTypeThatHasIt(SchemaType format) {
    registry.create(":.c:inner", "x.inner", format, BodyKind.STRUCT, "(q BIGINT)");
    registry.create(":.c:pair", "pair", format, BodyKind.STRUCT, "(a STRING, i inner)");
    // Its own inner below it, through pair, with no reference renamed.
    create("inner", format, "(id INT NOT NULL, p `:.c:pair`)");
    assertThatThrownBy(() -> create("u", format, "(id INT NOT NULL, d inner)"))
        .isInstanceOf(TypeRefusedException.class).hasMessage("Types inner and :.c:inner would "
            + "both be referred to as inner, which can name only one of them");

    // Altered or created with both, its reference to the other inner keeps the name given.
    for (boolean altered : new boolean[] {true, false}) {
      String type = altered ? "inner" : ":.d:inner";
      try {
        if (altered) {
          registry.alter(type, new AddAttribute("k", "`:.c:inner`"));
        } else {
          registry.create(type, "y.inner", format, BodyKind.STRUCT,
              "(id INT NOT NULL, k `:.c:inner`, p `:.c:pair`)");
        }
      } catch (TypeRefusedException | IllegalStateException e) {
        // A Protobuf file cannot import the one type by two names: refused here when
        // altered, and by the registry when created.
        assertThat(format).isEqualTo(SchemaType.PROTOBUF);
        assertThat(e.getMessage()).containsAnyOf("Duplicate named type",
            "Invalid schema of type PROTOBUF");
        continue;
      }
      assertThat(registry.latest(type).getReferences()).as(type)
          .extracting(SchemaReference::getName).doesNotContain("inner");
    }
  }

  // A version stored so, as another tool registers one, is altered only by a statement that
  // leaves it one version of the type.
  @Test
  void aVersionStoredWithTwoVersionsOfATypeIsRepairedByNamingItsTypeAgain() throws Exception {
    create("inner", SchemaType.AVRO, "(x STRING, y STRING)");
    create("pair", SchemaType.AVRO, "(a STRING, i inner)");
    registry.alter("inner", new AddAttribute("z", "INT"));
    Map<String, String> resolved = new LinkedHashMap<>();
    resolved.put("inner", registry.version("inner", 2).getSchema());
    resolved.put("pair", registry.version("pair", 1).getSchema());
    registry.client.register("m", new AvroSchema("{\"type\": \"record\", \"name\": \"m\", "
        + "\"fields\": [{\"name\": \"id\", \"type\": \"int\"}, {\"name\": \"p\", \"type\": "
        + "[\"null\", \"pair\"]}, {\"name\": \"j\", \"type\": [\"null\", \"inner\"], "
        + "\"default\": null}]}", List.of(new SchemaReference("inner", "inner", 2),
            new SchemaReference("pair", "pair", 1)), resolved, null));
    // A type that refers to it reads it as it is read alone, whatever it is.
    create("top", SchemaType.AVRO, "(id INT NOT NULL, t m)");
    registry.alter("top", new AddAttribute("j", "inner"));
    registry.alter("top", new AddAttribute("z", "INT"));
    assertThat(version("top")).isEqualTo(3);
    // Beside the type it has an older version through, it is two versions, in either order.
    for (String body : List.of("(id INT NOT NULL, t m, p pair)",
        "(id INT NOT NULL, p pair, t m)")) {
      assertThatThrownBy(() -> create("both", SchemaType.AVRO, body))
          .isInstanceOf(TypeRefusedException.class).hasMessageContaining("it would refer to "
              + "two versions of type inner, version ");
    }
    registry.alter("pair", new AlterAttributeType("i", "inner"));

    assertThatThrownBy(() -> registry.alter("m", new AddAttribute("n", "INT")))
        .isInstanceOf(TypeRefusedException.class).hasMessageContaining("it would refer to two "
            + "versions of type inner, version 2 (named directly) and version 1 (through "
            + "version 1 of type pair)");
    registry.alter("m", new AlterAttributeType("p", "pair"), new AddAttribute("n", "INT"));
    assertThat(version("m")).isEqualTo(2);
    assertThat(registry.latest("m").getReferences())
        .extracting(SchemaReference::getSubject, SchemaReference::getVersion)
        .containsExactlyInAnyOrder(tuple("inner", 2), tuple("pair", 2));
  }

  // A statement that makes no version of a type stored so is not refused: nothing is written.
  @Test
  void aStatementThatLeavesAVersionOfTwoVersionsOfATypeAsItIsMakesNone() throws Exception {
    create("inner", SchemaType.AVRO, "(x STRING, y STRING)");
    create("pair", SchemaType.AVRO, "(a STRING, i inner)");
    create("w", SchemaType.AVRO, "(id INT NOT NULL, p pair, j inner)");
    registry.alter("inner", new AddAttribute("z", "INT"));
    // The type as this writer writes it, stored with the newer inner beside the older pair.
    SchemaMetadata written = registry.latest("w");
    Map<String, String> resolved = new LinkedHashMap<>();
    resolved.put("inner", registry.version("inner", 2).getSchema());
    resolved.put("pair", registry.version("pair", 1).getSchema());
    registry.client.register("m", new AvroSchema(written.getSchema().replace("\"w\"", "\"m\""),
        List.of(new SchemaReference("inner", "inner", 2),
            new SchemaReference("pair", "pair", 1)), resolved, written.getMetadata(), null,
        null, false));

    assertThat(registry.alter("m", new AddAttribute("q", "INT"),
        new DropAttribute("q", false)).edited()).isNull();
    assertThatThrownBy(() -> registry.alter("m", new AddAttribute("q", "INT")))
        .isInstanceOf(TypeRefusedException.class).hasMessageContaining("two versions of type");
    assertThat(version("m")).isEqualTo(1);
  }

  // A Protobuf type created again over its dropped versions is numbered as an ALTER of the
  // last of them would number it, and an Avro one defaulted so.
  @Test
  void aTypeCreatedAgainKeepsTheNumbersAndDefaultsAnAlterWouldGive() {
    create("p", SchemaType.PROTOBUF, "(a INT NOT NULL, b STRING, c BIGINT)");
    ProtobufSchema again = (ProtobufSchema) Formats.toNative(SchemaType.PROTOBUF,
        TypeAlter.numberedAfterDrop(registry, "p", registry.latest("p"), TypeScripts.parse(
            TypeScripts.createScript("p", BodyKind.STRUCT, "(c BIGINT, a INT NOT NULL, d STRING)",
                Map.of(), Map.of()))), "p");
    assertThat(again.toDescriptor().findFieldByName("a").getNumber()).isEqualTo(1);
    assertThat(again.toDescriptor().findFieldByName("c").getNumber()).isEqualTo(3);
    assertThat(again.toDescriptor().findFieldByName("d").getNumber()).isEqualTo(4);

    create("v", SchemaType.AVRO, "(a INT)");
    Schema defaulted = TypeAlter.defaultedAfterDrop(registry, "v", registry.latest("v"),
        TypeScripts.parse(TypeScripts.createScript("v", BodyKind.STRUCT, "(a INT, b STRING)",
            Map.of(), Map.of()))).getRootSchema();
    assertThat(defaulted.getFields().get(0).hasDefaultValue()).isFalse();
    assertThat(defaulted.getFields().get(1).hasDefaultValue()).isTrue();
    assertThat(defaulted.getFields().get(1).getDefaultValue()).isNull();
  }

  // A new version continues the most recent one, soft-deleted or not, and takes the number
  // after it.
  @Test
  void anAlterContinuesASoftDeletedVersion() {
    create("sd", SchemaType.AVRO, "(a INT, b INT)");
    registry.alter("sd", new AddAttribute("c", "INT"));
    registry.softDelete("sd", 2);
    assertThat(version("sd")).isEqualTo(1);
    assertThat(TypeAlter.nextVersion(registry, "sd")).isEqualTo(3);

    TypeAlter.Result result = registry.alter("sd", new AddAttribute("d", "INT"));
    assertThat(result.base().getVersion()).isEqualTo(2);
    assertThat(result.version()).isEqualTo(3);
    assertThat(registry.struct("sd").getFields()).extracting(Schema.Field::getName)
        .containsExactly("a", "b", "c", "d");
    assertThat(TypeAlter.nextVersion(registry, "nope")).isEqualTo(1);
  }

  // At a transitive level a new version reads the records of every version before it, the
  // soft-deleted ones too, which the registry leaves out of its check.
  @Test
  void aTransitiveLevelCountsEverySoftDeletedVersion() throws Exception {
    for (String t : List.of("live", "gone", "plain")) {
      create(t, SchemaType.AVRO, "(a INT)");
      if (!t.equals("plain")) {
        registry.client.updateCompatibility(t, "BACKWARD_TRANSITIVE");
      }
      registry.alter(t, new AddAttribute("b", "STRING"));
      registry.alter(t, new DropAttribute("b", false), new AddAttribute("c", "INT"));
      registry.softDelete(t, 2);
    }
    // Whether the version it follows is live or not.
    registry.softDelete("gone", 3);
    for (String t : List.of("live", "gone")) {
      assertThatThrownBy(() -> registry.alter(t, new AddAttribute("b", "INT")))
          .isInstanceOf(TypeRefusedException.class)
          .hasMessageStartingWith("Type " + t + " cannot be changed so: it is incompatible "
              + "with version 2 of type " + t + ", whose records it reads, at "
              + "BACKWARD_TRANSITIVE compatibility: ");
      assertThat(TypeAlter.nextVersion(registry, t)).isEqualTo(4);
    }
    // At a level that is not transitive only the version it follows counts.
    registry.alter("plain", new AddAttribute("b", "INT"));
    assertThat(version("plain")).isEqualTo(4);
  }

  // An attribute renamed to the name it has is as it was, and keeps the name it had before.
  @Test
  void anAttributeRenamedToItsOwnNameIsAsItWas() {
    create("same", SchemaType.AVRO, "(a INT, b STRING)");
    registry.alter("same", new RenameAttribute("b", "bb"), new RenameAttribute("bb", "bb"));
    assertThat(registry.struct("same").getFields()).extracting(Schema.Field::getName)
        .containsExactly("a", "bb");
    assertThat(registry.struct("same").getFields().get(1).getAliases()).containsExactly("b");

    assertThat(registry.alter("same", new RenameAttribute("bb", "bb")).edited()).isNull();
    assertThat(version("same")).isEqualTo(2);
  }

  // A new version no schema of its format can be made of is refused with the reason, not left
  // to the registry with the checks of it skipped.
  @Test
  void aVersionNoSchemaCanBeMadeOfIsRefused() {
    create("un", SchemaType.AVRO, "(a INT)");
    assertThatThrownBy(() -> registry.alter("un", new AddAttribute("a-b", "INT")))
        .isInstanceOf(TypeRefusedException.class)
        .hasMessageStartingWith("Type un cannot be changed so: no AVRO schema can be made of "
            + "it: ");
    assertThat(version("un")).isEqualTo(1);
  }

  // The attributes of the latest version, in the order they are read in.
  private List<String> names(String subject) {
    return registry.logicalType(subject, registry.latest(subject)).getRootSchema().getFields()
        .stream().map(Schema.Field::getName).collect(Collectors.toList());
  }

  // The provenance id of each attribute of the latest version, by its name there.
  private Map<String, Integer> pids(String subject) throws Exception {
    List<ProvenanceVersion> versions = registry.client
        .getProvenanceByVersion(subject, "1", "latest", true).getVersions();
    Map<String, Integer> pids = new LinkedHashMap<>();
    for (ProvenanceField f : versions.get(versions.size() - 1).getFields()) {
      if (f.getPath().size() == 1) {
        pids.put(f.getNames().get(0), f.getPid());
      }
    }
    return pids;
  }

  @ParameterizedTest
  @EnumSource(SchemaType.class)
  void anAttributeIsAddedAtAPosition(SchemaType format) {
    create("ap", format, "(a INT NOT NULL, b STRING)");
    registry.alter("ap", new AddAttribute("c", "DOUBLE", Position.after("a")),
        new AddAttribute("d", "DOUBLE", Position.first()), new AddAttribute("e", "DOUBLE"));
    assertThat(names("ap")).containsExactly("d", "a", "c", "b", "e");
    // After one this statement adds, and not after one it drops or none has.
    registry.alter("ap", new AddAttribute("f", "INT"),
        new AddAttribute("g", "INT", Position.after("f")));
    assertThat(names("ap")).containsExactly("d", "a", "c", "b", "e", "f", "g");
    assertThatThrownBy(() -> registry.alter("ap", new DropAttribute("e", false),
        new AddAttribute("h", "INT", Position.after("e"))))
        .isInstanceOf(TypeRefusedException.class).hasMessage("Type ap has no attribute e");
    assertThatThrownBy(() -> registry.alter("ap",
        new AddAttribute("h", "INT", Position.after("h"))))
        .isInstanceOf(TypeRefusedException.class).hasMessage("Type ap has no attribute h");
    assertThat(version("ap")).isEqualTo(3);
  }

  @ParameterizedTest
  @EnumSource(SchemaType.class)
  void anAttributeIsMovedAndStaysTheAttributeItWas(SchemaType format) throws Exception {
    create("mv", format, "(a INT NOT NULL, b STRING COMMENT 'bee', c BIGINT)");
    Map<String, Integer> before = pids("mv");
    registry.alter("mv", new MoveAttribute("c", Position.first()),
        new MoveAttribute("a", Position.after("b")));
    assertThat(names("mv")).containsExactly("c", "b", "a");
    assertThat(version("mv")).isEqualTo(2);
    // Provenance reads each as the attribute it was, and it keeps what it had.
    assertThat(pids("mv")).containsExactlyInAnyOrderEntriesOf(before);
    assertThat(registry.logicalType("mv", registry.latest("mv")).getRootSchema().getField("b")
        .getDoc()).isEqualTo("bee");

    // Where it is already, nothing changes and no version is made.
    assertThat(registry.alter("mv", new MoveAttribute("c", Position.first()),
        new MoveAttribute("a", Position.after("b"))).edited()).isNull();
    assertThat(version("mv")).isEqualTo(2);
    // Nor do moves that undo each other.
    assertThat(registry.alter("mv", new MoveAttribute("a", Position.first()),
        new MoveAttribute("a", Position.after("b"))).edited()).isNull();
    assertThat(version("mv")).isEqualTo(2);
    assertThatThrownBy(() -> registry.alter("mv", new MoveAttribute("b", Position.after("b"))))
        .isInstanceOf(TypeRefusedException.class)
        .hasMessage("Attribute b cannot go after itself");
    assertThatThrownBy(() -> registry.alter("mv", new MoveAttribute("b", Position.after("z"))))
        .isInstanceOf(TypeRefusedException.class).hasMessage("Type mv has no attribute z");
    assertThatThrownBy(() -> registry.alter("mv", new MoveAttribute("z", Position.first())))
        .isInstanceOf(TypeRefusedException.class).hasMessage("Type mv has no attribute z");
  }

  // Avro names the records under the attributes in their order: first, a's record b would
  // take the name pa_a_b, which the record of a_b has.
  @Test
  void anAvroAttributeAddedFirstLeavesTheNamesOfTheOthersRecords() {
    create("pa", SchemaType.AVRO, "(a_b STRUCT(x STRING), s STRUCT(z INT))");
    AvroSchema before = (AvroSchema) registry.schema("pa");
    registry.alter("pa",
        new AddAttribute("a", "STRUCT(b STRUCT(y INT))", Position.first()));
    AvroSchema after = (AvroSchema) registry.schema("pa");
    assertThat(names("pa")).containsExactly("a", "a_b", "s");
    assertThat(after.isBackwardCompatible(before)).isEmpty();
    // The attributes there are written as they were, to the letter.
    for (String attribute : List.of("a_b", "s")) {
      assertThat(after.rawSchema().getField(attribute).schema().toString())
          .isEqualTo(before.rawSchema().getField(attribute).schema().toString());
    }
  }

  // The names are pinned again once an action has brought a record of its own: placed before
  // a's record b, a record added or given since does not take the name pa_a_b from it.
  @Test
  void anAvroRecordBroughtBetweenTwoMovesLeavesTheNamesOfTheOthers() {
    create("pm", SchemaType.AVRO, "(a STRUCT(b STRUCT(x INT)), c INT)");
    registry.alter("pm", new MoveAttribute("c", Position.first()),
        new AddAttribute("a_b", "STRUCT(y STRING)"), new MoveAttribute("a_b", Position.first()));
    create("pn", SchemaType.AVRO, "(a STRUCT(b STRUCT(x INT)), c INT, a_b INT)");
    registry.alter("pn", new MoveAttribute("c", Position.first()),
        new AlterAttributeType("a_b", "STRUCT(y STRING)"),
        new MoveAttribute("a_b", Position.first()));
    for (String type : List.of("pm", "pn")) {
      assertThat(names(type)).as(type).containsExactly("a_b", "c", "a");
      assertThat(registry.schema(type).canonicalString()).as(type)
          .contains("\"name\":\"" + type + "_a_b\",\"fields\":[{\"name\":\"x\"")
          .contains("\"name\":\"" + type + "_a_b_2\",\"fields\":[{\"name\":\"y\"");
    }
  }

  // The names are those of the statement so far, taken before an attribute is placed: one it
  // added or retyped before keeps the name it had there, as an attribute that is moved does.
  @Test
  void anAvroStatementThatAddsAndPlacesKeepsEachRecordsName() {
    String start = "(x_b STRUCT(q STRING), s INT)";
    List<AlterAction[]> statements = List.of(
        new AlterAction[] {new AddAttribute("x", "STRUCT(b STRUCT(y INT))"),
            new AddAttribute("z", "INT", Position.first())},
        new AlterAction[] {new AddAttribute("x", "STRUCT(b STRUCT(y INT))", Position.first()),
            new AddAttribute("y", "STRUCT(k INT)", Position.first())},
        new AlterAction[] {new AddAttribute("x", "STRUCT(b STRUCT(y INT))"),
            new MoveAttribute("x", Position.first())},
        new AlterAction[] {new AddAttribute("x", "STRUCT(b STRUCT(y INT))"),
            new MoveAttribute("x_b", Position.after("x"))});
    for (int i = 0; i < statements.size(); i++) {
      String type = "mx" + i;
      create(type, SchemaType.AVRO, start);
      AvroSchema before = (AvroSchema) registry.schema(type);
      registry.alter(type, statements.get(i));
      AvroSchema after = (AvroSchema) registry.schema(type);
      assertThat(after.isBackwardCompatible(before)).as("statement %d", i).isEmpty();
      assertThat(after.rawSchema().getField("x_b").schema().toString())
          .isEqualTo(before.rawSchema().getField("x_b").schema().toString());
    }
    // A retyped attribute's new member, too.
    create("mr", SchemaType.AVRO, "(a_b STRUCT(q STRING), a STRUCT(c INT), s INT)");
    AvroSchema before = (AvroSchema) registry.schema("mr");
    registry.alter("mr", new AlterAttributeType("a", "STRUCT(c INT, b STRUCT(y INT))"),
        new AddAttribute("z", "INT", Position.first()));
    assertThat(((AvroSchema) registry.schema("mr")).isBackwardCompatible(before)).isEmpty();
    assertThat(names("mr")).containsExactly("z", "a_b", "a", "s");
  }

  // The record a map's entries are written as has a name too: m's would take m_Entry's.
  @Test
  void anAvroMapPlacedFirstLeavesTheNamesOfTheOthersRecords() {
    create("me", SchemaType.AVRO,
        "(a INT NOT NULL, m_Entry STRUCT(q INT), m_value STRUCT(r INT))");
    AvroSchema before = (AvroSchema) registry.schema("me");
    registry.alter("me", new AddAttribute("m", "MAP<INT NOT NULL, STRUCT(x INT)>"),
        new MoveAttribute("m", Position.first()));
    AvroSchema after = (AvroSchema) registry.schema("me");
    assertThat(names("me")).containsExactly("m", "a", "m_Entry", "m_value");
    assertThat(after.isBackwardCompatible(before)).isEmpty();
    for (String attribute : List.of("m_Entry", "m_value")) {
      assertThat(after.rawSchema().getField(attribute).schema().toString())
          .isEqualTo(before.rawSchema().getField(attribute).schema().toString());
    }
  }

  // A CHECK is judged on the attributes the statement leaves, a position in it or not: the
  // names are read from the type as it stands, where the CHECK may not hold yet.
  @Test
  void anAvroCheckIsJudgedOnWhatAStatementWithAPositionLeaves() {
    Check later = new Check("ck", "z > 0", null);
    List<AlterAction[]> statements = List.of(
        new AlterAction[] {new AddConstraint(later), new MoveAttribute("b", Position.first()),
            new AddAttribute("z", "INT")},
        new AlterAction[] {new AddConstraint(later),
            new AddAttribute("y", "INT", Position.after("a")), new AddAttribute("z", "INT")},
        new AlterAction[] {new AddConstraint(new Check("ck", "bb > 0", null)),
            new MoveAttribute("a", Position.after("b")), new RenameAttribute("b", "bb")},
        // An attribute's own, which holds of the type it is given after.
        new AlterAction[] {new AddAttributeCheck("b", new Check("ck", "b > 0", null)),
            new AddAttribute("y", "INT", Position.first()),
            new AlterAttributeType("b", "INT")});
    for (int i = 0; i < statements.size(); i++) {
      String type = "pc" + i;
      create(type, SchemaType.AVRO, i < 3 ? "(a INT NOT NULL, b INT)"
          : "(a INT NOT NULL, b STRING)");
      registry.alter(type, statements.get(i));
      assertThat(version(type)).as("statement %d", i).isEqualTo(2);
      Schema struct = registry.struct(type);
      List<Rule> rules = new ArrayList<>(struct.getRules());
      struct.getFields().forEach(f -> rules.addAll(f.getRules()));
      assertThat(rules).as("statement %d", i).extracting(Rule::getName).containsExactly("ck");
    }
    // One that held of the type the attribute had, dropped after the attribute is retyped.
    create("pd", SchemaType.AVRO, "(a STRUCT(x INT), b INT, CONSTRAINT c1 CHECK (a.x > 0))");
    registry.alter("pd", new AlterAttributeType("a", "INT"),
        new AddAttribute("w", "INT", Position.first()), new DropConstraint("c1", false));
    assertThat(names("pd")).containsExactly("w", "a", "b");
    assertThat(registry.struct("pd").getRules()).isEmpty();
    // One that does not hold of what the statement leaves is refused as it was.
    create("pe", SchemaType.AVRO, "(a INT NOT NULL, b INT)");
    assertThatThrownBy(() -> registry.alter("pe", new AddConstraint(later),
        new MoveAttribute("b", Position.first()))).isInstanceOf(TypeRefusedException.class)
        .hasMessageContaining("Unknown column in CHECK: 'z'");
    assertThat(version("pe")).isEqualTo(1);
  }

  // A record whose name another type's begins with is read as a named type the root refers
  // to: its CHECKs are left out of the names' reading there as at the root.
  @Test
  void anAvroCheckIsJudgedAtTheEndOfAStatementOnARootReadAsANamedType() throws Exception {
    registry.client.register("nr", new AvroSchema("{\"type\": \"record\", \"name\": "
        + "\"Order\", \"fields\": [{\"name\": \"v\", \"type\": \"int\"}, "
        + "{\"name\": \"w\", \"type\": [\"null\", \"int\"], \"default\": null}, "
        + "{\"name\": \"s\", \"type\": {\"type\": \"record\", \"name\": \"Inner\", "
        + "\"namespace\": \"Order\", \"fields\": [{\"name\": \"x\", \"type\": \"int\"}]}}, "
        + "{\"name\": \"t\", \"type\": \"Order.Inner\"}]}"));
    assertThat(registry.logicalType("nr", registry.latest("nr")).getRootSchema().getType())
        .isEqualTo(Schema.Type.NAMED_TYPE_REF);
    registry.alter("nr", new AddConstraint(new Check("ck", "z > 0", null)),
        new MoveAttribute("w", Position.first()), new AddAttribute("z", "INT"));
    Schema order = registry.struct("nr");
    assertThat(order.getFields()).extracting(Schema.Field::getName)
        .containsExactly("w", "v", "s", "t", "z");
    assertThat(order.getRules()).extracting(Rule::getName).containsExactly("ck");
  }

  // A column's name compares whatever its case, in a position as elsewhere.
  @Test
  void aColumnIsPlacedAfterOneNamedInAnotherCase() {
    create("cs", SchemaType.AVRO, "(a INT NOT NULL, b STRING)");
    TypeAlter.Options columns = new TypeAlter.Options().columns(true);
    registry.alter("cs", columns, new AddAttribute("c", "INT", Position.after("A")),
        new MoveAttribute("B", Position.first()));
    assertThat(names("cs")).containsExactly("b", "a", "c");
  }

  // A move takes no number, so the type's earlier versions, read for the numbers they used,
  // are not read for a statement of moves alone.
  @Test
  void aProtobufStatementOfMovesAloneDoesNotReadTheTypesHistory() {
    create("ph", SchemaType.PROTOBUF, "(a INT NOT NULL, b STRING, c BIGINT)");
    registry.alter("ph", new AddAttribute("d", "INT"));
    registry.alter("ph", new AddAttribute("e", "INT"));
    List<Integer> read = new ArrayList<>();
    TypeLookup counting = new TypeLookup() {
      @Override
      public SchemaMetadata latest(String subject) {
        return registry.latest(subject);
      }

      @Override
      public SchemaMetadata version(String subject, int version) {
        read.add(version);
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
    };
    assertThat(TypeAlter.alter(counting, "ph", List.of(new MoveAttribute("c", Position.first()),
        new MoveAttribute("a", Position.after("b"))), new TypeAlter.Options()).edited())
        .isNotNull();
    assertThat(read).isEmpty();
    // A statement that may take a number reads them all.
    TypeAlter.alter(counting, "ph", List.of(new MoveAttribute("c", Position.first()),
        new AddAttribute("f", "INT")), new TypeAlter.Options());
    assertThat(read).contains(1, 2, 3);
  }

  // A Protobuf attribute is placed and moved as in the other formats, and is the field it
  // was: its number stays. A message declares its unions after its other fields, and is read
  // so: a position that would put a union before an attribute that is not one is refused.
  @Test
  void aProtobufAttributeIsPlacedAndKeepsItsNumber() {
    create("pp", SchemaType.PROTOBUF,
        "(a INT NOT NULL, b STRING, c BIGINT, u UNION(x INT, y STRING))");
    registry.alter("pp", new AddAttribute("d", "DOUBLE", Position.after("a")),
        new MoveAttribute("c", Position.first()));
    assertThat(names("pp")).containsExactly("c", "a", "d", "b", "u");
    assertThat(numbers(message("pp"))).contains("a=1", "b=2", "c=3", "x=4", "y=5", "d=6");
    // The text is normalized, its fields by number, and each says its place.
    String text = registry.schema("pp").canonicalString();
    assertThat(text.indexOf(" a = 1")).isBetween(0, text.indexOf(" c = 3"));
    assertThat(text).contains("key: \"connect.index\"");
    assertThat(registry.schema("pp").normalize().canonicalString()).isEqualTo(text);
    // A serializer looks the type up by a schema made of its descriptor, which is this text.
    assertThat(new ProtobufSchema(message("pp")).canonicalString()).isEqualTo(text);
    assertThat(LogicalTypeToDdlConverter.toDdl(registry.logicalType("pp", registry.latest("pp"))))
        .doesNotContain("connect.index");

    // With no position, an attribute that is no union goes after the last such one.
    registry.alter("pp", new AddAttribute("e", "INT"));
    assertThat(names("pp")).containsExactly("c", "a", "d", "b", "e", "u");
    // Unions are placed among themselves.
    registry.alter("pp", new AddAttribute("v", "UNION(p INT, q STRING)"),
        new MoveAttribute("v", Position.after("e")));
    assertThat(names("pp")).containsExactly("c", "a", "d", "b", "e", "v", "u");

    String unions = " in PROTOBUF type pp: a Protobuf message declares its unions after its "
        + "other fields, so a union follows every attribute that is not a union";
    assertThatThrownBy(() -> registry.alter("pp", new MoveAttribute("u", Position.first())))
        .isInstanceOf(TypeRefusedException.class)
        .hasMessage("Attribute u cannot be placed first" + unions);
    assertThatThrownBy(() -> registry.alter("pp",
        new AddAttribute("f", "INT", Position.after("u"))))
        .isInstanceOf(TypeRefusedException.class)
        .hasMessage("Attribute f cannot be placed after u" + unions);
    assertThatThrownBy(() -> registry.alter("pp", new MoveAttribute("z", Position.first())))
        .isInstanceOf(TypeRefusedException.class).hasMessage("Type pp has no attribute z");
    assertThat(version("pp")).isEqualTo(4);
  }

  // The options metadata is written in are declared by confluent/meta.proto. A file that has
  // them imports it, as protoc does not know them otherwise; one that has none does not.
  @Test
  void aProtobufFileThatHasMetadataImportsTheFileThatDeclaresIt() throws Exception {
    String imports = "import \"confluent/meta.proto\";";
    create("mp", SchemaType.PROTOBUF, "(a INT, b BIGINT)");
    assertThat(registry.schema("mp").canonicalString()).doesNotContain(imports);
    registry.alter("mp", new MoveAttribute("b", Position.first()));
    assertThat(registry.schema("mp").canonicalString()).contains(imports);

    List<String> bodies = List.of("(a INT COMMENT 'the a', b BIGINT)", "(a SMALLINT, b BIGINT)",
        "(a INT, s STRUCT(k INT COMMENT 'the k'))", "(a INT, u UNION(x INT, a STRING))",
        "(a INT, e ENUM('A' COMMENT 'the first', 'B'))");
    for (int i = 0; i < bodies.size(); i++) {
      String type = "mi" + i;
      create(type, SchemaType.PROTOBUF, bodies.get(i));
      String text = registry.schema(type).canonicalString();
      assertThat(text).as(type).contains(imports);
      // A schema made of the descriptor, as a serializer's is, has it too.
      assertThat(new ProtobufSchema(message(type)).canonicalString()).as(type).isEqualTo(text);
    }
    // A type stored by a writer that did not import it is the same type: a statement that
    // changes nothing makes no version of it, and one that changes it writes the import.
    String without = registry.schema("mi0").canonicalString().replace(imports + "\n", "")
        .replace("mi0", "mo");
    assertThat(without).doesNotContain(imports);
    registry.client.register("mo", new ProtobufSchema(without).copy(
        registry.latest("mi0").getMetadata(), null));
    assertThat(registry.alter("mo", new AddAttribute("q", "INT"),
        new DropAttribute("q", false)).edited()).isNull();
    registry.alter("mo", new AddAttribute("c", "INT"));
    assertThat(registry.schema("mo").canonicalString()).contains(imports);

    // Metadata of an enum, of a value of it, of a message, and of a message that is not the
    // first of several.
    for (String script : List.of(
        "ENUM Color ('RED', 'BLUE') COMMENT 'the colors'; STRUCT t (a INT NOT NULL, c Color); "
            + "TYPE t;",
        "ENUM Color ('RED' COMMENT 'r', 'BLUE'); STRUCT t (a INT NOT NULL, c Color); TYPE t;",
        "STRUCT t (a INT NOT NULL, b BIGINT NOT NULL) COMMENT 'the type';")) {
      assertThat(written(script)).as(script).contains(imports);
    }
    assertThat(LogicalTypeToProtoConverter.fromLogicalType(TypeScripts.parse(
        "STRUCT A (a INT NOT NULL); STRUCT B (k INT NOT NULL COMMENT 'the k');", true), "t",
        LogicalTypeVersion.V2).canonicalString()).doesNotContain("file_meta").contains(imports);
    // A comment that says nothing is no metadata, and is not written.
    assertThat(written("STRUCT t (a INT NOT NULL COMMENT '', b BIGINT NOT NULL) COMMENT '';"))
        .doesNotContain(imports).doesNotContain("meta");
    // The file declares messages of its own: a type of one of their names in its package
    // would be defined twice, and is refused where the file is imported.
    assertThatThrownBy(() -> written("NAMESPACE confluent; "
        + "STRUCT Rule (a INT COMMENT 'the a', b BIGINT);"))
        .isInstanceOf(ValidationException.class).hasMessage("Protobuf name 'confluent.Rule' is "
            + "defined by confluent/meta.proto, a file of the Protobuf format's own; give the "
            + "type another name or namespace");
    assertThat(written("NAMESPACE confluent; STRUCT Rule (a INT, b BIGINT);"))
        .doesNotContain(imports);
    assertThat(written("NAMESPACE acme; STRUCT Rule (a INT COMMENT 'the a', b BIGINT);"))
        .contains(imports);
    // So is an enum, a message that is not the first, one where the file is imported for
    // its own option alone, and one of the name of an option the file declares.
    for (String script : List.of(
        "NAMESPACE confluent; ENUM Meta ('A', 'B'); STRUCT t (a INT COMMENT 'c', m Meta); "
            + "TYPE t;",
        "NAMESPACE confluent; STRUCT t (a INT NOT NULL COMMENT 'c', m Meta NOT NULL); "
            + "STRUCT Meta (a INT NOT NULL); TYPE t;",
        "NAMESPACE confluent; STRUCT Rule (a INT NOT NULL, b BIGINT NOT NULL); "
            + "STRUCT Inner (a INT NOT NULL); TYPE Rule;",
        "NAMESPACE confluent; STRUCT field_meta (a INT COMMENT 'c');")) {
      assertThatThrownBy(() -> written(script)).as(script)
          .isInstanceOf(ValidationException.class)
          .hasMessageContaining("is defined by confluent/meta.proto");
    }
    // And a type of the name of one a DECIMAL or a TIMESTAMP is written as, where the file
    // has one; a wrapper's, which is read by its name, wherever.
    assertThatThrownBy(() -> written("NAMESPACE confluent.type; "
        + "STRUCT `Decimal` (a INT NOT NULL, d DECIMAL(10, 2) NOT NULL);"))
        .isInstanceOf(ValidationException.class)
        .hasMessageContaining("'confluent.type.Decimal' is defined by confluent/type/decimal");
    assertThatThrownBy(() -> written("NAMESPACE google.protobuf; "
        + "STRUCT `Timestamp` (a INT NOT NULL, t TIMESTAMP(3));"))
        .isInstanceOf(ValidationException.class)
        .hasMessageContaining("'google.protobuf.Timestamp' is defined by");
    assertThatThrownBy(() -> written("NAMESPACE google.protobuf; "
        + "STRUCT StringValue (a INT NOT NULL, b BIGINT NOT NULL);"))
        .isInstanceOf(ValidationException.class)
        .hasMessageContaining("'google.protobuf.StringValue' is defined by");
    // As the unsigned wrappers, which no root is written as and which are read all the same.
    for (String wrapper : List.of("UInt32Value", "UInt64Value")) {
      assertThatThrownBy(() -> written("NAMESPACE google.protobuf; STRUCT " + wrapper
          + " (a INT NOT NULL, b BIGINT NOT NULL);")).as(wrapper)
          .isInstanceOf(ValidationException.class)
          .hasMessageContaining("is defined by google/protobuf/wrappers.proto");
      assertThatThrownBy(() -> written("NAMESPACE google.protobuf; STRUCT " + wrapper
          + " (a INT NOT NULL); STRUCT t (id INT, d " + wrapper + "); TYPE t;")).as(wrapper)
          .isInstanceOf(ValidationException.class).hasMessageContaining("is defined by");
    }
    assertThat(written("NAMESPACE acme; STRUCT `Decimal` (d DECIMAL(10, 2) NOT NULL);"))
        .contains("message Decimal");
    // A field of a type of the name of one read by its name is read as that one's, whether
    // the file has a DECIMAL or a DATE or none: the type is refused where a field is of it.
    for (String script : List.of(
        "NAMESPACE confluent.type; STRUCT `Decimal` (a INT, b STRING); "
            + "STRUCT t (id INT, d `Decimal`); TYPE t;",
        "NAMESPACE google.type; STRUCT `Date` (a INT NOT NULL); "
            + "STRUCT t (id INT, d ARRAY<`Date`>); TYPE t;",
        "NAMESPACE google.type; STRUCT TimeOfDay (a INT NOT NULL); "
            + "STRUCT t (id INT, d TimeOfDay NOT NULL); TYPE t;",
        "NAMESPACE google.protobuf; STRUCT `Timestamp` (a INT NOT NULL); "
            + "STRUCT t (id INT, d MAP<STRING, `Timestamp`>); TYPE t;")) {
      assertThatThrownBy(() -> written(script)).as(script)
          .isInstanceOf(ValidationException.class).hasMessageContaining("is defined by");
    }
    // One no field is of is a message as any other, as one registered by hand is.
    for (String script : List.of(
        "NAMESPACE confluent.type; STRUCT `Decimal` (a INT, b STRING);",
        "NAMESPACE google.type; STRUCT `Date` (a INT NOT NULL);",
        "NAMESPACE google.type; STRUCT TimeOfDay (a INT NOT NULL);",
        "NAMESPACE google.protobuf; STRUCT `Timestamp` (a INT NOT NULL);")) {
      assertThat(written(script)).as(script).contains("message ");
    }
    assertThat(written("NAMESPACE confluent.type; STRUCT Other (d DECIMAL(10, 2) NOT NULL);"))
        .contains("message Other");
    assertThat(written("NAMESPACE google.protobuf; STRUCT Other (a INT NOT NULL);"))
        .contains("message Other");
    // A type whose root is a scalar is a wrapper, a field of which is read as the scalar:
    // a type that refers to it would be read as one that does not, and is refused. And V1
    // is written whatever the names, as Flink writes it.
    registry.create("pss", SchemaType.PROTOBUF, BodyKind.OTHER, "STRING");
    create("pr", SchemaType.PROTOBUF, "(id INT, c STRING)");
    String wrapped = "Type pss cannot be used in a PROTOBUF type: it is a value and no row, "
        + "written as a google.protobuf.StringValue, and an attribute of it would be read as "
        + "that value and not as the type. Give the attribute the value's type";
    assertThatThrownBy(() -> create("ps", SchemaType.PROTOBUF, "(id INT, s pss)"))
        .isInstanceOf(TypeRefusedException.class).hasMessage(wrapped);
    assertThatThrownBy(() -> registry.alter("pr", new AddAttribute("s", "pss")))
        .isInstanceOf(TypeRefusedException.class).hasMessage(wrapped);
    assertThatThrownBy(() -> registry.alter("pr", new AlterAttributeType("c", "ARRAY<pss>")))
        .isInstanceOf(TypeRefusedException.class).hasMessage(wrapped);
    assertThat(registry.latest("ps")).isNull();
    assertThat(version("pr")).isEqualTo(1);
    // A copy of one of the format's own files under a reference's name is that file, and a
    // type refers to it; one with a DATE of its own would import the two, and is refused.
    registry.client.register("vdd", new ProtobufSchema(Date.getDescriptor()));
    create("vda", SchemaType.PROTOBUF, "(id INT, d vdd)");
    registry.alter("vda", new AddAttribute("z", "INT"));
    assertThat(version("vda")).isEqualTo(2);
    assertThatThrownBy(() -> create("vdb", SchemaType.PROTOBUF, "(id INT, d vdd, own DATE)"))
        .isInstanceOf(RuntimeException.class);
    // The refusal tells of the copy, and not of a name the statement gave no type.
    LogicalType both = TypeReferences.parse(registry, SchemaType.PROTOBUF,
        aliases -> TypeScripts.createScript("vdb", BodyKind.STRUCT, "(id INT, d vdd, own DATE)",
          Map.of(), aliases), "vdb", Map.of());
    assertThatThrownBy(() -> ColumnIds.nativeOf("vdb", SchemaType.PROTOBUF, both, "Refused"))
        .isInstanceOf(TypeRefusedException.class).hasMessage("Refused: no PROTOBUF schema can "
            + "be made of it: Protobuf file 'vdd', which the type refers to, is a copy of "
            + "google/type/date.proto, a file of the Protobuf format's own that the type "
            + "imports as well for an attribute of its own: their names would be defined "
            + "twice. Give the attribute that refers to the copy the format's type instead");
    assertThat(registry.latest("vdb")).isNull();
    assertThat(LogicalTypeToProtoConverter.fromLogicalType(TypeScripts.parse(
        "NAMESPACE google.protobuf; STRUCT StringValue (a INT NOT NULL, b BIGINT NOT NULL);"),
        "t", LogicalTypeVersion.V1).canonicalString()).contains("message StringValue");
    // So is one a type refers to, which would be read as the DECIMAL it is not.
    registry.client.register("dn", new ProtobufSchema("syntax = \"proto3\";\n\n"
        + "package confluent.type;\n\nmessage Decimal {\n  int32 a = 1;\n  string b = 2;\n}\n"));
    assertThatThrownBy(() -> create("du", SchemaType.PROTOBUF, "(id INT, d dn, z STRING)"))
        .isInstanceOf(RuntimeException.class);
    assertThat(registry.latest("du")).isNull();
    // One registered by hand that refers to it is read as having a DECIMAL, and would be
    // written with the DECIMAL's own message: it is not altered while it has the field.
    registry.client.register("dh", new ProtobufSchema("syntax = \"proto3\";\n\n"
        + "import \"dn\";\n\nmessage Dh {\n  int32 id = 1;\n  confluent.type.Decimal d = 2;\n"
        + "}\n", List.of(new SchemaReference("dn", "dn", 1)),
        Map.of("dn", registry.latest("dn").getSchema()), null, null));
    assertThatThrownBy(() -> registry.alter("dh", new AddAttribute("z", "INT")))
        .isInstanceOf(TypeRefusedException.class).hasMessage("Type dh cannot be altered: "
            + "field d of message Dh is a confluent.type.Decimal that is not the one "
            + "confluent/type/decimal.proto declares, which it would be written as, so its "
            + "stored values would be read wrongly. Drop the attribute, or register its new "
            + "version as a Protobuf schema");
    registry.alter("dh", new DropAttribute("d", false));
    assertThat(version("dh")).isEqualTo(2);
    // The one registered is itself altered as any other: its root is read as a row.
    registry.alter("dn", new AddAttribute("c", "INT"));
    assertThat(version("dn")).isEqualTo(2);
    // One a type refers to is this file's to import as well.
    registry.create("ra", "confluent.Rule", SchemaType.PROTOBUF, BodyKind.STRUCT,
        "(a INT, b BIGINT)");
    create("rc", SchemaType.PROTOBUF, "(id INT, r ra)");
    assertThatThrownBy(() -> registry.alter("rc", new SetAttributeComment("id", "the id")))
        .isInstanceOf(TypeRefusedException.class)
        .hasMessageContaining("Protobuf name 'confluent.Rule' is defined by");
    // And one that a type it refers to refers to.
    create("rd", SchemaType.PROTOBUF, "(k INT, y rc)");
    assertThatThrownBy(() -> registry.alter("rd", new SetAttributeComment("k", "the k")))
        .isInstanceOf(TypeRefusedException.class)
        .hasMessageContaining("Protobuf name 'confluent.Rule' is defined by");

    // A comment of no words is none: it is not written, and saying one makes no version.
    assertThat(written("ENUM Color ('RED', 'BLUE') COMMENT ''; STRUCT t (a INT NOT NULL "
        + "COMMENT '  ', c Color NOT NULL, e ENUM('A' COMMENT ' ', 'B') NOT NULL) COMMENT ' '; "
        + "TYPE t;")).doesNotContain(imports).doesNotContain("doc");
    assertThat(registry.alter("mp", new SetAttributeComment("a", "  ")).edited()).isNull();

    // Another import than the metadata's is the type's own to tell: with one more, a
    // statement that changes nothing makes a version.
    registry.client.register("mu", new ProtobufSchema(registry.schema("mi0").canonicalString()
        .replace(imports, imports + "\nimport \"google/protobuf/timestamp.proto\";")
        .replace("mi0", "mu")).copy(registry.latest("mi0").getMetadata(), null));
    assertThat(registry.alter("mu", new AddAttribute("q", "INT"),
        new DropAttribute("q", false)).edited()).isNotNull();

    // V1 is written as Flink writes it, which imports none.
    assertThat(LogicalTypeToProtoConverter.fromLogicalType(TypeScripts.parse(
        "STRUCT t (a SMALLINT, d DECIMAL(10, 2));"), "t", LogicalTypeVersion.V1)
        .canonicalString()).doesNotContain(imports);
  }

  private static String written(String script) {
    return LogicalTypeToProtoConverter.fromLogicalType(TypeScripts.parse(script), "t",
        LogicalTypeVersion.V2).canonicalString();
  }

  // The Protobuf reader takes a param under one of its own names for how a type is encoded
  // or where it is. A statement that gives one is refused; the writer writes none.
  @Test
  void aProtobufParamUnderAReadersNameIsNotGiven() throws Exception {
    String kept = " cannot be given in a PROTOBUF type: the format keeps the names flink.*, "
        + "connect.*, logical.*, precision and scale for how it encodes a type";
    String[][] bodies = {
        {"(a INT WITH ('connect.type' = 'int8'))", "Property connect.type of attribute a"},
        {"(d DECIMAL(10, 2) WITH ('precision' = '4'))", "Property precision of attribute d"},
        {"(s STRING WITH ('flink.maxLength' = '3'))", "Property flink.maxLength of attribute s"},
        {"(a INT WITH ('connect.index' = '1'), b INT)", "Property connect.index of attribute a"},
        {"(r STRUCT(k INT WITH ('flink.notNull' = 'true')))",
            "Property flink.notNull of attribute k"},
        {"(u UNION(x INT WITH ('logical.branch' = 'z'), y STRING))",
            "Property logical.branch of branch x"},
        {"(e ENUM('A' WITH ('connect.x' = '1'), 'B'))", "Property connect.x of value A"}};
    for (int i = 0; i < bodies.length; i++) {
      String type = "rp" + i;
      String body = bodies[i][0];
      assertThatThrownBy(() -> create(type, SchemaType.PROTOBUF, body)).as(body)
          .isInstanceOf(TypeRefusedException.class).hasMessage(bodies[i][1] + kept);
    }
    // One on a type the statement declares is told by that type's name.
    assertThatThrownBy(() -> Formats.refuseUnwritable(SchemaType.PROTOBUF, TypeScripts.parse(
        "STRUCT Addr (s STRING) WITH ('connect.x' = '1'); STRUCT t (a Addr); TYPE t;")))
        .isInstanceOf(TypeRefusedException.class)
        .hasMessage("Property connect.x of type Addr" + kept);
    // One on the type is told by the type's name.
    assertThatThrownBy(() -> create("rt", SchemaType.PROTOBUF,
        "(a INT) WITH ('connect.name' = 'q')")).isInstanceOf(TypeRefusedException.class)
        .hasMessage("Property connect.name of type rt" + kept);
    create("rq", SchemaType.PROTOBUF, "(a INT, b STRING)");
    assertThatThrownBy(() -> registry.alter("rq",
        new SetAttributeWith("a", Map.of("connect.type", "int8"))))
        .isInstanceOf(TypeRefusedException.class)
        .hasMessage("Property connect.type of attribute a" + kept);
    assertThatThrownBy(() -> registry.alter("rq",
        new AddAttribute("c", "INT WITH ('connect.type' = 'int8')")))
        .isInstanceOf(TypeRefusedException.class).hasMessageContaining("Property connect.type");
    assertThatThrownBy(() -> registry.alter("rq", new SetWith(Map.of("logical.named", "x"))))
        .isInstanceOf(TypeRefusedException.class)
        .hasMessage("Property logical.named of type rq" + kept);
    assertThat(version("rq")).isEqualTo(1);
    // A param of another name is the type's own, and the other formats keep any.
    registry.alter("rq", new SetAttributeWith("a", Map.of("owner", "x")));
    assertThat(version("rq")).isEqualTo(2);
    create("ra", SchemaType.AVRO, "(a INT WITH ('connect.type' = 'int8'))");
    assertThat(registry.struct("ra").getField("a").getParams()).containsKey("connect.type");

    // A type that comes by one otherwise, as one made of another format's, is written
    // without it: it is read as it was said, in the order it was said in.
    LogicalType read = LogicalTypeConversion.toLogicalType(
        LogicalTypeToProtoConverter.fromLogicalType(TypeScripts.parse(
            "STRUCT t (a INT WITH ('connect.type' = 'int8', 'connect.index' = '2'), "
                + "d DECIMAL(10, 2) WITH ('precision' = '4', 'connect.index' = '0'), "
                + "u UNION(x INT WITH ('logical.branch' = 'z', 'connect.index' = '9'), "
                + "y STRING WITH ('connect.index' = '0')));"), "t", LogicalTypeVersion.V2));
    String ddl = LogicalTypeToDdlConverter.toDdl(read, true);
    assertThat(ddl).contains("(a INT, d DECIMAL(10, 2), u UNION(x INT, y STRING))");
    // An enum's value and a type alike.
    assertThat(written("STRUCT t (e ENUM('A' WITH ('connect.x' = '1', 'k' = 'v'), 'B'));"))
        .contains("\"k\"").doesNotContain("connect.x");
    assertThat(written("STRUCT t (a INT) WITH ('connect.x' = '1', 'k' = 'v');"))
        .contains("\"k\"").doesNotContain("connect.x");
    // And one a file gives a value of an enum is not told as the value's, as one it gives
    // a field is not.
    registry.client.register("re", new ProtobufSchema("syntax = \"proto3\";\n\n"
        + "import \"confluent/meta.proto\";\n\nmessage R {\n  E e = 1;\n\n  enum E {\n"
        + "    A = 0 [(confluent.enum_value_meta) = { params: [ { key: \"connect.x\", "
        + "value: \"1\" }, { key: \"owner\", value: \"x\" }, { key: "
        + "\"protobuf.enum.number\", value: \"9\" }, { key: \"avro.x\", value: \"y\" } ] }];\n"
        + "    B = 1;\n  }\n}\n"));
    assertThat(LogicalTypeToDdlConverter.toDdl(registry.logicalType("re",
        registry.latest("re")), true)).contains("'owner' = 'x'").doesNotContain("connect.x")
        .doesNotContain("avro.x").doesNotContain("'9'");
  }

  // The place of a field is the writer's to tell, wherever a message's fields, its oneofs or
  // a oneof's members are not in the order of their numbers, and only there.
  @Test
  void aProtobufFieldsPlaceIsToldWhereverTheOrderIsNotOfTheNumbers() throws Exception {
    // Only a union's members out of that order: they are read as said, and stay the fields
    // they were.
    create("im", SchemaType.PROTOBUF, "(a INT, u UNION(y STRING WITH "
        + "('protobuf.field.number' = '3'), x INT WITH ('protobuf.field.number' = '2')))");
    assertThat(branches("im", "u")).containsExactly("y", "x");
    registry.alter("im", new AddAttribute("z", "INT"));
    assertThat(branches("im", "u")).containsExactly("y", "x");
    assertThat(numbers(message("im")).split(" ")).contains("x=2", "y=3");

    // A message beside the root, declared out of that order, keeps its order.
    registry.client.register("ir", new ProtobufSchema("syntax = \"proto3\";\n\n"
        + "import \"confluent/meta.proto\";\n\noption (confluent.file_meta) = { params: [{ "
        + "key: \"logical.root\", value: \"Cust\" }] };\n\nmessage Cust {\n"
        + "  string id = 1;\n  Addr addr = 2;\n}\nmessage Addr {\n  string zip = 2;\n"
        + "  string street = 1;\n}\n"));
    registry.alter("ir", new AddAttribute("z", "INT"));
    assertThat(registry.logicalType("ir", registry.latest("ir")).getNamedTypes().get("Addr")
        .getFields()).extracting(Schema.Field::getName).containsExactly("zip", "street");

    // A place that is no number is none: its field follows those that have one.
    String place = " [(confluent.field_meta) = { params: [{ key: \"connect.index\", value: "
        + "\"%s\" }] }]";
    registry.client.register("ix", new ProtobufSchema("syntax = \"proto3\";\n\n"
        + "import \"confluent/meta.proto\";\n\nmessage R {\n  int32 a = 1"
        + String.format(place, "x") + ";\n  int32 b = 2" + String.format(place, "1")
        + ";\n  int32 c = 3" + String.format(place, "0") + ";\n}\n"));
    assertThat(names("ix")).containsExactly("c", "b", "a");

    // The writer's marker stands beside a place only where the field has no other param.
    create("ik", SchemaType.PROTOBUF, "(a INT WITH ('k' = 'v', 'protobuf.field.number' = '2'), "
        + "c INT WITH ('k' = 'v', 'l' = 'w', 'protobuf.field.number' = '3'), "
        + "b INT WITH ('protobuf.field.number' = '1'))");
    String stored = registry.schema("ik").canonicalString();
    assertThat(stored.substring(stored.indexOf(" a = 2"))).contains("connect.index")
        .doesNotContain("flink.version");
    assertThat(stored.substring(0, stored.indexOf(" a = 2"))).contains("flink.version");

    // V1 tells none.
    assertThat(LogicalTypeToProtoConverter.fromLogicalType(TypeScripts.parse(
        "STRUCT t (b INT WITH ('protobuf.field.number' = '2'), "
            + "a INT WITH ('protobuf.field.number' = '1'));"), "t", LogicalTypeVersion.V1)
        .canonicalString()).doesNotContain("connect.index");
  }

  private List<String> branches(String subject, String union) {
    return registry.struct(subject).getField(union).getSchema().getBranches().stream()
        .map(Schema.UnionBranch::getName).collect(Collectors.toList());
  }

  // Each attribute a statement adds is placed among those before it as they then stand:
  // one with no position after the last that is no union, and the next after that one.
  @Test
  void aProtobufAttributeIsPlacedAfterOneTheStatementAdds() {
    create("pg", SchemaType.PROTOBUF, "(a INT, u UNION(x INT, y STRING))");
    registry.alter("pg", new AddAttribute("e", "INT"),
        new AddAttribute("f", "INT", Position.after("e")));
    assertThat(names("pg")).containsExactly("a", "e", "f", "u");
    // And one given a union's type goes among the unions, for the others to be placed by.
    create("ph3", SchemaType.PROTOBUF, "(a INT, b INT, c INT)");
    registry.alter("ph3", new AlterAttributeType("a", "UNION(x INT, y STRING)"),
        new MoveAttribute("b", Position.after("c")));
    assertThat(names("ph3")).containsExactly("c", "b", "a");
  }

  // The order a message declares its fields in is the order of the type's attributes, and
  // is kept: a statement writes them by number, as normalized, and tells the place of each.
  @Test
  void aProtobufMessageDeclaredOutOfNumberOrderKeepsItsOrder() throws Exception {
    registry.client.register("po", new ProtobufSchema("syntax = \"proto3\";\n\n"
        + "message R {\n  int64 a = 2;\n  int64 b = 1;\n}\n"));
    registry.alter("po", new AddAttribute("x", "INT"));
    assertThat(names("po")).containsExactly("a", "b", "x");
    String text = registry.schema("po").canonicalString();
    assertThat(text.indexOf(" b = 1")).isBetween(0, text.indexOf(" a = 2"));
    assertThat(numbers(message("po"))).isEqualTo("b=1 a=2 x=3");

    // A type created with its numbers in another order has its attributes as it says them.
    create("pc", SchemaType.PROTOBUF, "(a BIGINT WITH ('protobuf.field.number' = '2'), "
        + "b BIGINT WITH ('protobuf.field.number' = '1'))");
    assertThat(names("pc")).containsExactly("a", "b");
    assertThat(numbers(message("pc"))).isEqualTo("b=1 a=2");

    // Normalized once more, as a registry does when asked, each is the type it was.
    for (String subject : List.of("po", "pc")) {
      registry.client.register(subject + "n", registry.schema(subject).normalize());
      assertThat(names(subject + "n")).isEqualTo(names(subject));
      assertThat(numbers(message(subject + "n"))).isEqualTo(numbers(message(subject)));
    }
    // A field with no place, as one added to the text by hand, follows those that have one.
    registry.client.register("ph2", new ProtobufSchema(registry.schema("pc").canonicalString()
        .replaceFirst("\n\\}\\s*$", "\n  int32 c = 9;\n  int32 d = 3;\n}\n")));
    assertThat(names("ph2")).containsExactly("a", "b", "c", "d");

    // Where the order is that of the numbers, nothing is told.
    create("pd", SchemaType.PROTOBUF, "(a INT NOT NULL, u UNION(x INT, y STRING))");
    registry.alter("pd", new AddAttribute("b", "INT"));
    assertThat(names("pd")).containsExactly("a", "b", "u");
    assertThat(registry.schema("pd").canonicalString()).doesNotContain("connect.index");
    registry.alter("pd", new MoveAttribute("b", Position.first()));
    assertThat(names("pd")).containsExactly("b", "a", "u");
    assertThat(registry.schema("pd").canonicalString()).contains("connect.index");
    registry.alter("pd", new MoveAttribute("a", Position.first()), new AddAttribute("c", "INT"));
    assertThat(names("pd")).containsExactly("a", "b", "c", "u");
    assertThat(registry.schema("pd").canonicalString()).doesNotContain("connect.index");
  }

  // Messages are named in the order of the attributes: first, fooBar would take the name
  // FooBarRow, which foo_bar's message has.
  @Test
  void aProtobufAttributeAddedFirstLeavesTheNamesOfTheOthersMessages() {
    create("pf", SchemaType.PROTOBUF, "(id INT NOT NULL, foo_bar STRUCT(x STRING))");
    ProtobufSchema before = (ProtobufSchema) registry.schema("pf");
    registry.alter("pf", new AddAttribute("fooBar", "STRUCT(y INT)", Position.first()));
    Descriptor after = message("pf");
    assertThat(after.findFieldByName("foo_bar").getMessageType().getName())
        .isEqualTo("FooBarRow");
    assertThat(after.findFieldByName("fooBar").getMessageType().getName())
        .isEqualTo("FooBar2Row");
    assertThat(((ProtobufSchema) registry.schema("pf")).isBackwardCompatible(before)).isEmpty();
  }

  // A message's fields share their names with the messages it declares: an attribute of such
  // a name would rename the message, or leave no schema to write.
  @Test
  void anAttributeNamedAsAMessageOfItsProtobufTypeIsRefused() throws Exception {
    create("pn", SchemaType.PROTOBUF, "(id INT NOT NULL, foo STRUCT(x STRING), bar INT)");
    ProtobufSchema before = (ProtobufSchema) registry.schema("pn");
    assertThatThrownBy(() -> registry.alter("pn", new AddAttribute("FooRow", "INT")))
        .isInstanceOf(TypeRefusedException.class).hasMessage("Type pn cannot be changed so: "
            + "no PROTOBUF schema can be made of it: Protobuf name 'FooRow' is defined twice "
            + "in pn, as a field and a message (used by fields foo); give the field another "
            + "name");
    // The message keeps its name when its attribute is renamed, so it is the new one's to
    // have another.
    assertThatThrownBy(() -> registry.alter("pn", new RenameAttribute("foo", "baz"),
        new AddAttribute("FooRow", "INT"))).isInstanceOf(TypeRefusedException.class)
        .hasMessageEndingWith("(used by fields baz); give the field another name");
    // However the name comes to be taken: by a rename, a oneof's member, or a new attribute
    // beside the row of an attribute that is now an array of that row.
    for (AlterAction[] taking : List.of(
        new AlterAction[] {new RenameAttribute("id", "FooRow")},
        new AlterAction[] {new AddAttribute("u", "UNION(FooRow INT, c STRING)")},
        new AlterAction[] {new AddAttribute("FooRow", "UNION(a INT, b STRING)")})) {
      assertThatThrownBy(() -> registry.alter("pn", taking))
          .isInstanceOf(TypeRefusedException.class)
          .hasMessageContaining("'FooRow' is defined twice in pn");
    }
    // An attribute given another kind has its types named anew: its row, now an array's,
    // would be renamed for the new attribute to have its name.
    assertThatThrownBy(() -> registry.alter("pn",
        new AlterAttributeType("foo", "ARRAY<STRUCT(x STRING) NOT NULL> NOT NULL"),
        new AddAttribute("FooRow", "INT"))).isInstanceOf(TypeRefusedException.class)
        .hasMessageContaining("FooRow, the type of foo in Protobuf message pn, would be "
            + "renamed Foo2Row");
    // A renamed attribute's message keeps its name by a pin, and so has no other to take.
    assertThatThrownBy(() -> registry.alter("pn", new RenameAttribute("foo", "baz"),
        new AddAttribute("foo", "INT"), new DropAttribute("foo", false),
        new AddAttribute("FooRow", "INT"))).isInstanceOf(TypeRefusedException.class)
        .hasMessageContaining("'FooRow' is defined twice");
    // One a pin keeps as Flink named it has no other name to take: no file can be written.
    registry.client.register("pm", new ProtobufSchema("syntax = \"proto3\";\n"
        + "message Record {\n"
        + "  optional addr_Row addr = 1;\n"
        + "  message addr_Row { optional string city = 1; }\n"
        + "}\n"));
    assertThatThrownBy(() -> registry.alter("pm", new AddAttribute("addr_Row", "INT")))
        .isInstanceOf(TypeRefusedException.class).hasMessageContaining("no PROTOBUF schema "
            + "can be made of it: Protobuf name 'addr_Row' is defined twice in Record");
    assertThat(version("pn")).isEqualTo(1);
    assertThat(version("pm")).isEqualTo(1);

    // The name is free once no field is of the message: its attribute dropped or no longer a
    // row, in whatever order the statement says it, or a oneof's member gone.
    for (AlterAction[] freeing : List.of(
        new AlterAction[] {new DropAttribute("foo", false), new AddAttribute("FooRow", "INT")},
        new AlterAction[] {new AddAttribute("FooRow", "INT"), new DropAttribute("foo", false)},
        new AlterAction[] {new AlterAttributeType("foo", "INT"),
            new AddAttribute("FooRow", "INT")})) {
      registry.client.register("pf", before);
      registry.alter("pf", freeing);
      ProtobufSchema after = (ProtobufSchema) registry.schema("pf");
      assertThat(numbers(after.toDescriptor())).contains("FooRow=");
      assertThat(after.toDescriptor().getNestedTypes()).isEmpty();
      registry.client.deleteSubject("pf");
      registry.client.deleteSubject("pf", true);
    }
    create("po", SchemaType.PROTOBUF,
        "(id INT NOT NULL, u UNION(card STRUCT(n STRING), iban STRING))");
    String card = message("po").findFieldByName("card").getMessageType().getName();
    registry.alter("po", new AlterAttributeType("u", "UNION(iban STRING, bic STRING)"),
        new AddAttribute(card, "INT"));
    assertThat(numbers(message("po"))).contains(card + "=");
  }

  // A union's member is a field of the message, named for its branch, or after its union too
  // when a field before it has that name: which, turns on an order the stored type is not
  // read in. A member keeps the name it is stored with, and a new one is named around it.
  @Test
  void aProtobufUnionMemberKeepsItsFieldName() {
    // {the type, the statement that gives two members one branch name, the fields after it}
    Object[][] cases = {
        {"(id INT NOT NULL, k INT)", new AlterAction[] {new AddAttribute("u", "UNION(d INT)"),
            new AlterAttributeType("k", "UNION(d STRING)")}, "u_d=4 d=5 id=1"},
        {"(id INT NOT NULL, k INT, m INT)", new AlterAction[] {
            new AlterAttributeType("m", "UNION(d INT)"),
            new AlterAttributeType("k", "UNION(d STRING)")}, "m_d=4 d=5 id=1"},
        {"(id INT NOT NULL, k INT, t UNION(a INT, b STRING))", new AlterAction[] {
            new AlterAttributeType("k", "UNION(a STRING, c INT)")}, "a=3 b=4 k_a=5 c=6 id=1"},
        {"(id INT NOT NULL, u UNION(x INT), v UNION(a INT))", new AlterAction[] {
            new AlterAttributeType("u", "UNION(x INT, a STRING)")}, "x=2 u_a=4 a=3 id=1"},
        // Created with its unions' numbers in another order than their attributes.
        {"(id INT NOT NULL, u UNION(d INT WITH('protobuf.field.number'='5')), "
            + "v UNION(d STRING WITH('protobuf.field.number'='2')))", new AlterAction[0],
            "v_d=2 d=5 id=1"}};
    for (int i = 0; i < cases.length; i++) {
      String type = "um" + i;
      create(type, SchemaType.PROTOBUF, (String) cases[i][0]);
      AlterAction[] statement = (AlterAction[]) cases[i][1];
      if (statement.length > 0) {
        registry.alter(type, statement);
      }
      String[] stored = ((String) cases[i][2]).split(" ");
      assertThat(numbers(message(type)).split(" ")).as(type)
          .containsExactlyInAnyOrder(stored);
      // Whatever a later statement says, they are the fields they were.
      int before = version(type);
      registry.alter(type, new AddAttribute("zz", "INT"));
      assertThat(version(type)).as(type).isEqualTo(before + 1);
      assertThat(numbers(message(type)).split(" ")).as(type).contains(stored);
      assertThat(registry.alter(type, new AddAttribute("q", "INT"),
          new DropAttribute("q", false)).edited()).as(type).isNull();
    }

    // Below the root too.
    create("un", SchemaType.PROTOBUF,
        "(id INT NOT NULL, s STRUCT(u UNION(a INT), v UNION(b INT)))");
    registry.alter("un", new AlterAttributeType("s",
        "STRUCT(v UNION(b INT, d STRING), u UNION(a INT, d INT))"));
    Descriptor row = message("un").findFieldByName("s").getMessageType();
    assertThat(numbers(row).split(" ")).containsExactlyInAnyOrder("b=2", "d=3", "a=1", "u_d=4");
    assertThat(registry.logicalType("un", registry.latest("un")).getRootSchema().getField("s")
        .getSchema().getFields()).extracting(Schema.Field::getName).containsExactly("v", "u");
    registry.alter("un", new AddAttribute("zz", "INT"));
    assertThat(numbers(message("un").findFieldByName("s").getMessageType()))
        .isEqualTo(numbers(row));

    // A member there keeps its name and its number when the field of its name goes, and
    // when its own type is of another kind.
    create("uw", SchemaType.PROTOBUF,
        "(id INT NOT NULL, s STRUCT(a INT, u UNION(a INT, b STRING)))");
    assertThat(numbers(message("uw").findFieldByName("s").getMessageType()))
        .isEqualTo("u_a=2 b=3 a=1");
    for (String type : List.of("STRUCT(u UNION(a INT, b STRING, c INT))",
        "STRUCT(u UNION(a ENUM('X', 'Y'), b STRING, c INT))")) {
      registry.alter("uw", new AlterAttributeType("s", type));
      assertThat(numbers(message("uw").findFieldByName("s").getMessageType())).as(type)
          .isEqualTo("u_a=2 b=3 c=4");
    }

    // A type in a namespace is one the root refers to: its members are kept the same.
    registry.create("ns", "com.acme.ns", SchemaType.PROTOBUF, BodyKind.STRUCT,
        "(id INT NOT NULL, k INT)");
    registry.alter("ns", new AddAttribute("u", "UNION(d INT)"),
        new AlterAttributeType("k", "UNION(d STRING)"));
    String stored = numbers(message("ns"));
    registry.alter("ns", new AddAttribute("zz", "INT"));
    assertThat(numbers(message("ns"))).startsWith(stored);

    // A branch given another kind is the field it was, and its type is named anew: an enum
    // is not named as the row was, and takes the name given for it.
    create("ub", SchemaType.PROTOBUF, "(id INT NOT NULL, u UNION(r STRUCT(k INT), s STRING))");
    String rowName = message("ub").findFieldByName("r").getMessageType().getName();
    registry.alter("ub", new AlterAttributeType("u", "UNION(r ENUM('X', 'Y'), s STRING)"));
    assertThat(numbers(message("ub"))).contains("r=2", "s=3");
    assertThat(message("ub").findFieldByName("r").getEnumType().getName())
        .isNotEqualTo(rowName);
    create("uc", SchemaType.PROTOBUF, "(id INT NOT NULL, u UNION(r STRUCT(k INT), s STRING))");
    registry.alter("uc", new AlterAttributeType("u",
        "UNION(r ENUM('X', 'Y') WITH ('protobuf.name' = 'Kind'), s STRING)"));
    assertThat(message("uc").findFieldByName("r").getEnumType().getName()).isEqualTo("Kind");

    // The name is the member's: no attribute takes it, and its union renamed leaves it.
    create("ut", SchemaType.PROTOBUF, "(id INT NOT NULL, a INT, t UNION(a STRING, b STRING))");
    assertThatThrownBy(() -> registry.alter("ut", new AddAttribute("b", "INT")))
        .isInstanceOf(TypeRefusedException.class).hasMessage("Type ut cannot be changed so: no "
            + "PROTOBUF schema can be made of it: Union branch 'b' of 't' in ut keeps its field "
            + "name 'b', which is another field's name");
    assertThatThrownBy(() -> registry.alter("ut", new RenameAttribute("t", "b")))
        .isInstanceOf(TypeRefusedException.class).hasMessageEndingWith("Union branch 'b' of "
            + "'b' in ut keeps its field name 'b', which is its union's name");
    // Refused as such whatever follows it in the statement.
    assertThatThrownBy(() -> registry.alter("ut", new AddAttribute("b", "INT"),
        new RenameAttribute("id", "idx"))).isInstanceOf(TypeRefusedException.class)
        .hasMessageContaining("keeps its field name 'b'");
    registry.alter("ut", new RenameAttribute("t", "tt"), new DropAttribute("a", false));
    assertThat(numbers(message("ut"))).isEqualTo("t_a=3 b=4 id=1");
    // Its union said again with another member, it is the member it was.
    registry.alter("ut", new AlterAttributeType("tt", "UNION(a STRING, b STRING, c INT)"));
    assertThat(numbers(message("ut"))).contains("t_a=3", "b=4", "c=5");
    assertThatThrownBy(() -> registry.alter("ut", new AddAttribute("t_a", "INT")))
        .isInstanceOf(TypeRefusedException.class).hasMessageContaining("keeps its field name");
    // A member dropped leaves its name to an attribute.
    registry.alter("ut", new AlterAttributeType("tt", "UNION(b STRING)"),
        new AddAttribute("t_a", "INT"));
    assertThat(numbers(message("ut"))).isEqualTo("b=4 id=1 t_a=6");
  }

  // A map has no place for the numbers of a key or value that is no union: an entry that
  // numbers them otherwise than in turn cannot be written again as it is.
  @Test
  void aProtobufMapEntryNumberedOtherwiseIsNotAltered() throws Exception {
    String entry = "syntax = \"proto3\";\n\nmessage R {\n  int32 id = 1;\n"
        + "  repeated MEntry m = 2;\n\n  message MEntry {\n    %s\n    %s\n  }\n}\n";
    registry.client.register("rv", new ProtobufSchema(
        String.format(entry, "int32 value = 1;", "int32 key = 2;")));
    assertThat(registry.struct("rv").getField("m").getSchema().getType())
        .isEqualTo(Schema.Type.MAP);
    assertThatThrownBy(() -> registry.alter("rv", new AddAttribute("z", "INT")))
        .isInstanceOf(TypeRefusedException.class).hasMessage("Type rv cannot be altered: "
            + "field key of map entry MEntry has the number 2, which would be written as 1, "
            + "so its stored entries would be read wrongly. Drop the attribute, or register "
            + "its new version as a Protobuf schema");
    assertThat(version("rv")).isEqualTo(1);
    // A statement that drops the map writes no such entry.
    registry.alter("rv", new DropAttribute("m", false), new AddAttribute("z", "INT"));
    assertThat(numbers(message("rv"))).isEqualTo("id=1 z=3");
    // The value as the key.
    registry.client.register("rw", new ProtobufSchema(
        String.format(entry, "int32 key = 1;", "int32 value = 3;")));
    assertThatThrownBy(() -> registry.alter("rw", new AddAttribute("z", "INT")))
        .isInstanceOf(TypeRefusedException.class).hasMessageContaining("field value of map "
            + "entry MEntry has the number 3, which would be written as 2");
    // One a nested message's field has, and one another file declares.
    registry.client.register("rn", new ProtobufSchema("syntax = \"proto3\";\n\nmessage R {\n"
        + "  int32 id = 1;\n  Inner i = 2;\n\n  message Inner {\n    repeated FooEntry a = 1;\n\n"
        + "    message FooEntry {\n      string key = 2;\n      int32 value = 1;\n    }\n  }\n}\n"));
    assertThatThrownBy(() -> registry.alter("rn", new AddAttribute("z", "INT")))
        .isInstanceOf(TypeRefusedException.class).hasMessageContaining("map entry FooEntry");
    String pair = "syntax = \"proto3\";\n\npackage o;\n\nmessage PairEntry {\n"
        + "  string key = 2;\n  int32 value = 1;\n}\n";
    registry.client.register("ent", new ProtobufSchema(pair));
    registry.client.register("ri", new ProtobufSchema("syntax = \"proto3\";\n\n"
        + "import \"ent\";\n\nmessage R {\n  int32 id = 1;\n  repeated o.PairEntry a = 2;\n}\n",
        List.of(new SchemaReference("ent", "ent", 1)), Map.of("ent", pair), null, null));
    assertThatThrownBy(() -> registry.alter("ri", new AddAttribute("z", "INT")))
        .isInstanceOf(TypeRefusedException.class).hasMessageContaining("map entry PairEntry");
    // A repeated message of a key and a value that is not named as an entry is a row's.
    registry.client.register("rr", new ProtobufSchema(String.format(entry,
        "int32 value = 1;", "int32 key = 2;").replace("MEntry", "Item")));
    registry.alter("rr", new AddAttribute("z", "INT"));
    assertThat(version("rr")).isEqualTo(2);
    // Numbered in turn, it is altered.
    registry.client.register("ok", new ProtobufSchema(
        String.format(entry, "int32 key = 1;", "int32 value = 2;")));
    registry.alter("ok", new AddAttribute("z", "INT"));
    assertThat(version("ok")).isEqualTo(2);
    // A message of an entry's shape that no repeated field has is a row, and is altered: a
    // row has its fields' numbers.
    registry.client.register("re", new ProtobufSchema("syntax = \"proto3\";\n\n"
        + "message REntry {\n  int32 value = 1;\n  int32 key = 2;\n}\n"));
    registry.client.register("rs", new ProtobufSchema(String.format(entry,
        "int32 value = 1;", "int32 key = 2;").replace("repeated MEntry", "MEntry")));
    assertThat(registry.struct("rs").getField("m").getSchema().getType())
        .isEqualTo(Schema.Type.STRUCT);
    for (String type : List.of("re", "rs")) {
      registry.alter(type, new AddAttribute("z", "INT"));
      assertThat(version(type)).as(type).isEqualTo(2);
    }
    assertThat(numbers(message("re")).split(" "))
        .containsExactlyInAnyOrder("value=1", "key=2", "z=3");
    assertThat(numbers(message("rs").findFieldByName("m").getMessageType()).split(" "))
        .containsExactlyInAnyOrder("value=1", "key=2");
  }

  // A map's entry is a message of its key and value, a union's members among them. Whatever
  // order they were numbered in, a later statement leaves the numbers its records are read by.
  @Test
  void aProtobufMapEntryKeepsItsNumbers() throws Exception {
    List<String> types = List.of("MAP<UNION(d INT, e INT), STRING>",
        "MAP<STRING, UNION(d INT, e INT)>", "MAP<UNION(d INT, e INT), UNION(f INT, g STRING)>",
        "MULTISET<UNION(d INT, e INT)>", "ARRAY<MAP<UNION(d INT), STRUCT(x INT)>>");
    for (int i = 0; i < types.size(); i++) {
      // Created so, and an attribute given the type after.
      for (boolean retyped : new boolean[] {false, true}) {
        String type = "me" + i + retyped;
        create(type, SchemaType.PROTOBUF, "(id INT NOT NULL, u "
            + (retyped ? "STRUCT(k INT)" : types.get(i)) + ")");
        if (retyped) {
          registry.alter(type, new AlterAttributeType("u", types.get(i)));
        }
        String entry = numbers(nested(message(type), "ValueEntry"));
        registry.alter(type, new SetAttributeComment("id", "the key"));
        registry.alter(type, new AddAttribute("zz", "INT"));
        assertThat(numbers(nested(message(type), "ValueEntry"))).as(type).isEqualTo(entry);
      }
    }
    // An entry written by hand with its value's members numbered before its key's.
    registry.client.register("mh", new ProtobufSchema("syntax = \"proto3\";\n\nmessage R {\n"
        + "  int32 id = 1;\n  repeated NEntry n = 2;\n\n  message NEntry {\n"
        + "    oneof value {\n      int32 f = 1;\n      string g = 2;\n    }\n"
        + "    oneof key {\n      int32 d = 3;\n      int32 e = 4;\n    }\n  }\n}\n"));
    registry.alter("mh", new AddAttribute("zz", "INT"));
    assertThat(numbers(nested(message("mh"), "NEntry")).split(" "))
        .containsExactlyInAnyOrder("f=1", "g=2", "d=3", "e=4");
    // Read for display, a type created so is as it was: its numbers told only where the
    // writer would not give them.
    assertThat(LogicalTypeToDdlConverter.toDdl(registry.logicalType("me1false",
        registry.latest("me1false")), true)).doesNotContain("protobuf.field.number");
  }

  // A name is pinned on the sort of type the writer generates: a collection that may be null
  // is wrapped, one that may not is its entries or elements. Given the other nullability, it
  // is named as a type created so is.
  @Test
  void aProtobufCollectionGivenAnotherNullabilityIsNamedAnew() {
    String[][] cases = {
        {"MAP<STRING, INT>", "MAP<STRING, INT> NOT NULL"},
        {"MULTISET<STRING>", "MULTISET<STRING> NOT NULL"},
        {"ARRAY<INT>", "ARRAY<INT> NOT NULL"},
        {"ARRAY<STRUCT(k INT)>", "ARRAY<STRUCT(k INT) NOT NULL> NOT NULL"},
        {"ARRAY<STRUCT(k INT)> NOT NULL", "ARRAY<STRUCT(k INT) NOT NULL> NOT NULL"},
        {"STRUCT(k INT, m MAP<STRING, INT>)", "STRUCT(k INT, m MAP<STRING, INT> NOT NULL)"}};
    for (int i = 0; i < cases.length; i++) {
      for (int from = 0; from < 2; from++) {
        String type = "cn" + i + from;
        String fresh = "cf" + i + from;
        create(type, SchemaType.PROTOBUF, "(id INT NOT NULL, a " + cases[i][from] + ", z INT)");
        registry.alter(type, new AlterAttributeType("a", cases[i][1 - from]));
        create(fresh, SchemaType.PROTOBUF,
            "(id INT NOT NULL, a " + cases[i][1 - from] + ", z INT)");
        assertThat(typeNames(message(type))).as(type).isEqualTo(typeNames(message(fresh)));
        // And a later statement finds it so.
        registry.alter(type, new AddAttribute("zz", "INT"));
        assertThat(version(type)).as(type).isEqualTo(3);
        assertThat(registry.alter(type, new AddAttribute("q", "INT"),
            new DropAttribute("q", false)).edited()).as(type).isNull();
      }
    }
  }

  // A union's member may be null whatever is said of it, and its collection is wrapped so:
  // said NOT NULL, it is the type it was, by the names it had under the union's old name.
  @Test
  void aProtobufUnionMemberSaidNotNullIsTheTypeItWas() {
    String[][] cases = {
        {"UNION(p INT, q MAP<STRING, INT>)", "UNION(p INT, q MAP<STRING, INT> NOT NULL)"},
        {"UNION(p INT, q ARRAY<STRUCT(k INT)>)", "UNION(p INT, q ARRAY<STRUCT(k INT)> NOT NULL)"},
        {"UNION(p INT, q MULTISET<STRING> NOT NULL)", "UNION(p INT, q MULTISET<STRING>)"}};
    for (int i = 0; i < cases.length; i++) {
      String type = "ub" + i;
      create(type, SchemaType.PROTOBUF, "(id BIGINT NOT NULL, a " + cases[i][0] + ")");
      registry.alter(type, new RenameAttribute("a", "b"));
      List<String> names = typeNames(message(type));
      assertThat(registry.alter(type, new AlterAttributeType("b", cases[i][1])).edited())
          .as(type).isNull();
      // With another change beside it, the version keeps the names.
      registry.alter(type, new AlterAttributeType("b", cases[i][1]),
          new AddAttribute("zz", "INT"));
      assertThat(version(type)).as(type).isEqualTo(3);
      assertThat(typeNames(message(type))).as(type).isEqualTo(names);
    }
    // A member that is a row is not marked as one that may not be null: read, it may be.
    create("um", SchemaType.PROTOBUF,
        "(id BIGINT NOT NULL, a UNION(x STRUCT(k INT) NOT NULL, y STRING))");
    assertThat(registry.schema("um").canonicalString()).doesNotContain("flink.notNull");
    assertThat(registry.alter("um", new AddAttribute("q", "INT"),
        new DropAttribute("q", false)).edited()).isNull();

    // Whatever its type, it is written as one that may be null, with what it has beside.
    for (String member : List.of("DECIMAL(10, 2)", "TIMESTAMP(3)", "STRUCT(k INT)", "DATE")) {
      String said = "un" + member.charAt(0) + member.length();
      create(said, SchemaType.PROTOBUF,
          "(id INT NOT NULL, u UNION(x " + member + " NOT NULL, y STRING))");
      create(said + "p", SchemaType.PROTOBUF,
          "(id INT NOT NULL, u UNION(x " + member + ", y STRING))");
      assertThat(registry.schema(said).canonicalString()).as(member).isEqualTo(
          registry.schema(said + "p").canonicalString().replace(said + "p", said));
    }
    create("uc", SchemaType.PROTOBUF,
        "(id INT NOT NULL, u UNION(x STRUCT(k INT) NOT NULL COMMENT 'mc', y STRING))");
    assertThat(registry.schema("uc").canonicalString()).contains("doc: \"mc\"");
    // A renamed union's array of another element nullability is named for the union it is of.
    create("uk", SchemaType.PROTOBUF, "(id INT NOT NULL, u UNION(p INT, q ARRAY<INT NOT NULL>))");
    registry.alter("uk", new RenameAttribute("u", "w"));
    assertThat(registry.schema("uk").canonicalString()).contains("UQRepeatedWrapper");
    registry.alter("uk", new AlterAttributeType("w", "UNION(p INT, q ARRAY<INT>)"));
    assertThat(registry.schema("uk").canonicalString()).contains("WQRepeatedWrapper")
        .doesNotContain("UQRepeatedWrapper");

    // An element that may now be null is wrapped, and is named anew.
    create("ue", SchemaType.PROTOBUF,
        "(id BIGINT NOT NULL, a UNION(p INT, q ARRAY<STRUCT(k INT) NOT NULL>))");
    registry.alter("ue", new AlterAttributeType("a", "UNION(p INT, q ARRAY<STRUCT(k INT)>)"));
    create("uf", SchemaType.PROTOBUF,
        "(id BIGINT NOT NULL, a UNION(p INT, q ARRAY<STRUCT(k INT)>))");
    assertThat(typeNames(message("ue"))).isEqualTo(typeNames(message("uf")));
  }

  // In every format a member said NOT NULL is written as any other member, and a statement
  // that changes nothing makes no version of it: null is the union's to allow.
  @ParameterizedTest
  @EnumSource(SchemaType.class)
  void aUnionMemberSaidNotNullIsWrittenAsAnyOther(SchemaType format) {
    List<String> members = List.of("INT", "STRUCT(k INT)", "ARRAY<INT>", "MAP<STRING, INT>",
        "ENUM('A', 'B')");
    for (int i = 0; i < members.size(); i++) {
      String type = "mn" + i;
      create(type, format, "(id INT NOT NULL, u UNION(x " + members.get(i)
          + " NOT NULL, y BOOLEAN))");
      assertThat(registry.alter(type, new AddAttribute("q", "INT"),
          new DropAttribute("q", false)).edited()).as(members.get(i)).isNull();
      assertThat(registry.alter(type, new AlterAttributeType("u", "UNION(x " + members.get(i)
          + ", y BOOLEAN)")).edited()).as(members.get(i)).isNull();
    }
  }

  // A branch that is a name is written as the type it is of: a value of that type that
  // another branch is written as would match both, whether the name is of a type the
  // statement declares or of one it refers to.
  @Test
  void aJsonUnionsBranchThatIsANameIsWrittenAsItsType() {
    assertThatThrownBy(() -> Formats.refuseUnwritable(SchemaType.JSON, TypeScripts.parse(
        "ENUM Color ('R', 'B'); STRUCT t (u UNION(a STRING, c Color)); TYPE t;")))
        .isInstanceOf(TypeRefusedException.class)
        .hasMessageContaining("cannot have branches a and c");
    registry.create("je", SchemaType.JSON, BodyKind.ENUM, "('A', 'B')");
    registry.create("jk", SchemaType.JSON, BodyKind.STRUCT, "(k INT)");
    assertThatThrownBy(() -> create("jv", SchemaType.JSON, "(u UNION(a STRING, b je))"))
        .isInstanceOf(TypeRefusedException.class)
        .hasMessageContaining("cannot have branches a and b");
    // And one a statement adds, or gives an attribute.
    create("jw", SchemaType.JSON, "(id INT NOT NULL, w INT)");
    assertThatThrownBy(() -> registry.alter("jw",
        new AddAttribute("u", "UNION(a STRING, b je)")))
        .isInstanceOf(TypeRefusedException.class)
        .hasMessageContaining("cannot have branches a and b");
    assertThatThrownBy(() -> registry.alter("jw",
        new AlterAttributeType("w", "UNION(a VARCHAR(3), b UNION(c BOOLEAN, d je))")))
        .isInstanceOf(TypeRefusedException.class)
        .hasMessageContaining("cannot have branches a and b");
    assertThat(version("jw")).isEqualTo(1);
    // One of another JSON type is a branch as any other; the other formats take them all.
    registry.alter("jw", new AddAttribute("u", "UNION(a INT, b je, c jk)"));
    registry.create("ae", SchemaType.AVRO, BodyKind.ENUM, "('A', 'B')");
    create("ax", SchemaType.AVRO, "(u UNION(a STRING, b ae))");
  }

  // Two fields of one message have one type: given another alone, one of them would need a
  // message of the name the other's has, and the way out is told.
  @Test
  void aProtobufMessageTwoFieldsShareIsRetypedForBoth() throws Exception {
    registry.client.register("sh", new ProtobufSchema("syntax = \"proto3\";\n\nmessage R {\n"
        + "  int32 a = 4;\n  N n1 = 9;\n  N n2 = 2;\n\n  message N {\n    int32 k = 7;\n"
        + "    string t = 3;\n  }\n}\n"));
    String type = "STRUCT(k INT NOT NULL, t STRING NOT NULL, u INT)";
    assertThatThrownBy(() -> registry.alter("sh", new AlterAttributeType("n1", type)))
        .isInstanceOf(TypeRefusedException.class)
        .hasMessageContaining("Protobuf name 'N' is defined twice in R")
        .hasMessageContaining("or where they share the message, give them its new type in "
            + "one statement");
    registry.alter("sh", new AlterAttributeType("n1", type),
        new AlterAttributeType("n2", type));
    assertThat(version("sh")).isEqualTo(2);
  }

  // A statement that would have a value of one row read as another's is refused: the rows
  // stored under the one would be read as the other's.
  @Test
  void aJsonUnionsRowIsNotGivenTheAttributesOfAnother() {
    create("jf", SchemaType.JSON,
        "(id BIGINT NOT NULL, u UNION(r1 STRUCT(a INT), r2 STRUCT(b STRING)))");
    assertThatThrownBy(() -> registry.alter("jf", new AlterAttributeType("u",
        "UNION(r1 STRUCT(a INT, b STRING), r2 STRUCT(b STRING))")))
        .isInstanceOf(TypeRefusedException.class).hasMessage("A UNION of attribute u in a "
            + "JSON type cannot have branches r1 and r2, which are written as one JSON type: "
            + "a value of either would match both");
    assertThat(version("jf")).isEqualTo(1);
    // A row that is a name is the row it is of.
    registry.create("jg", SchemaType.JSON, BodyKind.STRUCT, "(a INT)");
    assertThatThrownBy(() -> registry.alter("jf",
        new AddAttribute("v", "UNION(x jg, y STRUCT(a INT, b STRING))")))
        .isInstanceOf(TypeRefusedException.class)
        .hasMessageContaining("cannot have branches x and y");
    // Rows neither of which has all the other's attributes are told apart.
    registry.alter("jf", new AlterAttributeType("u",
        "UNION(r1 STRUCT(a INT, c INT), r2 STRUCT(b STRING))"));
    assertThat(version("jf")).isEqualTo(2);
  }

  // A branch that is a name is of the version of its type the reference is to. A statement
  // that moves the reference to a later one gives the branch that version's attributes: where
  // a value could then be both branches', it is refused, as giving the branch them would be.
  @Test
  void aJsonUnionsNamedBranchIsNotMovedToAVersionAnotherBranchIsReadAs() {
    registry.create("w1r", SchemaType.JSON, BodyKind.STRUCT, "(a INT)");
    create("w1u", SchemaType.JSON, "(id INT, u UNION(x w1r, y STRUCT(b STRING)))");
    registry.alter("w1r", new AddAttribute("b", "STRING"));
    assertThatThrownBy(() -> registry.alter("w1u", new AddAttribute("z", "w1r")))
        .isInstanceOf(TypeRefusedException.class).hasMessage("Type w1u cannot be changed so: "
            + "its reference would move, type w1r from version 1 to version 2, and with that "
            + "version's attributes branches x and y of the UNION of attribute u would be "
            + "written as one JSON type: a value of either would match both");
    assertThat(version("w1u")).isEqualTo(1);
    // A statement that moves no reference leaves the union as it is.
    registry.alter("w1u", new AddAttribute("q", "INT"));
    assertThat(version("w1u")).isEqualTo(2);
    assertThat(registry.latest("w1u").getReferences()).extracting(SchemaReference::getVersion)
        .containsExactly(1);

    // A name of a name: a branch is what the type its type refers to is.
    registry.create("nu1", SchemaType.JSON, BodyKind.OTHER, "UNION(c STRUCT(a INT), d BOOLEAN)");
    registry.create("nu2", SchemaType.JSON, BodyKind.OTHER, "UNION(e nu1, f STRING)");
    assertThatThrownBy(() -> create("nu3", SchemaType.JSON, "(u UNION(x BOOLEAN, y nu2))"))
        .isInstanceOf(TypeRefusedException.class)
        .hasMessageContaining("cannot have branches x and y");
    create("nu4", SchemaType.JSON, "(u UNION(x INT, y nu2))");
  }

  // A name of a document the type refers to whole is of that document, though another it
  // refers to has a definition of that name.
  @Test
  void aJsonReferenceIsNotTakenForAnotherDocumentsDefinitionOfItsName() throws Exception {
    registry.client.register("d1p", new JsonSchema("{\"type\":\"object\",\"title\":\"d1p\","
        + "\"properties\":{\"k\":{\"type\":\"integer\"}},\"additionalProperties\":false,"
        + "\"$defs\":{\"d1in\":{\"type\":\"object\",\"properties\":{\"a\":"
        + "{\"type\":\"integer\"}},\"required\":[\"a\"],\"additionalProperties\":false}}}"));
    registry.create("d1in", SchemaType.JSON, BodyKind.STRUCT, "(a STRING NOT NULL)");
    create("d1w", SchemaType.JSON, "(id INT, i d1in)");
    registry.alter("d1w", new AddAttribute("z", "d1p", Position.after("id")));
    assertThat(registry.schema("d1w").canonicalString()).contains("\"$ref\":\"d1in\"")
        .doesNotContain("d1p#/$defs/d1in");
    assertThat(registry.struct("d1w").getField("i").getSchema().getType())
        .isEqualTo(Schema.Type.NAMED_TYPE_REF);
  }

  // The names in a type referred to are its own: a type whose root is a name, or a union
  // with one, is what that name is of there, whatever the type that refers to it calls its
  // own references.
  @Test
  void aJsonUnionsBranchIsJudgedByTheNamesOfTheTypeItIsOf() {
    registry.create("je", SchemaType.JSON, BodyKind.ENUM, "('A', 'B')");
    registry.create("jk", SchemaType.JSON, BodyKind.STRUCT, "(k INT)");
    registry.create("jalias", SchemaType.JSON, BodyKind.OTHER, "je");
    registry.create("jkalias", SchemaType.JSON, BodyKind.OTHER, "jk");
    registry.create("jdom", SchemaType.JSON, BodyKind.OTHER, "UNION(p je, q BOOLEAN)");
    for (String union : List.of("UNION(a STRING, b jalias)",
        "UNION(a STRUCT(k INT), b jkalias)", "UNION(a STRING, b jdom)",
        "UNION(a BOOLEAN, b jdom)")) {
      assertThatThrownBy(() -> create("jc", SchemaType.JSON, "(u " + union + ")")).as(union)
          .isInstanceOf(TypeRefusedException.class)
          .hasMessageContaining("cannot have branches a and b");
    }
    // A type that refers to the row has the enum's alias beside a row, and not beside a
    // string; one that refers to the enum, the other way.
    create("jw1", SchemaType.JSON, "(id INT NOT NULL, e jk)");
    registry.alter("jw1", new AddAttribute("u", "UNION(a STRUCT(k INT), b jalias)"));
    assertThatThrownBy(() -> registry.alter("jw1",
        new AddAttribute("u2", "UNION(a STRING, b jalias)")))
        .isInstanceOf(TypeRefusedException.class)
        .hasMessageContaining("cannot have branches a and b");
    create("jw2", SchemaType.JSON, "(id INT NOT NULL, e je)");
    registry.alter("jw2", new AddAttribute("u", "UNION(a STRING, b jkalias)"));
    assertThatThrownBy(() -> registry.alter("jw2",
        new AddAttribute("u2", "UNION(a STRUCT(k INT), b jkalias)")))
        .isInstanceOf(TypeRefusedException.class)
        .hasMessageContaining("cannot have branches a and b");
  }

  // The types references are of are read only for a union with a name in it: a statement
  // that has none reads no type it refers to.
  @Test
  void aJsonStatementWithNoNamedBranchReadsNoReferredType() {
    registry.create("je", SchemaType.JSON, BodyKind.ENUM, "('A', 'B')");
    registry.create("jk", SchemaType.JSON, BodyKind.STRUCT, "(k INT)");
    create("jq", SchemaType.JSON, "(id INT NOT NULL, e je, r jk, u UNION(a INT, b jk))");
    List<String> read = new ArrayList<>();
    TypeLookup counting = new TypeLookup() {
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
        read.add(subject);
        return registry.logicalType(subject, version);
      }

      @Override
      public Config config(String subject) {
        return registry.config(subject);
      }
    };
    TypeAlter.alter(counting, "jq", List.of(new AddAttribute("t", "INT"),
        new AddAttribute("v", "UNION(a INT, b STRING)"),
        new AlterAttributeType("id", "BIGINT NOT NULL")), new TypeAlter.Options());
    assertThat(read).doesNotContain("je", "jk");
    // One with a named branch reads them, once for the statement.
    read.clear();
    TypeAlter.alter(counting, "jq", List.of(new AddAttribute("w", "UNION(a INT, b je)"),
        new AddAttribute("x", "UNION(a BOOLEAN, b jk)")), new TypeAlter.Options());
    assertThat(read).containsOnlyOnce("je").containsOnlyOnce("jk");
    // A type that others refer to is read once, by however many ways it is come to.
    registry.create("jd1", SchemaType.JSON, BodyKind.STRUCT, "(k INT)");
    registry.create("jd2", SchemaType.JSON, BodyKind.STRUCT, "(r jd1)");
    registry.create("jd3", SchemaType.JSON, BodyKind.STRUCT, "(p jd1, q jd2)");
    read.clear();
    TypeAlter.alter(counting, "jq", List.of(new AddAttribute("y", "UNION(a INT, b jd3)"),
        new AddAttribute("y2", "UNION(a INT, b jd2)")), new TypeAlter.Options());
    assertThat(read).containsOnlyOnce("jd1").containsOnlyOnce("jd2").containsOnlyOnce("jd3");
    // A type made with no union reads none, nor one whose reference moves.
    create("jc1", SchemaType.JSON, "(id INT, e je)");
    read.clear();
    TypeCreate.create(counting, "jc2", "jc2", BodyKind.STRUCT, "(id INT, e je, k jk)", "",
        Map.of(TypeAlter.FORMAT, "JSON"), null);
    TypeAlter.alter(counting, "jc1", List.of(new AddAttribute("d", "jd1")),
        new TypeAlter.Options());
    registry.alter("je", new AddValue("C", null, null));
    TypeAlter.alter(counting, "jc1", List.of(new AddAttribute("e2", "je")),
        new TypeAlter.Options());
    assertThat(read).doesNotContain("je", "jk", "jd1");
  }

  private void jsonRefused(String subject, String body) {
    assertThatThrownBy(() -> create(subject, SchemaType.JSON, body)).as(body)
        .isInstanceOf(TypeRefusedException.class).hasMessageContaining("cannot have branches");
    assertThat(registry.latest(subject)).isNull();
  }

  private void movedRefused(String subject, AlterAction... actions) {
    assertThatThrownBy(() -> registry.alter(subject, actions)).as(subject)
        .isInstanceOf(TypeRefusedException.class)
        .hasMessageContaining("its reference would move");
  }

  // A JSON type as the writer writes a script, registered as it is, with its references.
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

  // A reference's subject, and the name the type referred to gives itself, are no names of
  // the type's: one that is another reference's name is that reference's, in whichever
  // order the two are named.
  @Test
  void aJsonReferencesNameIsNotAnotherReferredTypesOwn() throws Exception {
    registry.create("Zed", SchemaType.JSON, BodyKind.ENUM, "('X', 'Y')");
    registry.client.register("aaa", new JsonSchema("{\"type\":\"object\",\"title\":\"Zed\","
        + "\"properties\":{\"k\":{\"type\":\"integer\"}},\"additionalProperties\":false}"));
    jsonRefused("o0", "(u UNION(a STRING, b Zed), z aaa)");
    jsonRefused("o1", "(z aaa, u UNION(a STRING, b Zed))");
    create("o2", SchemaType.JSON, "(u UNION(a STRUCT(k BIGINT), b Zed), z aaa)");
    create("o3", SchemaType.JSON, "(z aaa, u UNION(a STRUCT(k BIGINT), b Zed))");
    // Two referred types of one own name: the name is the one's whose reference it is.
    registry.create("Color", SchemaType.JSON, BodyKind.ENUM, "('X', 'Y')");
    registry.client.register("other", new JsonSchema("{\"type\":\"object\","
        + "\"title\":\"Color\",\"properties\":{\"k\":{\"type\":\"integer\"}},"
        + "\"additionalProperties\":false}"));
    jsonRefused("o4", "(u UNION(a STRING, b Color), z other)");
    // A reference whose subject is another reference's name.
    create("bbb", SchemaType.JSON, "(k BIGINT)");
    Map<String, String> resolved = new LinkedHashMap<>();
    resolved.put("x.json", registry.latest("Zed").getSchema());
    resolved.put("Zed", registry.latest("bbb").getSchema());
    registry.client.register("hs", new JsonSchema("{\"type\":\"object\",\"title\":\"hs\","
        + "\"properties\":{\"e\":{\"$ref\":\"x.json\"},\"r\":{\"$ref\":\"Zed\"}},"
        + "\"additionalProperties\":false}", List.of(new SchemaReference("x.json", "Zed", 1),
        new SchemaReference("Zed", "bbb", 1)), resolved, null));
    assertThatThrownBy(() -> registry.alter("hs",
        new AddAttribute("u", "UNION(a STRUCT(k BIGINT), b bbb)")))
        .isInstanceOf(TypeRefusedException.class).hasMessageContaining("cannot have branches");
  }

  // Each row's names are its own type's: two rows that name types of one name are judged
  // by what each one's is, in a context as out of one.
  @Test
  void aJsonUnionsRowsAreJudgedEachByItsOwnTypesNames() {
    for (String ctx : List.of("", ":.ctx:")) {
      registry.create(ctx + "e1", SchemaType.JSON, BodyKind.ENUM, "('A')");
      registry.create(ctx + "e2", SchemaType.JSON, BodyKind.ENUM, "('B')");
      create(ctx + "wa", SchemaType.JSON, "(tag e1 NOT NULL)");
      create(ctx + "wb", SchemaType.JSON, "(tag e2 NOT NULL)");
      create(ctx + "wa2", SchemaType.JSON, "(tag e1 NOT NULL, z INT)");
    }
    create("ma", SchemaType.JSON, "(inner wa NOT NULL)");
    create("mb", SchemaType.JSON, "(inner wb NOT NULL)");
    create("ma2", SchemaType.JSON, "(inner wa2 NOT NULL)");
    create("sok", SchemaType.JSON, "(u UNION(a ma, b mb))");
    jsonRefused("sno", "(u UNION(a ma, b ma2))");
    create("smap", SchemaType.JSON, "(u UNION(a ma, b MAP<STRING, wb>))");
    jsonRefused("smapno", "(u UNION(a ma, b MAP<STRING, wa>))");
    create("dok", SchemaType.JSON, "(u UNION(a `:.ctx:wa`, b `:.ctx:wb`))");
    jsonRefused("dno", "(u UNION(a `:.ctx:wa`, b `:.ctx:wa2`))");
    // A type two of the referred types refer to.
    create("mx", SchemaType.JSON, "(t e1 NOT NULL, f e2)");
    create("my", SchemaType.JSON, "(t e2 NOT NULL)");
    create("sdiamond", SchemaType.JSON, "(u UNION(a mx, b my))");
  }

  // A union with a name in it is judged wherever it is: in a map, an array, a multiset, in
  // a type that names itself; and whatever the name is of: a definition of the type's own,
  // a reference stored under another name, a root that is a union of its definitions.
  @Test
  void aJsonUnionsNamedBranchIsJudgedWhereverAndHoweverNamed() throws Exception {
    registry.create("je", SchemaType.JSON, BodyKind.ENUM, "('A', 'B')");
    jsonRefused("h21", "(m MAP<STRING, UNION(a STRING, b je)>)");
    jsonRefused("h21b", "(m MAP<UNION(a STRING, b je), INT>)");
    jsonRefused("h22", "(m ARRAY<UNION(a STRING, b je)>)");
    jsonRefused("h22b", "(m MULTISET<UNION(a STRING, b je)>)");
    jsonRefused("rec1", "(nxt rec1, u UNION(a STRING, b je))");
    registry.client.register("hu", new JsonSchema("{\"oneOf\":[{\"$ref\":\"#/$defs/A\"},"
        + "{\"type\":\"boolean\"}],\"$defs\":{\"A\":{\"enum\":[\"X\",\"Y\"],\"title\":\"A\"}}}"));
    jsonRefused("t5", "(u UNION(a STRING, b hu))");
    registry.client.register("h1", new JsonSchema("{\"type\":\"object\",\"properties\":{"
        + "\"id\":{\"type\":\"integer\"},\"c\":{\"$ref\":\"enums/color.json\"}},"
        + "\"additionalProperties\":false}",
        List.of(new SchemaReference("enums/color.json", "je", 1)),
        Map.of("enums/color.json", registry.latest("je").getSchema()), null));
    assertThatThrownBy(() -> registry.alter("h1",
        new AddAttribute("u", "UNION(a STRING, b je)")))
        .isInstanceOf(TypeRefusedException.class).hasMessageContaining("cannot have branches");
    // A name of the type's own definitions that a statement gives a subject.
    registry.create("Color", SchemaType.JSON, BodyKind.ENUM, "('X', 'Y')");
    registry.client.register("h3", new JsonSchema("{\"type\":\"object\",\"properties\":{"
        + "\"id\":{\"type\":\"integer\"},\"c\":{\"$ref\":\"#/$defs/Color\"}},"
        + "\"additionalProperties\":false,\"$defs\":{\"Color\":{\"type\":\"object\","
        + "\"title\":\"Color\",\"properties\":{\"r\":{\"type\":\"integer\"}},"
        + "\"additionalProperties\":false}}}"));
    assertThatThrownBy(() -> registry.alter("h3",
        new AddAttribute("u", "UNION(a STRING, b Color)")))
        .isInstanceOf(TypeRefusedException.class).hasMessageContaining("cannot have branches");
    // A referred type whose own name is the created type's.
    registry.client.register("other", new JsonSchema("{\"type\":\"string\",\"enum\":[\"X\","
        + "\"Y\"],\"title\":\"h9\"}"));
    create("h9", SchemaType.JSON, "(id INT NOT NULL, z other, u UNION(a h9, b STRING))");
  }

  // A union whose branch's reference moves is judged again wherever it is: in an array, a
  // map's values or its keys, a branch, one of the type's own named types, a context. One
  // that had two such branches already is left.
  @Test
  void aJsonUnionIsJudgedAgainWhereverItIsWhenAReferenceMoves() throws Exception {
    String row = "STRUCT(k INT NOT NULL, m INT NOT NULL)";
    for (String ctx : List.of("", ":.ctx:")) {
      create(ctx + "w", SchemaType.JSON, "(k INT NOT NULL)");
      create(ctx + "v", SchemaType.JSON, "(id INT, u UNION(a " + row + ", b w))");
    }
    create("v16", SchemaType.JSON, "(id INT, arr ARRAY<UNION(a " + row + ", b w)>)");
    create("v17", SchemaType.JSON, "(id INT, mp MAP<STRING, UNION(a " + row + ", b w)>)");
    create("v19", SchemaType.JSON, "(id INT, mk MAP<UNION(a " + row + ", b w), INT>)");
    create("v18", SchemaType.JSON,
        "(id INT, u UNION(p BOOLEAN, q STRUCT(i UNION(a " + row + ", b w))))");
    nativeJson("v7", "USING TYPE Ref1 FOR REF 'w'; STRUCT N (u UNION(a Ref1, b " + row
        + ")); STRUCT v7 (id INT, n N); TYPE v7;", "w");
    nativeJson("v9", "USING TYPE Ref1 FOR REF 'w'; STRUCT N (i Ref1 NOT NULL); "
        + "STRUCT v9 (id INT, u UNION(a N, b STRUCT(i " + row + " NOT NULL))); TYPE v9;", "w");
    registry.alter("w", new AddAttribute("m", "INT"));
    registry.alter(":.ctx:w", new AddAttribute("m", "INT"));
    for (String type : List.of("v", ":.ctx:v", "v16", "v17", "v18", "v19", "v7", "v9")) {
      movedRefused(type, new AddAttribute("y", "w"));
    }
    // One whose branches a value was of both already, one of them a reference or one of
    // the type's own named types.
    create("w15", SchemaType.JSON, "(k INT)");
    nativeJson("v15", "USING TYPE Ref1 FOR REF 'w15'; STRUCT v15 (id INT, "
        + "u UNION(a Ref1, b STRUCT(k INT)));", "w15");
    nativeJson("n15", "USING TYPE Ref1 FOR REF 'w15'; STRUCT N (k INT); STRUCT n15 (id INT, "
        + "z Ref1, u UNION(a N, b STRUCT(k INT))); TYPE n15;", "w15");
    registry.alter("w15", new AddAttribute("m", "INT"));
    registry.alter("v15", new AddAttribute("y", "w15"));
    registry.alter("n15", new AddAttribute("y", "w15"));
    assertThat(version("v15")).isEqualTo(2);
    assertThat(version("n15")).isEqualTo(2);
  }

  // A file of the name and package of one of the format's own that is not that file, in a
  // field's type, its number, its label, its name or a message in it, is not taken for it.
  @Test
  void aProtobufFileThatIsNotTheFormatsOwnIsNotTakenForIt() throws Exception {
    String p3 = "syntax = \"proto3\";\n";
    Map<String, String> files = new LinkedHashMap<>();
    files.put("f8", "package confluent.type;\n\nmessage Decimal {\n  string value = 1;\n"
        + "  uint32 precision = 2;\n  int32 scale = 3;\n}\n");
    files.put("f9", "package confluent.type;\n\nmessage Decimal {\n  bytes value = 2;\n"
        + "  uint32 precision = 1;\n  int32 scale = 3;\n}\n");
    files.put("f10", "package google.protobuf;\n\nmessage Timestamp {\n  int64 seconds = 1;\n"
        + "  int32 nanos = 2;\n  message Inner {\n    int32 a = 1;\n  }\n}\n");
    files.put("f16", "package google.protobuf;\n\nmessage Timestamp {\n"
        + "  repeated int64 seconds = 1;\n  int32 nanos = 2;\n}\n");
    files.put("f17", "package google.protobuf;\n\nmessage Timestamp {\n  int64 secs = 1;\n"
        + "  int32 nanos = 2;\n}\n");
    // One whose fields are of a oneof, or tell that they are set.
    files.put("f18", "package google.protobuf;\n\nmessage Timestamp {\n  oneof at {\n"
        + "    int64 seconds = 1;\n    int32 nanos = 2;\n  }\n}\n");
    files.put("f19", "package google.protobuf;\n\nmessage Timestamp {\n"
        + "  optional int64 seconds = 1;\n  int32 nanos = 2;\n}\n");
    for (Map.Entry<String, String> file : files.entrySet()) {
      registry.client.register(file.getKey(), new ProtobufSchema(p3 + file.getValue()));
      assertThatThrownBy(() -> create("u" + file.getKey(), SchemaType.PROTOBUF,
          "(id INT, d " + file.getKey() + ")")).as(file.getKey())
          .isInstanceOf(RuntimeException.class);
      assertThat(registry.latest("u" + file.getKey())).as(file.getKey()).isNull();
    }
    // A referred file that imports a file one of the format's own imports is no such file.
    registry.client.register("ds", new ProtobufSchema(p3
        + "import \"google/protobuf/descriptor.proto\";\n\nmessage Ds {\n"
        + "  google.protobuf.FieldDescriptorProto d = 1;\n}\n"));
    create("jds", SchemaType.PROTOBUF, "(id INT COMMENT 'has metadata', d ds)");
    assertThat(version("jds")).isEqualTo(1);
  }

  // A JSON map keyed by strings is written as an object, and is read from an array of key
  // and value entries too, as Connect writes one whose key may be null. One stored so would
  // be written as an object, which its stored values are not: a statement that leaves the
  // type with it is refused, and one that drops it, or gives it another kind of type, not.
  @Test
  void aJsonMapStoredAsEntriesIsNotWrittenAsAnObject() throws Exception {
    String entries = "{\"type\":\"array\",\"connect.type\":\"map\",\"items\":{\"type\":"
        + "\"object\",\"properties\":{\"key\":{\"type\":\"string\"},\"value\":{\"type\":"
        + "\"integer\",\"connect.type\":\"int32\"}}}";
    Map<String, String> maps = new LinkedHashMap<>();
    maps.put("jem", entries + "}");
    maps.put("jes", entries + ",\"flink.type\":\"multiset\",\"flink.version\":\"1\"}");
    maps.put("jeu", "{\"oneOf\":[{\"type\":\"null\"}," + entries + "},{\"type\":\"object\","
        + "\"connect.type\":\"map\",\"additionalProperties\":{\"type\":\"string\"}}]}");
    maps.put("jea", "{\"type\":\"array\",\"items\":" + entries + "}}");
    maps.put("jer", entries + "}");
    // One keyed otherwise is written as entries, and one stored as an object as an object.
    maps.put("jei", entries.replace("\"key\":{\"type\":\"string\"}",
        "\"key\":{\"type\":\"integer\",\"connect.type\":\"int32\"}") + "}");
    maps.put("jeo", "{\"type\":\"object\",\"connect.type\":\"map\","
        + "\"additionalProperties\":{\"type\":\"string\"}}");
    for (Map.Entry<String, String> map : maps.entrySet()) {
      registry.client.register(map.getKey(), new JsonSchema("{\"type\":\"object\",\"title\":\""
          + map.getKey() + "\",\"properties\":{\"id\":{\"type\":\"integer\"},\"x\":"
          + map.getValue() + ",\"n\":{\"type\":\"integer\"}},\"additionalProperties\":false}"));
    }
    // One in a type the document defines is told as the attribute that is of that type, and
    // is no more in the way when that attribute is dropped or given another type.
    String defined = "{\"type\":\"object\",\"title\":\"%s\",\"properties\":{\"id\":{\"type\":"
        + "\"integer\"},\"x\":{\"$ref\":\"#/$defs/D\"},\"n\":{\"type\":\"integer\"}},"
        + "\"additionalProperties\":false,\"$defs\":{\"D\":{\"type\":\"object\",\"properties\":"
        + "{\"m\":" + entries + "}},\"additionalProperties\":false}}}";
    for (String type : List.of("jed", "jet")) {
      registry.client.register(type, new JsonSchema(String.format(defined, type)));
    }
    assertThatThrownBy(() -> registry.alter("jet", new AddAttribute("z", "INT")))
        .isInstanceOf(TypeRefusedException.class)
        .hasMessageStartingWith("Type jet cannot be altered: attribute x has a map stored as");
    registry.alter("jet", new AlterAttributeType("x", "INT"));
    assertThat(version("jet")).isEqualTo(2);
    for (String type : List.of("jem", "jes", "jeu", "jea", "jed")) {
      for (AlterAction action : List.of(new MoveAttribute("n", Position.first()),
          new AddAttribute("z", "INT"))) {
        assertThatThrownBy(() -> registry.alter(type, action)).as(type)
            .isInstanceOf(TypeRefusedException.class).hasMessage("Type " + type + " cannot be "
                + "altered: attribute x has a map stored as an array of key and value "
                + "entries, which would be written as an object, so its stored values would "
                + "be read wrongly. Drop the attribute, or register its new version as a JSON "
                + "schema");
      }
      assertThat(version(type)).as(type).isEqualTo(1);
      registry.alter(type, new DropAttribute("x", false),
          new MoveAttribute("n", Position.first()));
      assertThat(version(type)).as(type).isEqualTo(2);
    }
    assertThatThrownBy(() -> registry.alter("jer",
        new AlterAttributeType("x", "MAP<STRING, BIGINT>")))
        .isInstanceOf(TypeRefusedException.class).hasMessageContaining("attribute x has a map");
    registry.alter("jer", new AlterAttributeType("x", "ARRAY<INT>"));
    registry.alter("jei", new MoveAttribute("n", Position.first()));
    registry.alter("jeo", new MoveAttribute("n", Position.first()));
    for (String type : List.of("jer", "jei", "jeo")) {
      assertThat(version(type)).as(type).isEqualTo(2);
    }
    assertThat(registry.schema("jei").canonicalString()).contains("\"type\":\"array\"");
  }

  // A number said for a new Protobuf attribute is its number where no field has it and
  // none of a version had; it is not left out without a word.
  @Test
  void aProtobufAttributeIsAddedWithTheNumberSaid() {
    create("pnn", SchemaType.PROTOBUF, "(a INT, b STRING, u UNION(x INT, y STRING))");
    registry.alter("pnn", new AddAttribute("e", "INT WITH ('protobuf.field.number' = '9')"));
    assertThat(numbers(message("pnn"))).contains("e=9");
    registry.alter("pnn", new AddAttribute("f", "INT"));
    assertThat(numbers(message("pnn"))).contains("f=10");
    String[][] taken = {{"2", "attribute b has it"}, {"4", "branch y of attribute u has it"},
        {"19000", "Protobuf keeps it for itself"}};
    for (String[] number : taken) {
      assertThatThrownBy(() -> registry.alter("pnn", new AddAttribute("g",
          "INT WITH ('protobuf.field.number' = '" + number[0] + "')")))
          .isInstanceOf(TypeRefusedException.class).hasMessage("Attribute g cannot have the "
              + "Protobuf field number " + number[0] + ": " + number[1]
              + "; give it another, or none");
    }
    // A union is no field, and has no number for another to have: its members have.
    create("pnu", SchemaType.PROTOBUF, "(a INT, b STRING)");
    registry.alter("pnu", new AddAttribute("u", "UNION(x INT, y STRING)"),
        new AddAttribute("e", "INT WITH ('protobuf.field.number' = '3')"));
    assertThat(numbers(message("pnu")).split(" ")).contains("e=3", "x=4", "y=5");
    // A number that is none a field can have is refused, and not left out.
    for (String none : List.of("0", "-3")) {
      assertThatThrownBy(() -> registry.alter("pnu", new AddAttribute("g",
          "INT WITH ('protobuf.field.number' = '" + none + "')")))
          .isInstanceOf(TypeRefusedException.class).hasMessage("Property "
              + "protobuf.field.number of attribute g is not a field's number, which is 1 "
              + "or more: '" + none + "'");
    }
    // So is one said for a new union, which has none, or for a member of it.
    assertThatThrownBy(() -> registry.alter("pnu", new AddAttribute("w",
        "UNION(c INT, d STRING) WITH ('protobuf.field.number' = '60')")))
        .isInstanceOf(TypeRefusedException.class).hasMessage("Attribute w cannot have a "
            + "Protobuf field number: a UNION is a oneof, which has none; give it none");
    assertThatThrownBy(() -> registry.alter("pnu", new AddAttribute("w",
        "UNION(c INT WITH ('protobuf.field.number' = '60'), d STRING)")))
        .isInstanceOf(TypeRefusedException.class).hasMessage("Branch c of attribute w cannot "
            + "be given a Protobuf field number: the members of a new UNION are numbered as "
            + "the type's new fields are; give it none");
    assertThat(version("pnu")).isEqualTo(2);
    // The last number there is leaves none above it: the next are the lowest never used,
    // for a field and for a union's members.
    create("pnm", SchemaType.PROTOBUF, "(a INT, b STRING)");
    registry.alter("pnm", new AddAttribute("c",
        "STRING WITH ('protobuf.field.number' = '536870911')"));
    registry.alter("pnm", new DropAttribute("b", false));
    registry.alter("pnm", new AddAttribute("d", "INT", Position.first()));
    registry.alter("pnm", new AddAttribute("u", "UNION(x INT, y STRING)"));
    registry.alter("pnm", new AddAttribute("e", "INT"));
    assertThat(numbers(message("pnm")).split(" "))
        .containsExactlyInAnyOrder("a=1", "c=536870911", "d=3", "x=5", "y=6", "e=4");
    // As in a row's message, and for a union in it.
    create("pnr", SchemaType.PROTOBUF, "(a INT, "
        + "r STRUCT(x INT, y INT WITH ('protobuf.field.number' = '536870911')) NOT NULL)");
    registry.alter("pnr", new AlterAttributeType("r",
        "STRUCT(x INT, y INT, z INT, u UNION(p INT, q STRING)) NOT NULL"));
    assertThat(numbers(message("pnr").findFieldByName("r").getMessageType()).split(" "))
        .containsExactlyInAnyOrder("x=1", "y=536870911", "z=2", "p=3", "q=4");
    // A number said for a member of a type given anew is the one it has, or is refused.
    create("pnt", SchemaType.PROTOBUF,
        "(a INT, r STRUCT(m INT, n STRING), u UNION(x INT, y STRING))");
    registry.alter("pnt", new AlterAttributeType("r",
        "STRUCT(m INT WITH ('protobuf.field.number' = '1'), n STRING, k INT)"));
    assertThat(version("pnt")).isEqualTo(2);
    String[][] others = {
        {"r", "STRUCT(m INT, n STRING, k INT, j INT WITH ('protobuf.field.number' = '50'))",
            "Attribute j cannot be given the Protobuf field number 50: a new member of a type "
            + "given anew is numbered as a new attribute is; give it none"},
        {"r", "STRUCT(m INT WITH ('protobuf.field.number' = '51'), n STRING, k INT)",
            "Attribute m cannot be given the Protobuf field number 51: it has 1; give it none"},
        {"u", "UNION(x INT, y STRING, z BIGINT WITH ('protobuf.field.number' = '80'))",
            "Branch z cannot be given the Protobuf field number 80: a new member of a type "
            + "given anew is numbered as a new attribute is; give it none"},
        {"a", "UNION(p INT WITH ('protobuf.field.number' = '70'), q STRING)",
            "Branch p cannot be given the Protobuf field number 70: a new member of a type "
            + "given anew is numbered as a new attribute is; give it none"}};
    for (String[] other : others) {
      assertThatThrownBy(() -> registry.alter("pnt", new AlterAttributeType(other[0], other[1])))
          .as(other[1]).isInstanceOf(TypeRefusedException.class).hasMessage(other[2]);
    }
    assertThat(version("pnt")).isEqualTo(2);
    registry.alter("pnn", new DropAttribute("e", false));
    assertThatThrownBy(() -> registry.alter("pnn", new AddAttribute("g",
        "INT WITH ('protobuf.field.number' = '9')")))
        .isInstanceOf(TypeRefusedException.class)
        .hasMessageContaining("an attribute of an earlier version had it");
  }

  // A JSON union's null is one of its alternatives, beside its members, as in Avro: one
  // beside each member too would have a null value match them all, which a oneOf refuses.
  @Test
  void aJsonUnionHasNullAsOneOfItsAlternatives() throws Exception {
    String flat = "\"oneOf\":[{\"type\":\"null\"},{\"connect.type\":\"int32\","
        + "\"type\":\"number\"},{\"type\":\"string\"}]";
    create("ju", SchemaType.JSON, "(id INT NOT NULL, u UNION(x INT, y STRING))");
    create("jn", SchemaType.JSON,
        "(id INT NOT NULL, u UNION(x INT NOT NULL, y STRING NOT NULL))");
    for (String type : List.of("ju", "jn")) {
      JsonSchema schema = (JsonSchema) registry.schema(type);
      assertThat(schema.canonicalString()).as(type).contains(flat);
      schema.validate(new ObjectMapper().readTree("{\"id\":1,\"u\":null}"));
      schema.validate(new ObjectMapper().readTree("{\"id\":1,\"u\":5}"));
    }
    // One that may not be null has none.
    create("jr", SchemaType.JSON, "(id INT NOT NULL, u UNION(x INT, y STRING) NOT NULL)");
    assertThat(registry.schema("jr").canonicalString()).doesNotContain("\"null\"");

    // A type stored with a null beside each member is read as it was, and written so.
    String nested = registry.schema("ju").canonicalString().replace("\"title\":\"ju\"",
        "\"title\":\"jo\"").replace(flat, "\"oneOf\":[{\"type\":\"null\"},{\"oneOf\":["
        + "{\"type\":\"null\"},{\"connect.type\":\"int32\",\"type\":\"number\"}]},"
        + "{\"oneOf\":[{\"type\":\"null\"},{\"type\":\"string\"}]}]");
    registry.client.register("jo", new JsonSchema(nested));
    assertThat(LogicalTypeToDdlConverter.toDdl(registry.logicalType("jo", registry.latest("jo")),
        true)).isEqualTo(LogicalTypeToDdlConverter.toDdl(registry.logicalType("ju",
        registry.latest("ju")), true).replace("ju", "jo"));
    registry.alter("jo", new AddAttribute("z", "INT"));
    assertThat(registry.schema("jo").canonicalString()).contains(flat);
  }

  // The names of the messages a message declares, at every level, with their fields' numbers.
  private static List<String> typeNames(Descriptor message) {
    List<String> names = new ArrayList<>();
    for (Descriptor inner : message.getNestedTypes()) {
      names.add(inner.getName() + "{" + numbers(inner) + "}");
      names.addAll(typeNames(inner));
    }
    return names;
  }

  // An enum's name is one of the message's as a nested message's is.
  @Test
  void anAttributeNamedAsAnEnumOfItsProtobufTypeIsRefused() {
    create("en", SchemaType.PROTOBUF, "(id INT NOT NULL, e ENUM('A', 'B'), bar INT)");
    String name = message("en").findFieldByName("e").getEnumType().getName();
    for (String type : List.of("INT", "UNION(p INT, q STRING)")) {
      assertThatThrownBy(() -> registry.alter("en", new AddAttribute(name, type)))
          .isInstanceOf(TypeRefusedException.class).hasMessageContaining("Protobuf name '"
              + name + "' is defined twice in en").hasMessageContaining(" an enum");
    }
    assertThat(version("en")).isEqualTo(1);
    // And an enum may be what takes a message's name.
    create("em", SchemaType.PROTOBUF, "(id INT NOT NULL, foo STRUCT(x STRING))");
    assertThatThrownBy(() -> registry.alter("em", new AddAttribute("k",
        "ENUM('A', 'B') WITH ('protobuf.name' = 'FooRow')")))
        .isInstanceOf(TypeRefusedException.class).hasMessageContaining(
            "Protobuf name 'FooRow' is defined twice in em");
    assertThat(version("em")).isEqualTo(1);
  }

  // A reference to a whole document is given a name, Ref1, that is read with the namespace:
  // a document so titled is written with no name, as its declaration would be that type.
  @Test
  void aNamespacedJsonTypeTitledAsItsWholeDocumentReferenceIsAltered() throws Exception {
    String common = "{\"type\": \"object\", \"properties\": "
        + "{\"x\": {\"type\": \"string\"}}}";
    registry.client.register("common", new JsonSchema(common));
    SchemaReference reference = new SchemaReference("common.json", "common", 1);
    // The title may give the namespace too.
    for (String title : List.of("Ref1", "shop.Ref1")) {
      String subject = "jr" + title.length();
      registry.client.register(subject, new JsonSchema("{\"type\": \"object\", \"title\": \""
          + title + "\", \"confluent:namespace\": \"shop\", \"properties\": {\"id\": "
          + "{\"type\": \"string\"}, \"prev\": {\"$ref\": \"common.json\"}}}",
          List.of(reference), Map.of("common.json", common), null));
      registry.alter(subject, new AddAttribute("nick", "STRING"));
      assertThat(version(subject)).isEqualTo(2);
      assertThat(registry.schema(subject).canonicalString()).contains(
          "\"$ref\":\"common.json\"", "\"nick\"", "\"confluent:namespace\":\"shop\"");
      assertThat(registry.schema(subject).references()).containsExactly(reference);
    }
  }

  // A definition of the document's own takes the name as one it refers to does.
  @ParameterizedTest
  @ValueSource(strings = {"Order", "shop.Order"})
  void aNamespacedJsonTypeTitledAsADefinitionOfItsOwnIsAltered(String title) throws Exception {
    registry.client.register("jd", new JsonSchema("{\"type\": \"object\", \"title\": \""
        + title + "\", \"confluent:namespace\": \"shop\", \"properties\": {\"id\": "
        + "{\"type\": \"string\"}, \"prev\": {\"$ref\": \"#/$defs/Order\"}}, \"$defs\": "
        + "{\"Order\": {\"type\": \"object\", \"properties\": {\"x\": {\"type\": "
        + "\"string\"}}}}}"));
    registry.alter("jd", new AddAttribute("nick", "STRING"));
    assertThat(version("jd")).isEqualTo(2);
    // The definition is written by its name in the namespace, as it was before.
    assertThat(registry.schema("jd").canonicalString())
        .contains("\"$ref\":\"#/$defs/shop.Order\"", "\"nick\"");
  }

  // A title that is dotted beyond its namespace is read as written, and one under a
  // definition of the document's reads as nested in it, which no root is.
  @Test
  void aJsonTypeOfADottedTitleBesideItsDefinitionsIsAltered() throws Exception {
    String member = "{\"type\": \"object\", \"properties\": {\"m\": {\"type\": \"string\"}}}";
    // {title, namespace, the definitions it declares and refers to}
    String[][] cases = {
        {"shop.Outer.Order", "shop", "Outer.Order"},
        {"shop.shop.Order", "shop", "shop.Order"},
        {"Outer.Order", "shop", "Outer", "shop.Outer.Order"},
        {"Outer.Order", "shop", "Outer"},
        {"Outer.Order", null, "Outer"}};
    for (int i = 0; i < cases.length; i++) {
      String[] c = cases[i];
      StringBuilder properties = new StringBuilder("\"id\": {\"type\": \"string\"}");
      StringBuilder defs = new StringBuilder();
      for (int d = 2; d < c.length; d++) {
        properties.append(", \"p").append(d).append("\": {\"$ref\": \"#/$defs/")
            .append(c[d]).append("\"}");
        defs.append(d > 2 ? ", " : "").append('"').append(c[d]).append("\": ").append(member);
      }
      String subject = "dt" + i;
      registry.client.register(subject, new JsonSchema("{\"type\": \"object\", \"title\": \""
          + c[0] + "\", " + (c[1] == null ? "" : "\"confluent:namespace\": \"" + c[1] + "\", ")
          + "\"properties\": {" + properties + "}, \"$defs\": {" + defs + "}}"));
      registry.alter(subject, new AddAttribute("nick", "STRING"));
      assertThat(version(subject)).as(c[0]).isEqualTo(2);
      String altered = registry.schema(subject).canonicalString();
      assertThat(altered).as(c[0]).contains("\"nick\"", "\"m\"");
      // Each definition it referred to is one it still refers to.
      assertThat(altered.split("\"\\$ref\":\"#/\\$defs/", -1).length - 1).as(c[0])
          .isGreaterThanOrEqualTo(c.length - 2);
      // The first two keep their name: the root is the definition of that name.
      if (i < 2) {
        assertThat(altered).contains("\"" + c[0] + "\":{").doesNotContain("\"title\":\"dt");
      }
    }
  }

  // A JSON document titled as the definition it refers to: they are two types of one name,
  // which only the reference tells apart.
  @ParameterizedTest
  @ValueSource(strings = {"", "\"confluent:namespace\": \"shop\", ",
      "\"confluent:namespace\": \"a.b\", "})
  void aJsonTypeTitledAsTheDefinitionItRefersToIsAltered(String namespace) throws Exception {
    String common = "{\"$defs\": {\"Order\": {\"type\": \"object\", \"properties\": "
        + "{\"x\": {\"type\": \"string\"}}}, \"shop.Order\": {\"type\": \"object\", "
        + "\"properties\": {\"y\": {\"type\": \"string\"}}}}}";
    registry.client.register("common", new JsonSchema(common));
    SchemaReference reference = new SchemaReference("common.json", "common", 1);
    // The title may give the namespace too.
    for (String[] titled : List.of(new String[] {"Order", "Order"},
        new String[] {"Order", "shop.Order"}, new String[] {"shop.Order", "shop.Order"})) {
      String definition = titled[1];
      String subject = "js" + titled[0].length() + definition.length() + namespace.length();
      registry.client.register(subject, new JsonSchema("{\"type\": \"object\", \"title\": \""
          + titled[0] + "\", " + namespace + "\"properties\": {\"id\": {\"type\": "
          + "\"string\"}, \"prev\": {\"$ref\": \"common.json#/$defs/" + definition + "\"}}}",
          List.of(reference), Map.of("common.json", common), null));
      registry.alter(subject, new AddAttribute("nick", "STRING"));
      assertThat(version(subject)).isEqualTo(2);
      assertThat(registry.schema(subject).canonicalString())
          .contains("common.json#/$defs/" + definition, "\"nick\"");
      assertThat(registry.schema(subject).references()).containsExactly(reference);
    }
  }

  // The referred message has the root's own simple name, PostalAddress, and no package.
  @Test
  void aRootNamedAsTheTypeOfNoNamespaceItRefersToKeepsItsName() throws Exception {
    String common = "syntax = \"proto3\";\n\nmessage PostalAddress {\n  string street = 1;\n}\n";
    registry.client.register("common", new ProtobufSchema(common));
    SchemaReference reference = new SchemaReference("common.proto", "common", 1);
    ProtobufSchema stored = new ProtobufSchema("syntax = \"proto3\";\npackage shop;\n\n"
        + "import \"common.proto\";\n\n"
        + "message PostalAddress {\n"
        + "  string id = 1;\n"
        + "  optional .PostalAddress billing = 2;\n"
        + "}\n", List.of(reference), Map.of("common.proto", common), null, null);
    registry.client.register("pa", stored);
    registry.alter("pa", new AddAttribute("nickname", "STRING"));
    ProtobufSchema altered = (ProtobufSchema) registry.schema("pa");
    assertThat(altered.toDescriptor().getFullName()).isEqualTo("shop.PostalAddress");
    assertThat(altered.toDescriptor().findFieldByName("billing").getMessageType().getFullName())
        .isEqualTo("PostalAddress");
    assertThat(altered.isBackwardCompatible(stored)).isEmpty();
    assertThat(altered.references()).containsExactly(reference);
  }

  // A type of a namespace, registered as Avro, that refers to one of none: the registry's DDL
  // names the referred type as itself, so the type can be altered and keeps its reference.
  @Test
  void aTypeReferringToOneOfNoNamespaceIsAltered() throws Exception {
    registry.client.register("PostalAddress", new AvroSchema("{\"type\":\"record\","
        + "\"name\":\"PostalAddress\",\"fields\":[{\"name\":\"street\",\"type\":\"string\"}]}"));
    SchemaReference reference = new SchemaReference("PostalAddress", "PostalAddress", 1);
    registry.client.register("customer", new AvroSchema("{\"type\":\"record\","
        + "\"name\":\"Customer\",\"namespace\":\"shop\",\"fields\":["
        + "{\"name\":\"id\",\"type\":\"string\"},"
        + "{\"name\":\"addr\",\"type\":[\"null\",\"PostalAddress\"],\"default\":null}]}",
        List.of(reference), Map.of("PostalAddress",
            registry.client.getLatestSchemaMetadata("PostalAddress").getSchema()), null));
    registry.alter("customer", new AddAttribute("nickname", "STRING"));
    assertThat(version("customer")).isEqualTo(2);
    assertThat(registry.schema("customer").canonicalString())
        .contains("\"type\":[\"null\",\"PostalAddress\"]").contains("\"name\":\"nickname\"");
    assertThat(registry.schema("customer").references()).containsExactly(reference);
  }

  // The referred type's name begins with that of a record the type declares, PostalAddress,
  // and is no type nested in that one.
  @Test
  void aTypeReferringToOneNamedLikeANestedTypeOfItsOwnIsAltered() throws Exception {
    registry.client.register("geo", new AvroSchema("{\"type\":\"record\",\"name\":\"Geo\","
        + "\"namespace\":\"PostalAddress\",\"fields\":[{\"name\":\"lat\",\"type\":\"double\"}]}"));
    SchemaReference reference = new SchemaReference("PostalAddress.Geo", "geo", 1);
    registry.client.register("customer", new AvroSchema("{\"type\":\"record\","
        + "\"name\":\"Customer\",\"namespace\":\"shop\",\"fields\":["
        + "{\"name\":\"home\",\"type\":{\"type\":\"record\",\"name\":\"PostalAddress\","
        + "\"fields\":[{\"name\":\"street\",\"type\":\"string\"}]}},"
        + "{\"name\":\"geo\",\"type\":\"PostalAddress.Geo\"}]}",
        List.of(reference), Map.of("PostalAddress.Geo",
            registry.client.getLatestSchemaMetadata("geo").getSchema()), null));
    registry.alter("customer", new AddAttribute("nickname", "STRING"));
    assertThat(version("customer")).isEqualTo(2);
    org.apache.avro.Schema altered = ((AvroSchema) registry.schema("customer")).rawSchema();
    assertThat(altered.getField("geo").schema().getFullName()).isEqualTo("PostalAddress.Geo");
    assertThat(altered.getField("home").schema().getFullName())
        .isEqualTo("shop.PostalAddress");
    assertThat(registry.schema("customer").references()).containsExactly(reference);
  }

  // Written from the logical type a message Flink named addr_Row would be AddrRow, which the
  // registry takes for another type: an alter keeps the names the version has.
  @Test
  void aProtobufTypeFlinkWroteKeepsTheNamesOfItsMessages() throws Exception {
    ProtobufSchema flink = new ProtobufSchema("syntax = \"proto3\";\npackage shop;\n\n"
        + "message Record {\n"
        + "  optional string id = 1;\n"
        + "  optional addr_Row addr = 2;\n"
        + "  repeated item_Row item = 3;\n"
        + "  oneof pay {\n"
        + "    pay_card_Row card = 4;\n"
        + "    string iban = 5;\n"
        + "  }\n"
        + "  optional Shared one = 6;\n"
        + "  optional Shared other = 7;\n\n"
        + "  message addr_Row {\n"
        + "    optional string street = 1;\n"
        + "    optional string zip = 2;\n"
        + "    optional string city = 3;\n"
        + "    optional geo_Row geo = 4;\n"
        + "    message geo_Row {\n"
        + "      optional double lat = 1;\n"
        + "    }\n"
        + "  }\n"
        + "  message item_Row {\n"
        + "    optional string sku = 1;\n"
        + "    optional dim_Row dim = 2;\n"
        + "    message dim_Row {\n"
        + "      optional int32 w = 1;\n"
        + "    }\n"
        + "  }\n"
        + "  message pay_card_Row {\n"
        + "    optional string number = 1;\n"
        + "  }\n"
        + "  message Shared {\n"
        + "    optional in_Row in = 1;\n"
        + "    message in_Row {\n"
        + "      optional string x = 1;\n"
        + "    }\n"
        + "  }\n"
        + "}\n");
    registry.client.register("places", flink);
    registry.alter("places", new AddAttribute("nickname", "STRING"));
    ProtobufSchema added = (ProtobufSchema) registry.schema("places");
    assertThat(added.toDescriptor().findFieldByName("addr").getMessageType().getName())
        .isEqualTo("addr_Row");
    assertThat(added.toDescriptor().findFieldByName("item").getMessageType().getName())
        .isEqualTo("item_Row");
    // At every level: a row in a row and in an array's, a oneof member's, a shared message's.
    assertThat(added.isBackwardCompatible(flink)).isEmpty();
    assertThat(added.canonicalString()).contains("message geo_Row", "message dim_Row",
        "message pay_card_Row", "message in_Row");

    // Retyped, it is the message it was, its fields at their numbers, and so is the row it
    // keeps inside.
    registry.alter("places", new AlterAttributeType("addr",
        "STRUCT(street STRING, city STRING, geo STRUCT(lat DOUBLE, lon DOUBLE))"));
    Descriptor addr = message("places").findFieldByName("addr").getMessageType();
    assertThat(addr.getName()).isEqualTo("addr_Row");
    assertThat(numbers(addr)).isEqualTo("street=1 city=3 geo=4");
    assertThat(addr.findFieldByName("geo").getMessageType().getName()).isEqualTo("geo_Row");
    assertThat(version("places")).isEqualTo(3);

    // The row is the one it was, whatever name the new type pins on it.
    registry.alter("places", new AlterAttributeType("addr", "STRUCT(street STRING, "
        + "city STRING, geo STRUCT(lat DOUBLE, lon DOUBLE) WITH ('protobuf.name' = 'MyGeo'), "
        + "zip STRING)"));
    assertThat(message("places").findFieldByName("addr").getMessageType()
        .findFieldByName("geo").getMessageType().getName()).isEqualTo("geo_Row");
    // A member named as that row's message could not be written beside it.
    assertThatThrownBy(() -> registry.alter("places", new AlterAttributeType("addr",
        "STRUCT(street STRING, city STRING, geo STRUCT(lat DOUBLE, lon DOUBLE), zip STRING, "
            + "geo_Row INT)"))).isInstanceOf(TypeRefusedException.class)
        .hasMessageContaining("no PROTOBUF schema can be made of it: Protobuf name 'geo_Row' "
            + "is defined twice in Record.addr_Row");
    assertThat(version("places")).isEqualTo(4);
  }

  // Below the root as at it: a member named as the message this writer derives for another.
  @Test
  void aMemberNamedAsAMessageOfANestedProtobufRowIsRefused() {
    create("pr", SchemaType.PROTOBUF,
        "(id INT NOT NULL, addr STRUCT(street STRING, geo STRUCT(lat DOUBLE)))");
    assertThatThrownBy(() -> registry.alter("pr", new AlterAttributeType("addr",
        "STRUCT(street STRING, geo STRUCT(lat DOUBLE), GeoRow INT)")))
        .isInstanceOf(TypeRefusedException.class)
        .hasMessageContaining("Protobuf name 'GeoRow' is defined twice in pr.AddrRow");
    assertThatThrownBy(() -> registry.alter("pr", new AlterAttributeType("addr",
        "STRUCT(street STRING, geo STRUCT(lat DOUBLE), u UNION(GeoRow INT, s STRING))")))
        .isInstanceOf(TypeRefusedException.class)
        .hasMessageContaining("'GeoRow' is defined twice in pr.AddrRow");
    assertThat(version("pr")).isEqualTo(1);
  }

  // A type the attribute has already makes no version, a position with it or not.
  @ParameterizedTest
  @EnumSource(SchemaType.class)
  void anAttributeRetypedAsItIsMakesNoVersion(SchemaType format) {
    create("same", format, "(a INT, b STRING, s STRUCT(x INT))");
    assertThat(registry.alter("same", new AlterAttributeType("a", "INT"),
        new MoveAttribute("a", Position.first()),
        new AlterAttributeType("s", "STRUCT(x INT)")).edited()).isNull();
    assertThat(version("same")).isEqualTo(1);
    // A move with it is still made, where the format keeps an order.
    registry.alter("same", new AlterAttributeType("b", "STRING"),
        new MoveAttribute("b", Position.first()));
    assertThat(version("same")).isEqualTo(2);
    assertThat(names("same")).containsExactly("b", "a", "s");
  }

  // What makes a version is what the statement leaves, set beside what is stored: a type read
  // back otherwise than it is said, as a union's branches or a map's key, is the type it was.
  @ParameterizedTest
  @EnumSource(SchemaType.class)
  void aStatementThatLeavesTheTypeAsItIsStoredMakesNoVersion(SchemaType format) {
    Map<String, String> types = new LinkedHashMap<>();
    types.put("u", "UNION(i INT, t STRING)");
    types.put("m", "MAP<STRING, INT>");
    types.put("e", "MULTISET<STRING>");
    types.put("s", "STRUCT(v UNION(i INT, t STRING), w MAP<STRING, STRUCT(k INT)>)");
    StringBuilder body = new StringBuilder("(a INT NOT NULL");
    types.forEach((name, type) -> body.append(", ").append(name).append(' ').append(type));
    create("kept", format, body.append(", p STRUCT(x INT, y STRING))").toString());
    // Each said as it was created, moved first and back after the attribute before it; a
    // Protobuf union, which cannot go first, is moved where it is.
    List<String> order = names("kept");
    for (Map.Entry<String, String> type : types.entrySet()) {
      String name = type.getKey();
      String before = order.get(order.indexOf(name) - 1);
      boolean union = format == SchemaType.PROTOBUF && name.equals("u");
      assertThat(registry.alter("kept", new AlterAttributeType(name, type.getValue()),
          new MoveAttribute(name, union ? Position.after(before) : Position.first()),
          new MoveAttribute(name, Position.after(before))).edited()).as(name).isNull();
    }
    // An attribute added and dropped, and a comment set and taken away.
    assertThat(registry.alter("kept", new AddAttribute("z", "INT", Position.first()),
        new DropAttribute("z", false)).edited()).isNull();
    assertThat(registry.alter("kept", new SetAttributeComment("a", "the key"),
        new SetAttributeComment("a", null)).edited()).isNull();
    assertThat(version("kept")).isEqualTo(1);

    // What changes only a comment or a CHECK is still a version.
    registry.alter("kept", new AlterAttributeType("u", types.get("u")),
        new SetAttributeComment("a", "the key"));
    assertThat(version("kept")).isEqualTo(2);
    registry.alter("kept", new AddConstraint(new Check("ck", "a > 0", null)));
    assertThat(version("kept")).isEqualTo(3);
    // And so is a row said with its members in another order: it is the order they then have.
    registry.alter("kept", new AlterAttributeType("p", "STRUCT(y STRING, x INT)"));
    assertThat(version("kept")).isEqualTo(4);
  }

  // A schema this writer did not write is rewritten by a version, without what the logical
  // type does not carry, as an enum's default: a type said as it is must not make one.
  @Test
  void anAttributeOfANativeSchemaRetypedAsItIsMakesNoVersion() throws Exception {
    registry.client.register("nv", new AvroSchema("{\"type\": \"record\", \"name\": \"R\", "
        + "\"fields\": [{\"name\": \"id\", \"type\": \"int\"}, {\"name\": \"e\", \"type\": "
        + "{\"type\": \"enum\", \"name\": \"E\", \"symbols\": [\"A\", \"B\"], "
        + "\"default\": \"A\"}}]}"));
    assertThat(registry.latest("nv").getMetadata()).isNull();
    assertThat(registry.alter("nv", new AlterAttributeType("id", "INT NOT NULL"),
        new MoveAttribute("id", Position.first())).edited()).isNull();
    assertThat(version("nv")).isEqualTo(1);
  }

  // A oneof is read after the fields beside it, and the messages a type declares are written
  // in its attributes' order: a file that only declares them in another order is the same.
  @Test
  void aProtobufTypeDeclaringItsMessagesInAnotherOrderIsNoVersion() {
    create("pu", SchemaType.PROTOBUF,
        "(id INT NOT NULL, u UNION(r STRUCT(k INT), s STRING), w STRUCT(q INT))");
    String stored = registry.schema("pu").canonicalString();
    assertThat(registry.alter("pu", new AddAttribute("d", "INT"),
        new DropAttribute("d", false)).edited()).isNull();
    assertThat(registry.alter("pu",
        new AlterAttributeType("u", "UNION(r STRUCT(k INT), s STRING)")).edited()).isNull();
    assertThat(version("pu")).isEqualTo(1);

    // Below the root's own messages too, and for enums.
    create("pn", SchemaType.PROTOBUF,
        "(id INT NOT NULL, s STRUCT(u UNION(r STRUCT(k INT), t STRING), w STRUCT(q INT)))");
    create("pe", SchemaType.PROTOBUF,
        "(id INT NOT NULL, u UNION(e ENUM('A', 'B'), s STRING), f ENUM('C', 'D'))");
    for (String type : List.of("pn", "pe")) {
      assertThat(registry.alter(type, new AddAttribute("d", "INT"),
          new DropAttribute("d", false)).edited()).as(type).isNull();
    }
    // The same lines in other places are another schema: two comments swapped.
    create("ps", SchemaType.PROTOBUF, "(id INT NOT NULL, a INT COMMENT 'x', b INT COMMENT 'y', "
        + "u UNION(r STRUCT(k INT), s STRING), w STRUCT(q INT))");
    String lines = registry.schema("ps").canonicalString();
    registry.alter("ps", new SetAttributeComment("a", "y"), new SetAttributeComment("b", "x"));
    assertThat(version("ps")).isEqualTo(2);
    assertThat(registry.schema("ps").canonicalString().lines().sorted())
        .containsExactlyElementsOf(lines.lines().sorted().collect(Collectors.toList()));

    // What changes a line of it is a version: a comment, a CHECK, a member of a nested row.
    registry.alter("pu", new SetAttributeComment("w", "the weight"));
    registry.alter("pu", new AddConstraint(new Check("ck", "id > 0", null)));
    registry.alter("pu", new AlterAttributeType("w", "STRUCT(q INT, p INT)"));
    assertThat(version("pu")).isEqualTo(4);
    assertThat(registry.schema("pu").canonicalString()).isNotEqualTo(stored)
        .contains("the weight", "id > 0");
  }

  // The names of an Avro root union's branches are in a property written beside the schema,
  // which the version is read with: a branch is added, and one commented is a version of the
  // same schema.
  @Test
  void anAvroRootUnionIsAltered() {
    registry.create("ru", SchemaType.AVRO, BodyKind.OTHER, "UNION(i INT, s STRING)");
    registry.alter("ru", new SetDataType("UNION(i INT, s STRING, l BIGINT)"));
    assertThat(version("ru")).isEqualTo(2);
    String text = registry.schema("ru").canonicalString();
    assertThat(text).contains("\"long\"");

    String commented = "UNION(i INT COMMENT 'the count', s STRING, l BIGINT)";
    registry.alter("ru", new SetDataType(commented));
    assertThat(version("ru")).isEqualTo(3);
    assertThat(registry.schema("ru").canonicalString()).isEqualTo(text);
    assertThat(registry.latest("ru").getMetadata().getProperties().get("confluent:union"))
        .contains("the count");
    assertThat(registry.alter("ru", new SetDataType(commented)).edited()).isNull();
  }

  // The properties the writer sets are part of what is stored: a schema registered without
  // them, as another tool's is, is not yet what a statement would write of it.
  @Test
  void aSchemaStoredWithoutTheWritersPropertiesIsWrittenWithThem() throws Exception {
    create("np", SchemaType.AVRO, "(a INT NOT NULL, u UNION(i INT, t STRING))");
    String text = registry.schema("np").canonicalString();
    registry.client.deleteSubject("np");
    registry.client.deleteSubject("np", true);
    registry.client.register("np", new AvroSchema(text));
    assertThat(registry.latest("np").getMetadata()).isNull();

    registry.alter("np", new AlterAttributeType("u", "UNION(i INT, t STRING)"));
    assertThat(version("np")).isEqualTo(2);
    assertThat(registry.schema("np").canonicalString()).isEqualTo(text);
    assertThat(registry.latest("np").getMetadata().getProperties())
        .containsEntry("confluent:edition", "2");
    assertThat(registry.alter("np",
        new AlterAttributeType("u", "UNION(i INT, t STRING)")).edited()).isNull();
  }

  // The names a retyped attribute's members were given are kept in Avro as they are in
  // Protobuf, where the records are otherwise named anew after the attribute.
  @Test
  void aRetypedAvroAttributeKeepsTheNamesPinnedBelowIt() {
    create("av", SchemaType.AVRO, "(id INT NOT NULL, addr STRUCT(street STRING, "
        + "geo STRUCT(lat DOUBLE) WITH ('avro.name' = 'GeoPoint')))");
    AvroSchema before = (AvroSchema) registry.schema("av");
    assertThat(before.canonicalString()).contains("\"name\":\"GeoPoint\"");
    registry.alter("av", new AlterAttributeType("addr",
        "STRUCT(street STRING, geo STRUCT(lat DOUBLE, lon DOUBLE), zip STRING)"));
    AvroSchema after = (AvroSchema) registry.schema("av");
    assertThat(after.canonicalString()).contains("\"name\":\"GeoPoint\"");
    assertThat(after.isBackwardCompatible(before)).isEmpty();

    // A collection that may now be null, or no longer, is the type it was with a null beside
    // it: its entry's or element's record keeps its name, at the root and below it.
    String[][] types = {
        {"MAP<INT NOT NULL, STRING> NOT NULL WITH ('avro.entry.name' = 'Kept')",
            "MAP<INT NOT NULL, STRING>"},
        {"STRUCT(m MAP<INT NOT NULL, STRING> WITH ('avro.entry.name' = 'Kept'))",
            "STRUCT(m MAP<INT NOT NULL, STRING> NOT NULL)"}};
    for (int i = 0; i < types.length; i++) {
      String type = "an" + i;
      create(type, SchemaType.AVRO, "(id INT NOT NULL, a " + types[i][0] + ")");
      assertThat(registry.schema(type).canonicalString()).as(type)
          .contains("\"name\":\"Kept\"");
      registry.alter(type, new AlterAttributeType("a", types[i][1]));
      assertThat(version(type)).as(type).isEqualTo(2);
      assertThat(registry.schema(type).canonicalString()).as(type)
          .contains("\"name\":\"Kept\"");
    }
  }
}
