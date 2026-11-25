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

import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.Descriptors.FieldDescriptor;
import io.confluent.kafka.schemaregistry.ParsedSchema;
import io.confluent.kafka.schemaregistry.avro.AvroSchema;
import io.confluent.kafka.schemaregistry.client.rest.entities.ProvenanceField;
import io.confluent.kafka.schemaregistry.client.rest.entities.ProvenanceVersion;
import io.confluent.kafka.schemaregistry.client.rest.entities.SchemaReference;
import io.confluent.kafka.schemaregistry.json.JsonSchema;
import io.confluent.kafka.schemaregistry.protobuf.ProtobufSchema;
import io.confluent.kafka.schemaregistry.type.logical.Rule;
import io.confluent.kafka.schemaregistry.type.logical.Schema;
import io.confluent.kafka.schemaregistry.type.logical.SchemaType;
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
    for (String type : List.of("o1", "o2", "o3")) {
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
  @EnumSource(value = SchemaType.class, names = {"AVRO", "JSON"})
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
  @EnumSource(value = SchemaType.class, names = {"AVRO", "JSON"})
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

  // A message's fields are written in the order of their numbers, which a position leaves.
  @Test
  void aPositionDoesNothingInProtobuf() {
    create("pp", SchemaType.PROTOBUF, "(a INT NOT NULL, b STRING, c BIGINT)");
    registry.alter("pp", new AddAttribute("d", "DOUBLE", Position.after("a")));
    assertThat(numbers(message("pp"))).isEqualTo("a=1 b=2 c=3 d=4");
    assertThat(names("pp")).containsExactly("a", "b", "c", "d");
    assertThat(registry.alter("pp", new MoveAttribute("c", Position.first())).edited())
        .isNull();
    assertThat(version("pp")).isEqualTo(2);
    assertThatThrownBy(() -> registry.alter("pp", new MoveAttribute("z", Position.first())))
        .isInstanceOf(TypeRefusedException.class).hasMessage("Type pp has no attribute z");
    assertThatThrownBy(() -> registry.alter("pp",
        new AddAttribute("e", "INT", Position.after("z"))))
        .isInstanceOf(TypeRefusedException.class).hasMessage("Type pp has no attribute z");
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
            + "FooRow, the type of foo in Protobuf message pn, would be renamed Foo2Row, as "
            + "another member or type of the message would have its name");
    // However the name comes to be taken: by a rename, a oneof's member, or a new attribute
    // beside the row of an attribute that is now an array of that row.
    for (AlterAction[] taking : List.of(
        new AlterAction[] {new RenameAttribute("id", "FooRow")},
        new AlterAction[] {new AddAttribute("u", "UNION(FooRow INT, c STRING)")},
        new AlterAction[] {new AddAttribute("FooRow", "UNION(a INT, b STRING)")},
        new AlterAction[] {
            new AlterAttributeType("foo", "ARRAY<STRUCT(x STRING) NOT NULL> NOT NULL"),
            new AddAttribute("FooRow", "INT")})) {
      assertThatThrownBy(() -> registry.alter("pn", taking))
          .isInstanceOf(TypeRefusedException.class).hasMessageContaining("would be renamed");
    }
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
      assertThat(numbers(message(type))).as(type).isEqualTo(cases[i][2]);
      // Whatever a later statement says, they are the fields they were.
      int before = version(type);
      registry.alter(type, new AddAttribute("zz", "INT"));
      assertThat(version(type)).as(type).isEqualTo(before + 1);
      assertThat(numbers(message(type))).as(type).startsWith((String) cases[i][2]);
      assertThat(registry.alter(type, new AddAttribute("q", "INT"),
          new DropAttribute("q", false)).edited()).as(type).isNull();
    }

    // Below the root too.
    create("un", SchemaType.PROTOBUF,
        "(id INT NOT NULL, s STRUCT(u UNION(a INT), v UNION(b INT)))");
    registry.alter("un", new AlterAttributeType("s",
        "STRUCT(v UNION(b INT, d STRING), u UNION(a INT, d INT))"));
    Descriptor row = message("un").findFieldByName("s").getMessageType();
    assertThat(numbers(row)).isEqualTo("a=1 u_d=4 b=2 d=3");
    registry.alter("un", new AddAttribute("zz", "INT"));
    assertThat(numbers(message("un").findFieldByName("s").getMessageType()))
        .isEqualTo(numbers(row));

    // The name is the member's: no attribute takes it, and its union renamed leaves it.
    create("ut", SchemaType.PROTOBUF, "(id INT NOT NULL, a INT, t UNION(a STRING, b STRING))");
    assertThatThrownBy(() -> registry.alter("ut", new AddAttribute("b", "INT")))
        .isInstanceOf(TypeRefusedException.class).hasMessage("Type ut cannot be changed so: no "
            + "PROTOBUF schema can be made of it: Union branch 'b' of 't' in ut keeps its field "
            + "name 'b', which is another field's name");
    registry.alter("ut", new RenameAttribute("t", "tt"), new DropAttribute("a", false));
    assertThat(numbers(message("ut"))).isEqualTo("t_a=3 b=4 id=1");
    assertThatThrownBy(() -> registry.alter("ut", new AddAttribute("t_a", "INT")))
        .isInstanceOf(TypeRefusedException.class).hasMessageContaining("keeps its field name");
    // A member dropped leaves its name to an attribute.
    registry.alter("ut", new AlterAttributeType("tt", "UNION(b STRING)"),
        new AddAttribute("t_a", "INT"));
    assertThat(numbers(message("ut"))).isEqualTo("b=4 id=1 t_a=5");
  }

  // An enum's name is one of the message's as a nested message's is.
  @Test
  void anAttributeNamedAsAnEnumOfItsProtobufTypeIsRefused() {
    create("en", SchemaType.PROTOBUF, "(id INT NOT NULL, e ENUM('A', 'B'), bar INT)");
    String name = message("en").findFieldByName("e").getEnumType().getName();
    for (String type : List.of("INT", "UNION(p INT, q STRING)")) {
      assertThatThrownBy(() -> registry.alter("en", new AddAttribute(name, type)))
          .isInstanceOf(TypeRefusedException.class).hasMessageContaining(name
              + ", the type of e in Protobuf message en, would be renamed");
    }
    assertThat(version("en")).isEqualTo(1);
    // And an enum may be what takes a message's name.
    create("em", SchemaType.PROTOBUF, "(id INT NOT NULL, foo STRUCT(x STRING))");
    assertThatThrownBy(() -> registry.alter("em", new AddAttribute("k",
        "ENUM('A', 'B') WITH ('protobuf.name' = 'FooRow')")))
        .isInstanceOf(TypeRefusedException.class).hasMessageContaining(
            "FooRow, the type of foo in Protobuf message em, would be renamed Foo2Row");
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
      assertThat(altered).as(c[0]).contains("\"nick\"");
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
        .hasMessageContaining("GeoRow, the type of geo in Protobuf message AddrRow, would be "
            + "renamed Geo2Row");
    assertThatThrownBy(() -> registry.alter("pr", new AlterAttributeType("addr",
        "STRUCT(street STRING, geo STRUCT(lat DOUBLE), u UNION(GeoRow INT, s STRING))")))
        .isInstanceOf(TypeRefusedException.class).hasMessageContaining("would be renamed");
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
    assertThat(version("same")).isEqualTo(format == SchemaType.PROTOBUF ? 1 : 2);
    assertThat(names("same")).containsExactly(format == SchemaType.PROTOBUF
        ? new String[] {"a", "b", "s"} : new String[] {"b", "a", "s"});
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
    // Each said as it was created, moved first and back after the attribute before it.
    String before = "a";
    for (Map.Entry<String, String> type : types.entrySet()) {
      assertThat(registry.alter("kept", new AlterAttributeType(type.getKey(), type.getValue()),
          new MoveAttribute(type.getKey(), Position.first()),
          new MoveAttribute(type.getKey(), Position.after(before))).edited())
          .as(type.getKey()).isNull();
      before = type.getKey();
    }
    // An attribute added and dropped, and a comment set and taken away.
    assertThat(registry.alter("kept", new AddAttribute("z", "INT", Position.first()),
        new DropAttribute("z", false)).edited()).isNull();
    assertThat(registry.alter("kept", new SetAttributeComment("a", "the key"),
        new SetAttributeComment("a", null)).edited()).isNull();
    if (format == SchemaType.PROTOBUF) {
      // A message's fields are written by their numbers, in whatever order they are said.
      assertThat(registry.alter("kept",
          new AlterAttributeType("p", "STRUCT(y STRING, x INT)")).edited()).isNull();
    }
    assertThat(version("kept")).isEqualTo(1);

    // What changes only a comment or a CHECK is still a version.
    registry.alter("kept", new AlterAttributeType("u", types.get("u")),
        new SetAttributeComment("a", "the key"));
    assertThat(version("kept")).isEqualTo(2);
    registry.alter("kept", new AddConstraint(new Check("ck", "a > 0", null)));
    assertThat(version("kept")).isEqualTo(3);
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
  }
}
