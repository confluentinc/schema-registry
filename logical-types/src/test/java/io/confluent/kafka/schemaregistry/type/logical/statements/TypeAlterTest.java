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
import com.google.protobuf.Descriptors.FieldDescriptor;
import io.confluent.kafka.schemaregistry.ParsedSchema;
import io.confluent.kafka.schemaregistry.avro.AvroSchema;
import io.confluent.kafka.schemaregistry.client.rest.entities.ProvenanceField;
import io.confluent.kafka.schemaregistry.client.rest.entities.ProvenanceVersion;
import io.confluent.kafka.schemaregistry.client.rest.entities.SchemaReference;
import io.confluent.kafka.schemaregistry.protobuf.ProtobufSchema;
import io.confluent.kafka.schemaregistry.type.logical.Rule;
import io.confluent.kafka.schemaregistry.type.logical.Schema;
import io.confluent.kafka.schemaregistry.type.logical.SchemaType;
import io.confluent.kafka.schemaregistry.type.logical.statements.AlterAction.AddAttribute;
import io.confluent.kafka.schemaregistry.type.logical.statements.AlterAction.AddConstraint;
import io.confluent.kafka.schemaregistry.type.logical.statements.AlterAction.AddValue;
import io.confluent.kafka.schemaregistry.type.logical.statements.AlterAction.AlterAttributeType;
import io.confluent.kafka.schemaregistry.type.logical.statements.AlterAction.Check;
import io.confluent.kafka.schemaregistry.type.logical.statements.AlterAction.DropAttribute;
import io.confluent.kafka.schemaregistry.type.logical.statements.AlterAction.DropConstraint;
import io.confluent.kafka.schemaregistry.type.logical.statements.AlterAction.MoveAttribute;
import io.confluent.kafka.schemaregistry.type.logical.statements.AlterAction.Position;
import io.confluent.kafka.schemaregistry.type.logical.statements.AlterAction.RenameAttribute;
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
    create("pn", SchemaType.PROTOBUF, "(id INT NOT NULL, foo STRUCT(x STRING))");
    assertThatThrownBy(() -> registry.alter("pn", new AddAttribute("FooRow", "INT")))
        .isInstanceOf(TypeRefusedException.class).hasMessage("Type pn cannot have an attribute "
            + "named FooRow: its Protobuf message declares a message or enum of that name");
    assertThatThrownBy(() -> registry.alter("pn", new RenameAttribute("id", "FooRow")))
        .isInstanceOf(TypeRefusedException.class)
        .hasMessageStartingWith("Type pn cannot have an attribute named FooRow");
    // One a pin keeps as Flink named it.
    registry.client.register("pm", new ProtobufSchema("syntax = \"proto3\";\n"
        + "message Record {\n"
        + "  optional addr_Row addr = 1;\n"
        + "  message addr_Row { optional string city = 1; }\n"
        + "}\n"));
    assertThatThrownBy(() -> registry.alter("pm", new AddAttribute("addr_Row", "INT")))
        .isInstanceOf(TypeRefusedException.class)
        .hasMessageStartingWith("Type pm cannot have an attribute named addr_Row");
    assertThat(version("pn")).isEqualTo(1);
    assertThat(version("pm")).isEqualTo(1);
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
  }
}
