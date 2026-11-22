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

package io.confluent.kafka.schemaregistry.type.logical.alter;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.confluent.kafka.schemaregistry.ParsedSchema;
import io.confluent.kafka.schemaregistry.client.rest.entities.SchemaReference;
import io.confluent.kafka.schemaregistry.protobuf.ProtobufSchema;
import io.confluent.kafka.schemaregistry.type.logical.Rule;
import io.confluent.kafka.schemaregistry.type.logical.Schema;
import io.confluent.kafka.schemaregistry.type.logical.SchemaType;
import io.confluent.kafka.schemaregistry.type.logical.alter.AlterAction.AddAttribute;
import io.confluent.kafka.schemaregistry.type.logical.alter.AlterAction.AddConstraint;
import io.confluent.kafka.schemaregistry.type.logical.alter.AlterAction.AddValue;
import io.confluent.kafka.schemaregistry.type.logical.alter.AlterAction.AlterAttributeType;
import io.confluent.kafka.schemaregistry.type.logical.alter.AlterAction.Check;
import io.confluent.kafka.schemaregistry.type.logical.alter.AlterAction.DropAttribute;
import io.confluent.kafka.schemaregistry.type.logical.alter.AlterAction.DropConstraint;
import io.confluent.kafka.schemaregistry.type.logical.alter.AlterAction.RenameAttribute;
import io.confluent.kafka.schemaregistry.type.logical.alter.AlterAction.SetDataType;
import io.confluent.kafka.schemaregistry.type.logical.alter.AlterAction.SetWith;
import io.confluent.kafka.schemaregistry.type.logical.alter.TypeScripts.BodyKind;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
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
}
