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
import io.confluent.kafka.schemaregistry.avro.AvroSchema;
import io.confluent.kafka.schemaregistry.protobuf.ProtobufSchema;
import io.confluent.kafka.schemaregistry.type.logical.LogicalTypeToDdlConverter;
import io.confluent.kafka.schemaregistry.type.logical.Schema;
import io.confluent.kafka.schemaregistry.type.logical.SchemaType;
import io.confluent.kafka.schemaregistry.type.logical.statements.AlterAction.AddAttribute;
import io.confluent.kafka.schemaregistry.type.logical.statements.AlterAction.DropAttribute;
import io.confluent.kafka.schemaregistry.type.logical.statements.AlterAction.RenameAttribute;
import io.confluent.kafka.schemaregistry.type.logical.statements.TypeScripts.BodyKind;
import java.util.LinkedHashMap;
import java.util.Map;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

class TypeCreateTest {

  private final TestRegistry registry = new TestRegistry();

  // The statement's result, not registered.
  private TypeAlter.Result struct(String subject, String body, Map<String, String> with) {
    return TypeCreate.create(registry, subject, subject, BodyKind.STRUCT, body, "", with, null);
  }

  private TypeAlter.Result create(String subject, SchemaType format, String body) {
    return registry.register(subject, struct(subject, body, Map.of("format", format.name())));
  }

  @ParameterizedTest
  @EnumSource(SchemaType.class)
  void aNewTypeIsItsSubjectsFirstVersion(SchemaType format) {
    TypeAlter.Result result = create("t", format, "(id BIGINT NOT NULL, name STRING)");

    assertThat(result.format()).isEqualTo(format);
    assertThat(result.base()).isNull();
    assertThat(result.version()).isEqualTo(1);
    assertThat(result.policy()).isNull();
    assertThat(result.request().getVersion()).isEqualTo(1);
    assertThat(registry.latest("t").getSchemaType()).isEqualTo(format.name());
    assertThat(registry.struct("t").getFields()).extracting(Schema.Field::getName)
        .containsExactly("id", "name");
  }

  @Test
  void theWithGivesTheFormatThePolicyAndTheTypesProperties() {
    assertThat(struct("a", "(id INT)", Map.of()).format()).isEqualTo(SchemaType.AVRO);

    Map<String, String> with = new LinkedHashMap<>();
    with.put("Format", "protobuf");
    with.put("Compatibility.Policy", "strict");
    with.put("owner", "sales");
    TypeAlter.Result result = struct("p", "(id INT)", with);
    assertThat(result.format()).isEqualTo(SchemaType.PROTOBUF);
    assertThat(result.policy()).isEqualTo("STRICT");
    assertThat(LogicalTypeToDdlConverter.toDdl(result.edited())).contains("'owner' = 'sales'")
        .doesNotContain("rotobuf").doesNotContain("trict");
    // The statement's WITH is the caller's, and is left as it was.
    assertThat(with).hasSize(3);

    assertThatThrownBy(() -> struct("x", "(id INT)", Map.of("format", "XML")))
        .isInstanceOf(TypeRefusedException.class).hasMessageContaining("Unknown format 'XML'");
    assertThatThrownBy(() -> struct("x", "(id INT)", Map.of("compatibility.policy", "LOOSE")))
        .isInstanceOf(TypeRefusedException.class)
        .hasMessageContaining("Unknown compatibility.policy 'LOOSE'");
  }

  @Test
  void aTypeWhoseWithNamesNoPolicyGetsTheCallers() {
    assertThat(TypeCreate.create(registry, "t", "t", BodyKind.STRUCT, "(id INT)", "", Map.of(),
        "LOGICAL").policy()).isEqualTo("LOGICAL");
    assertThat(TypeCreate.create(registry, "t", "t", BodyKind.STRUCT, "(id INT)", "",
        Map.of("compatibility.policy", "LENIENT"), "LOGICAL").policy()).isEqualTo("LENIENT");
  }

  @Test
  void aDottedNameIsTheTypesNamespaceAndName() {
    registry.register(":.prod:Parcel", TypeCreate.create(registry, ":.prod:Parcel",
        "com.acme.Parcel", BodyKind.STRUCT, "(id INT)", "COMMENT 'a parcel'", Map.of(), null));

    AvroSchema stored = (AvroSchema) registry.schema(":.prod:Parcel");
    assertThat(stored.rawSchema().getFullName()).isEqualTo("com.acme.Parcel");
    assertThat(stored.rawSchema().getDoc()).isEqualTo("a parcel");
  }

  @ParameterizedTest
  @EnumSource(SchemaType.class)
  void aTypeThatIsOneValueOrAnEnumIsCreated(SchemaType format) {
    Map<String, String> with = Map.of("format", format.name());
    registry.register("id", TypeCreate.create(registry, "id", "id", BodyKind.OTHER,
        "BIGINT NOT NULL", "", with, null));
    TypeAlter.Result color = TypeCreate.create(registry, "color", "color", BodyKind.ENUM,
        "('RED', 'GREEN')", "", with, null);

    assertThat(registry.versions("id", false)).containsExactly(1);
    assertThat(LogicalTypeToDdlConverter.toDdl(color.edited())).contains("ENUM color");
    // The mock registry takes no Protobuf file whose root is an enum.
    if (format != SchemaType.PROTOBUF) {
      registry.register("color", color);
      assertThat(registry.versions("color", false)).containsExactly(1);
    }
  }

  @ParameterizedTest
  @EnumSource(SchemaType.class)
  void aTypeRefersToTheLatestVersionOfTheTypesItNames(SchemaType format) {
    create("address", format, "(city STRING)");
    registry.alter("address", new AddAttribute("zip", "INT"));

    TypeAlter.Result result = create("o", format, "(id INT NOT NULL, addr address)");

    assertThat(result.edited().getReferences()).hasSize(1);
    assertThat(result.edited().getReferences().get(0).getSubject()).isEqualTo("address");
    assertThat(result.edited().getReferences().get(0).getVersion()).isEqualTo(2);
    assertThat(registry.latest("o").getReferences()).hasSize(1);
  }

  @Test
  void aTypeWithALiveVersionAlreadyExists() {
    create("t", SchemaType.AVRO, "(id INT)");

    assertThatThrownBy(() -> struct("t", "(id INT, n INT)", Map.of()))
        .isInstanceOf(TypeRefusedException.class).hasMessage("Type t already exists");
  }

  @ParameterizedTest
  @EnumSource(SchemaType.class)
  void aDroppedTypeIsCreatedAgainAsItsNextVersion(SchemaType format) {
    create("t", format, "(a INT)");
    registry.alter("t", new AddAttribute("b", "INT"));
    registry.drop("t");

    TypeAlter.Result result = create("t", format, "(a INT, b INT, c INT)");

    assertThat(result.base().getVersion()).isEqualTo(2);
    assertThat(result.version()).isEqualTo(3);
    assertThat(registry.versions("t", false)).containsExactly(3);
  }

  @Test
  void aDroppedTypeIsCreatedAgainOnlyInItsFormat() {
    create("t", SchemaType.AVRO, "(a INT)");
    registry.drop("t");

    assertThatThrownBy(() -> struct("t", "(a INT)", Map.of("format", "PROTOBUF")))
        .isInstanceOf(TypeRefusedException.class).hasMessage("Type t was dropped as AVRO, and "
            + "keeps the versions it had; it can be created again only as AVRO");
  }

  // Created again in another order, a Protobuf type keeps its dropped versions' numbers by name,
  // at every level, and a new attribute is numbered above every number used.
  @Test
  void aProtobufTypeCreatedAgainKeepsItsNumbers() {
    create("p", SchemaType.PROTOBUF, "(a INT, b INT, s STRUCT(x INT, y STRING), c STRING)");
    registry.drop("p");

    create("p", SchemaType.PROTOBUF, "(b INT, s STRUCT(y STRING, z INT, x INT), a INT, d STRING)");

    Descriptor root = ((ProtobufSchema) registry.schema("p")).toDescriptor();
    Map<String, Integer> numbers = new LinkedHashMap<>();
    root.getFields().forEach(f -> numbers.put(f.getName(), f.getNumber()));
    assertThat(numbers).isEqualTo(Map.of("a", 1, "b", 2, "s", 3, "d", 5));
    Map<String, Integer> nested = new LinkedHashMap<>();
    root.findFieldByName("s").getMessageType().getFields()
        .forEach(f -> nested.put(f.getName(), f.getNumber()));
    assertThat(nested).isEqualTo(Map.of("x", 1, "y", 2, "z", 3));
  }

  // Avro reads the dropped versions' data by the default of an attribute they lack.
  @Test
  void anAvroTypeCreatedAgainDefaultsTheNullableAttributesItAdds() {
    create("v", SchemaType.AVRO, "(a INT)");
    registry.drop("v");

    create("v", SchemaType.AVRO, "(a INT, b STRING)");

    org.apache.avro.Schema stored = ((AvroSchema) registry.schema("v")).rawSchema();
    assertThat(stored.getField("a").hasDefaultValue()).isFalse();
    assertThat(stored.getField("b").hasDefaultValue()).isTrue();
  }

  // The registry checks dropped versions only under LOGICAL, so the check is made here.
  @ParameterizedTest
  @EnumSource(SchemaType.class)
  void aTypeCreatedAgainIncompatiblyWithItsLastVersionIsRefused(SchemaType format) {
    create("ci", format, "(id INT NOT NULL, q INT)");
    registry.drop("ci");

    assertThatThrownBy(() -> create("ci", format, "(id INT NOT NULL, q STRING)"))
        .isInstanceOf(TypeRefusedException.class).hasMessageStartingWith("Type ci cannot be "
            + "created again so: it is incompatible with version 1, whose records it reads, at "
            + "BACKWARD compatibility");
    assertThat(registry.versions("ci", true)).containsExactly(1);

    create("ci", format, "(id INT NOT NULL, q BIGINT, r STRING)");
    assertThat(registry.versions("ci", true)).containsExactly(1, 2);
  }

  // A name its referenced type dropped and added again would pair with the old attribute's
  // column in Avro and JSON; Protobuf pairs by number, and the new attribute has a new one.
  @ParameterizedTest
  @EnumSource(SchemaType.class)
  void aTypeCreatedAgainOverANameAddedAgainIsRefused(SchemaType format) {
    registry.create("address", "com.acme.address", format, BodyKind.STRUCT,
        "(city STRING, zip INT)");
    create("o", format, "(id INT NOT NULL, addr address)");
    registry.drop("o");
    registry.alter("address", format == SchemaType.AVRO
        ? new RenameAttribute("city", "town") : new DropAttribute("city", false));
    registry.alter("address", new AddAttribute("city", "STRING"));

    if (format == SchemaType.PROTOBUF) {
      create("o", format, "(id INT NOT NULL, addr address)");
      assertThat(registry.versions("o", true)).containsExactly(1, 2);
      return;
    }
    assertThatThrownBy(() -> create("o", format, "(id INT NOT NULL, addr address)"))
        .isInstanceOf(TypeRefusedException.class).hasMessageStartingWith("Type o cannot be "
            + "created again referring to version 3 of type address: Schema Registry's "
            + "provenance would pair its attributes otherwise than versions 1 to 3 of address "
            + "do");
    assertThat(registry.versions("o", true)).containsExactly(1);
  }

  // Its dropped version referred to a version of a type that is soft-deleted since, so the new
  // one would refer to an earlier one.
  @ParameterizedTest
  @EnumSource(SchemaType.class)
  void aTypeCreatedAgainReferringToAnEarlierVersionIsRefused(SchemaType format) {
    create("address", format, "(city STRING)");
    registry.alter("address", new AddAttribute("zip", "INT"));
    create("o", format, "(id INT NOT NULL, addr address)");
    registry.drop("o");
    registry.softDelete("address", 2);

    assertThatThrownBy(() -> create("o", format, "(id INT NOT NULL, addr address)"))
        .isInstanceOf(TypeRefusedException.class).hasMessageStartingWith("Type o cannot be "
            + "created again so: its reference to type address would move back from version 2 "
            + "to version 1, the latest live one, because version 2 is soft-deleted");

    // Under LOGICAL the registry checks soft-deleted versions itself.
    assertThat(struct("o", "(id INT NOT NULL, addr address)", Map.of("format", format.name(),
        "compatibility.policy", "LOGICAL")).edited().getReferences().get(0).getVersion())
        .isEqualTo(1);
  }
}
