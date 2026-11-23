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
import io.confluent.kafka.schemaregistry.client.rest.entities.Config;
import io.confluent.kafka.schemaregistry.protobuf.ProtobufSchema;
import io.confluent.kafka.schemaregistry.type.logical.LogicalTypeToDdlConverter;
import io.confluent.kafka.schemaregistry.type.logical.Schema;
import io.confluent.kafka.schemaregistry.type.logical.SchemaType;
import io.confluent.kafka.schemaregistry.type.logical.statements.AlterAction.AddAttribute;
import io.confluent.kafka.schemaregistry.type.logical.statements.AlterAction.DropAttribute;
import io.confluent.kafka.schemaregistry.type.logical.statements.AlterAction.RenameAttribute;
import io.confluent.kafka.schemaregistry.type.logical.statements.TypeScripts.BodyKind;
import java.lang.reflect.Proxy;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
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

  // Avro alone would not tell: it splits a dotted record name itself.
  @ParameterizedTest
  @EnumSource(SchemaType.class)
  void aDottedNameIsTheTypesNamespaceAndName(SchemaType format) {
    Map<String, String> with = Map.of("format", format.name());
    registry.register(":.prod:Parcel", TypeCreate.create(registry, ":.prod:Parcel",
        "com.acme.Parcel", BodyKind.STRUCT, "(id INT)", "COMMENT 'a parcel'", with, null));

    String stored = registry.schema(":.prod:Parcel").canonicalString();
    switch (format) {
      case AVRO:
        assertThat(stored).contains("\"name\":\"Parcel\"", "\"namespace\":\"com.acme\"",
            "\"doc\":\"a parcel\"");
        break;
      case PROTOBUF:
        assertThat(stored).contains("package com.acme;", "message Parcel {");
        break;
      default:
        assertThat(stored).contains("\"title\":\"Parcel\"",
            "\"confluent:namespace\":\"com.acme\"");
    }
    // A type that is one value declares no name, so its dotted name is no namespace.
    assertThat(LogicalTypeToDdlConverter.toDdl(TypeCreate.create(registry, "com.acme.id",
        "com.acme.id", BodyKind.OTHER, "BIGINT", "", with, null).edited()))
        .doesNotContain("NAMESPACE");
  }

  // A version registered between the two reads then fails to register, by its number.
  @Test
  void theSubjectsVersionsAreReadBeforeItsLatestVersion() {
    List<String> reads = new ArrayList<>();
    TypeLookup recording = (TypeLookup) Proxy.newProxyInstance(
        TypeLookup.class.getClassLoader(), new Class<?>[] {TypeLookup.class},
        (proxy, method, args) -> {
          reads.add(method.getName());
          return method.invoke(registry, args);
        });

    TypeCreate.create(recording, "t", "t", BodyKind.STRUCT, "(id INT)", "", Map.of(), null);

    assertThat(reads).startsWith("versions", "latest");
  }

  @ParameterizedTest
  @EnumSource(SchemaType.class)
  void aTypeThatIsOneValueOrAnEnumIsCreated(SchemaType format) {
    Map<String, String> with = Map.of("format", format.name());
    registry.register("id", TypeCreate.create(registry, "id", "id", BodyKind.OTHER,
        "BIGINT NOT NULL", "", with, null));

    assertThat(registry.versions("id", false)).containsExactly(1);
    // The root of a Protobuf schema is a message, so an enum is no type by itself there.
    if (format == SchemaType.PROTOBUF) {
      assertThatThrownBy(() -> TypeCreate.create(registry, "color", "color", BodyKind.ENUM,
          "('RED', 'GREEN')", "", with, null)).isInstanceOf(TypeRefusedException.class)
          .hasMessage("A PROTOBUF type cannot be an ENUM: the root of a Protobuf schema is a "
              + "message");
      return;
    }
    registry.register("color", TypeCreate.create(registry, "color", "color", BodyKind.ENUM,
        "('RED', 'GREEN')", "", with, null));
    assertThat(registry.versions("color", false)).containsExactly(1);
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
    assertThat(result.request().getVersion()).isEqualTo(3);
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

  // A statement writes a proto3 file with no reserved numbers and plain integers. Over a file
  // written by hand with more, a new attribute could take a reserved number, as c would take 3.
  @Test
  void aProtobufTypeCreatedAgainOverWhatAStatementCannotCarryIsRefused() throws Exception {
    String proto3 = "syntax = \"proto3\";\n";
    registry.client.register("r", new ProtobufSchema(proto3
        + "message r {\n  reserved 3;\n  int32 a = 1;\n  string b = 2;\n}\n"));
    registry.client.register("q", new ProtobufSchema("syntax = \"proto2\";\nmessage q {\n"
        + "  required int32 a = 1;\n  optional string b = 2 [default = \"x\"];\n}\n"));
    registry.client.register("s", new ProtobufSchema(proto3
        + "message s {\n  sint32 a = 1;\n}\n"));
    registry.client.register("plain", new ProtobufSchema(proto3
        + "message plain {\n  int32 a = 1;\n  string b = 2;\n}\n"));
    // Several messages, at a compatibility level that checks nothing.
    registry.client.register("m", new ProtobufSchema(proto3
        + "message first {\n  int32 a = 1;\n}\nmessage second {\n  string b = 1;\n}\n"));
    registry.client.updateConfig("m", new Config("NONE"));
    for (String subject : new String[] {"r", "q", "s", "plain", "m"}) {
      registry.drop(subject);
    }

    assertThatThrownBy(() -> create("r", SchemaType.PROTOBUF, "(a INT, b STRING, c STRING)"))
        .isInstanceOf(TypeRefusedException.class).hasMessage("Type r cannot be created again: "
            + "it reserves field numbers or names, which would be dropped, and a new field "
            + "could then take one. Register its new version as a Protobuf schema");
    assertThatThrownBy(() -> create("q", SchemaType.PROTOBUF, "(a INT NOT NULL, b STRING)"))
        .isInstanceOf(TypeRefusedException.class)
        .hasMessageStartingWith("Type q cannot be created again: it is not a proto3 file");
    assertThatThrownBy(() -> create("s", SchemaType.PROTOBUF, "(a INT)"))
        .isInstanceOf(TypeRefusedException.class).hasMessageStartingWith("Type s cannot be "
            + "created again: field a of message s is a sint32, which would be written as int32");
    assertThatThrownBy(() -> create("m", SchemaType.PROTOBUF, "(a INT)"))
        .isInstanceOf(TypeRefusedException.class).hasMessageStartingWith("Type m cannot be "
            + "created again: it is several Protobuf messages, and would be registered as one");
    assertThat(registry.versions("r", true)).containsExactly(1);
    assertThat(registry.versions("m", true)).containsExactly(1);

    // A file written by hand with none of these is created again as any other.
    create("plain", SchemaType.PROTOBUF, "(a INT, b STRING, c STRING)");
    Descriptor plain = ((ProtobufSchema) registry.schema("plain")).toDescriptor();
    assertThat(plain.findFieldByName("c").getNumber()).isEqualTo(3);
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
