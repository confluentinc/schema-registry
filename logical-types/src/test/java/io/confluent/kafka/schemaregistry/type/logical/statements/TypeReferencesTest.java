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

import io.confluent.kafka.schemaregistry.ParsedSchema;
import io.confluent.kafka.schemaregistry.client.SchemaMetadata;
import io.confluent.kafka.schemaregistry.client.rest.entities.Config;
import io.confluent.kafka.schemaregistry.client.rest.entities.SchemaReference;
import io.confluent.kafka.schemaregistry.type.logical.LogicalType;
import io.confluent.kafka.schemaregistry.type.logical.SchemaType;
import io.confluent.kafka.schemaregistry.type.logical.statements.AlterAction.AddAttribute;
import io.confluent.kafka.schemaregistry.type.logical.statements.TypeScripts.BodyKind;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

class TypeReferencesTest {

  private final TestRegistry registry = new TestRegistry();

  @ParameterizedTest
  @EnumSource(SchemaType.class)
  void aNamedTypeBecomesAReferenceToItsLatestVersion(SchemaType format) {
    registry.create("com.acme.address", format, BodyKind.STRUCT,
        "(city STRING NOT NULL, zip INT)");
    registry.alter("com.acme.address", new AddAttribute("country", "STRING"));
    LogicalType order = registry.create("com.acme.order_value", format, BodyKind.STRUCT,
        "(id BIGINT NOT NULL, addr com.acme.address, tags ARRAY<STRING>)");

    assertThat(order.getReferences()).hasSize(1);
    assertThat(order.getReferences().get(0).getSubject()).isEqualTo("com.acme.address");
    assertThat(order.getReferences().get(0).getVersion()).isEqualTo(2);
    ParsedSchema stored = registry.parse(registry.latest("com.acme.order_value"));
    assertThat(stored.schemaType()).isEqualTo(format.name());
    assertThat(stored.references()).extracting(SchemaReference::getSubject)
        .containsExactly("com.acme.address");
  }

  @ParameterizedTest
  @EnumSource(SchemaType.class)
  void eachReferencedSchemaComesAfterThoseItRefersTo(SchemaType format) {
    registry.create("geo.point", format, BodyKind.STRUCT,
        "(lat DOUBLE NOT NULL, lon DOUBLE NOT NULL)");
    registry.create("geo.address", format, BodyKind.STRUCT, "(city STRING, location geo.point)");
    LogicalType customer = registry.create("customer", format, BodyKind.STRUCT,
        "(id BIGINT NOT NULL, home geo.address)");

    // The parsers read a schema's references in this order: a type after those it uses.
    assertThat(customer.getReferences()).extracting(SchemaReference::getSubject)
        .containsExactly("geo.address");
    assertThat(customer.getResolvedReferences().keySet())
        .containsExactly("geo.point", "geo.address");
    assertThat(TypeReferences.resolve(registry, "customer", customer.getReferences()).keySet())
        .containsExactly("geo.point", "geo.address");
  }

  @ParameterizedTest
  @EnumSource(value = SchemaType.class, names = {"AVRO", "PROTOBUF"})
  void aTypeWhoseSchemaDeclaresAnotherNameIsAliasedToIt(SchemaType format) {
    registry.create("addr", "com.acme.Address", format, BodyKind.STRUCT, "(city STRING)");
    LogicalType order = registry.create("order", format, BodyKind.STRUCT,
        "(id BIGINT NOT NULL, ship_to addr)");

    SchemaReference ref = order.getReferences().get(0);
    assertThat(ref.getName()).isEqualTo("addr");
    assertThat(ref.getSubject()).isEqualTo("addr");
    assertThat(TypeReferences.definedNames(registry, "order", ref))
        .containsExactly("addr", "com.acme.Address");
    assertThat(TypeReferences.definedNames(registry, "order",
        new SchemaReference("addr", "addr", 9))).isNull();
  }

  // A reference kept because its names could not be told is one thing; a registry that
  // could not be asked is another, and fails the statement.
  @Test
  void aLookupThatFailsIsNotAnUnknownName() {
    registry.create("addr", SchemaType.AVRO, BodyKind.STRUCT, "(city STRING)");
    registry.create("geo", SchemaType.AVRO, BodyKind.STRUCT, "(lat DOUBLE)");
    LogicalType order = registry.create("order", SchemaType.AVRO, BodyKind.STRUCT,
        "(id BIGINT NOT NULL, ship_to addr, at geo)");
    TypeLookup down = new TypeLookup() {
      @Override
      public SchemaMetadata latest(String subject) {
        return registry.latest(subject);
      }

      @Override
      public SchemaMetadata version(String subject, int version) {
        throw new IllegalStateException("The registry cannot be reached");
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

    assertThatThrownBy(() -> TypeReferences.definedNames(down, "order",
        order.getReferences().get(0)))
        .isInstanceOf(IllegalStateException.class).hasMessage("The registry cannot be reached");
    // As is a referred type a union's branches are judged by: it is not left out as one
    // that cannot be read.
    assertThatThrownBy(() -> TypeReferences.referredTypes(down, "order",
        order.getReferences(), Map.of()))
        .isInstanceOf(IllegalStateException.class).hasMessage("The registry cannot be reached");
    // Dropping the attribute of one type asks which references are still used.
    assertThatThrownBy(() -> TypeAlter.alter(down, "order",
        List.of(new AlterAction.DropAttribute("at", false)), new TypeAlter.Options()))
        .isInstanceOf(IllegalStateException.class).hasMessage("The registry cannot be reached");
  }

  // A schema holds one version of a name. The registry's resolver keeps the first it meets,
  // and what is checked here is what it will register.
  @Test
  void aNameKeepsTheFirstVersionBoundToIt() {
    registry.create("point", SchemaType.AVRO, BodyKind.STRUCT, "(x INT)");
    registry.create("address", SchemaType.AVRO, BodyKind.STRUCT, "(city STRING, at point)");
    registry.alter("point", new AddAttribute("y", "INT"));
    String v1 = registry.version("point", 1).getSchema();
    String v2 = registry.version("point", 2).getSchema();

    // address refers to point's first version, and is named first.
    LogicalType first = TypeReferences.parse(registry, SchemaType.AVRO,
        aliases -> TypeScripts.createScript("t", BodyKind.STRUCT, "(a address, p point)",
            Map.of(), aliases), "t", Map.of());
    assertThat(first.getResolvedReferences().get("point")).isEqualTo(v1);
    assertThat(first.getResolvedReferences()).isEqualTo(
        TypeReferences.resolve(registry, "t", first.getReferences()));

    // point is named first, at its latest version.
    LogicalType second = TypeReferences.parse(registry, SchemaType.AVRO,
        aliases -> TypeScripts.createScript("t", BodyKind.STRUCT, "(p point, a address)",
            Map.of(), aliases), "t", Map.of());
    assertThat(second.getResolvedReferences().get("point")).isEqualTo(v2);
    assertThat(second.getResolvedReferences()).isEqualTo(
        TypeReferences.resolve(registry, "t", second.getReferences()));
  }

  @Test
  void anUnknownTypeOrOneOfAnotherFormatIsRefused() {
    assertThatThrownBy(() -> registry.create("order", SchemaType.AVRO, BodyKind.STRUCT,
        "(ship_to nope)"))
        .isInstanceOf(TypeRefusedException.class).hasMessage("Unknown type nope");

    registry.create("addr", SchemaType.AVRO, BodyKind.STRUCT, "(city STRING)");
    assertThatThrownBy(() -> registry.create("order", SchemaType.PROTOBUF, BodyKind.STRUCT,
        "(ship_to addr)"))
        .isInstanceOf(TypeRefusedException.class)
        .hasMessage("Type addr is AVRO, so it cannot be used in a PROTOBUF type");
  }

  @Test
  void aJsonReferenceNameNamesOneType() {
    registry.create("a", SchemaType.JSON, BodyKind.STRUCT, "(x INT)");
    registry.create("b", SchemaType.JSON, BodyKind.STRUCT, "(y INT)");
    // Each takes the name a reference to it was stored with; here both were stored as one.
    assertThatThrownBy(() -> TypeReferences.parse(registry, SchemaType.JSON,
        aliases -> TypeScripts.createScript("both", BodyKind.STRUCT, "(p a, q b)", Map.of(),
            aliases), "both", Map.of("a", "shared", "b", "shared")))
        .isInstanceOf(TypeRefusedException.class)
        .hasMessage("Types a and b would both be referred to as shared, which can name only "
            + "one of them");
  }

  @Test
  void aNameWithoutAContextIsOfTheReferrersContext() {
    assertThat(TypeReferences.referenced("order", "address")).isEqualTo("address");
    assertThat(TypeReferences.referenced(":.prod:order", "address"))
        .isEqualTo(":.prod:address");
    assertThat(TypeReferences.referenced(":.prod:order", ":.dev:address"))
        .isEqualTo(":.dev:address");
    assertThat(TypeReferences.referenced(null, "address")).isEqualTo("address");
  }

  @Test
  void aNameWithAnEmptyPartCannotNameATypeInDdl() {
    assertThat(TypeReferences.nameable("com.acme.address")).isTrue();
    assertThat(TypeReferences.nameable("./address")).isFalse();
    assertThat(TypeReferences.nameable("com..address")).isFalse();
  }
}
