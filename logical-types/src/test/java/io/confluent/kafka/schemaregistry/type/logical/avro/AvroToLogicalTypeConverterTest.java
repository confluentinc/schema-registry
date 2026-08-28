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

package io.confluent.kafka.schemaregistry.type.logical.avro;

import io.confluent.kafka.schemaregistry.avro.AvroSchema;
import io.confluent.kafka.schemaregistry.type.logical.LogicalTypeToDdlConverter;
import io.confluent.kafka.schemaregistry.type.logical.Schema;
import io.confluent.kafka.schemaregistry.type.logical.ValidationException;
import org.apache.avro.SchemaBuilder;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class AvroToLogicalTypeConverterTest {

  @Test
  void testTypeMappings() {
    for (CommonMappings.TypeMapping mapping :
        CommonMappings.get().collect(Collectors.toList())) {
      Schema result = AvroToLogicalTypeConverter.toRootSchema(
          new AvroSchema(mapping.getAvroSchema()));
      assertEquals(mapping.getLogicalType(), result, "Failed for: " + mapping);
    }
  }

  @Test
  void testVariantParsedFromStringMapsToVariant() {
    // Production read path: a schema fetched from Schema Registry arrives as a JSON
    // string. kafka-avro-types registers no "variant" logical-type factory, so after
    // parsing getLogicalType() is null and detection must be structural (a record
    // confluent.type.Variant with two bytes fields metadata/value).
    String withLogicalTypeProp =
        "{\"type\":\"record\",\"name\":\"Variant\",\"namespace\":\"confluent.type\","
            + "\"fields\":[{\"name\":\"metadata\",\"type\":\"bytes\"},"
            + "{\"name\":\"value\",\"type\":\"bytes\"}],\"logicalType\":\"variant\"}";
    Schema withProp =
        AvroToLogicalTypeConverter.toRootSchema(new AvroSchema(withLogicalTypeProp));
    assertEquals(Schema.Type.VARIANT, withProp.getType());
    assertFalse(withProp.isNullable());

    // Avro parsing canonical form strips the logicalType property entirely;
    // structural detection must still recognize the record as VARIANT.
    String noProp =
        "{\"type\":\"record\",\"name\":\"Variant\",\"namespace\":\"confluent.type\","
            + "\"fields\":[{\"name\":\"metadata\",\"type\":\"bytes\"},"
            + "{\"name\":\"value\",\"type\":\"bytes\"}]}";
    Schema withoutProp = AvroToLogicalTypeConverter.toRootSchema(new AvroSchema(noProp));
    assertEquals(Schema.Type.VARIANT, withoutProp.getType());

    // Nullable variant: union [null, Variant] parsed from a string stays nullable VARIANT.
    String nullableVariant =
        "[\"null\",{\"type\":\"record\",\"name\":\"Variant\",\"namespace\":\"confluent.type\","
            + "\"fields\":[{\"name\":\"metadata\",\"type\":\"bytes\"},"
            + "{\"name\":\"value\",\"type\":\"bytes\"}],\"logicalType\":\"variant\"}]";
    Schema nullable = AvroToLogicalTypeConverter.toRootSchema(new AvroSchema(nullableVariant));
    assertEquals(Schema.Type.VARIANT, nullable.getType());
    assertTrue(nullable.isNullable());
  }

  @Test
  void testRecordWithCycles() {
    // Build a truly cyclic schema. Cycles round-trip cleanly through the LT
    // model now that every unmarked Avro record becomes a NAMED_TYPE_REF +
    // entry in localNamedTypes — the self-reference resolves via the named
    // type rather than infinite recursion.
    org.apache.avro.Schema recordSchema =
        org.apache.avro.Schema.createRecord("CyclicRecord", null, null, false);
    recordSchema.setFields(
        java.util.Collections.singletonList(
            new org.apache.avro.Schema.Field("self",
                org.apache.avro.Schema.createUnion(
                    org.apache.avro.Schema.create(org.apache.avro.Schema.Type.NULL),
                    recordSchema),
                null, null)));

    io.confluent.kafka.schemaregistry.type.logical.LogicalType lt =
        AvroToLogicalTypeConverter.toLogicalType(new AvroSchema(recordSchema));
    assertEquals(Schema.Type.NAMED_TYPE_REF, lt.getRootSchema().getType());
    assertEquals("CyclicRecord", lt.getRootSchema().getQualifiedName());
    Schema cyclic = lt.getNamedTypes().get("CyclicRecord");
    assertEquals(Schema.Type.STRUCT, cyclic.getType());
    assertEquals(1, cyclic.getFields().size());
    Schema selfField = cyclic.getFields().get(0).getSchema();
    assertEquals(Schema.Type.NAMED_TYPE_REF, selfField.getType());
    assertEquals("CyclicRecord", selfField.getQualifiedName());
    assertTrue(selfField.isNullable());
  }

  @Test
  void testEnumConversion() {
    // A leaf named enum root is unwrapped to a bare ENUM body carrying its name on
    // LogicalType.name (the canonical named-root shape), not a NAMED_TYPE_REF into namedTypes.
    org.apache.avro.Schema enumSchema = SchemaBuilder.enumeration("Color")
        .symbols("RED", "GREEN", "BLUE");
    io.confluent.kafka.schemaregistry.type.logical.LogicalType lt =
        AvroToLogicalTypeConverter.toLogicalType(new AvroSchema(enumSchema));
    assertEquals(Schema.Type.ENUM, lt.getRootSchema().getType());
    assertEquals("Color", lt.getName());
    assertTrue(lt.getNamedTypes().isEmpty());
    Schema named = lt.getRootSchema();
    assertEquals(3, named.getEnumValues().size());
    assertEquals("RED", named.getEnumValues().get(0).getSymbol());
    assertEquals("GREEN", named.getEnumValues().get(1).getSymbol());
    assertEquals("BLUE", named.getEnumValues().get(2).getSymbol());
  }

  @Test
  void namedLeafRootNameRoundTripsThroughAvro() {
    // Avro -> LT -> Avro: a non-recursive named record root unwraps to a bare STRUCT carrying its
    // name on read, and the writer re-emits it under the same record name (identity preserved).
    String json = "{\"type\":\"record\",\"name\":\"Order\",\"fields\":["
        + "{\"name\":\"id\",\"type\":\"int\"}]}";
    io.confluent.kafka.schemaregistry.type.logical.LogicalType lt =
        AvroToLogicalTypeConverter.toLogicalType(new AvroSchema(json));
    assertEquals(Schema.Type.STRUCT, lt.getRootSchema().getType());
    assertEquals("Order", lt.getName());
    AvroSchema out = LogicalTypeToAvroConverter.fromLogicalType(lt, "IGNORED");
    assertEquals("Order", out.rawSchema().getName());
    // A named root is a genuine named record, NOT marked logical.anonymous, so its name survives
    // a further Avro -> LT read-back (rather than being discarded as a synthetic row wrapper).
    assertNull(out.rawSchema().getProp("logical.anonymous"));
    assertEquals("Order", AvroToLogicalTypeConverter.toLogicalType(out).getName());

    // The DDL projection declares the root as a named STRUCT; a single unreferenced root needs no
    // explicit trailing TYPE (first-wins inference recovers it).
    String ddl = LogicalTypeToDdlConverter.toDdl(lt);
    assertThat(ddl).contains("STRUCT Order (");
    assertThat(ddl).doesNotContain("TYPE");
  }

  @Test
  void nullableNamespacedRootInfersNamespace() {
    // A natural nullable Avro root is a top-level ["null", record] union; the namespace fallback
    // must still infer the record's namespace (matching the NOT NULL case and the Proto/JSON
    // readers) rather than seeing UNION and giving up.
    String json = "[\"null\",{\"type\":\"record\",\"name\":\"Order\",\"namespace\":\"acme\","
        + "\"fields\":[{\"name\":\"id\",\"type\":\"int\"}]}]";
    io.confluent.kafka.schemaregistry.type.logical.LogicalType lt =
        AvroToLogicalTypeConverter.toLogicalType(new AvroSchema(json));
    assertEquals("acme", lt.getNamespace());
    assertEquals(Schema.Type.STRUCT, lt.getRootSchema().getType());
    assertEquals("Order", lt.getName());
    assertTrue(lt.getRootSchema().isNullable());
  }

  @Test
  void anonymousRootDoesNotInferNamespace() {
    // An anonymous root (logical.anonymous=true) named after a qualified rowName must NOT contribute
    // a document namespace — its name/namespace are synthetic, so inferring one would add a spurious
    // NAMESPACE on read-back.
    String json = "{\"type\":\"record\",\"name\":\"Row\",\"namespace\":\"acme\","
        + "\"logical.anonymous\":\"true\","
        + "\"fields\":[{\"name\":\"id\",\"type\":\"int\"}]}";
    io.confluent.kafka.schemaregistry.type.logical.LogicalType lt =
        AvroToLogicalTypeConverter.toLogicalType(new AvroSchema(json));
    assertNull(lt.getNamespace());
    assertNull(lt.getName());
    assertEquals(Schema.Type.STRUCT, lt.getRootSchema().getType());
  }

  @Test
  void testProperUnionConversion() {
    org.apache.avro.Schema unionSchema = org.apache.avro.Schema.createUnion(
        SchemaBuilder.builder().intType(),
        SchemaBuilder.builder().stringType());
    Schema result = AvroToLogicalTypeConverter.toRootSchema(new AvroSchema(unionSchema));
    assertEquals(Schema.Type.UNION, result.getType());
    assertEquals(2, result.getBranches().size());
    assertEquals("int", result.getBranches().get(0).getName());
    assertEquals(Schema.Type.INT, result.getBranches().get(0).getSchema().getType());
    assertEquals("string", result.getBranches().get(1).getName());
    assertEquals(Schema.Type.VARCHAR, result.getBranches().get(1).getSchema().getType());
  }

  @Test
  void testNullableUnionConversion() {
    org.apache.avro.Schema nullableString = SchemaBuilder.unionOf()
        .nullType().and().stringType().endUnion();
    Schema result = AvroToLogicalTypeConverter.toRootSchema(new AvroSchema(nullableString));
    assertEquals(Schema.Type.VARCHAR, result.getType());
    assertTrue(result.isNullable());
  }

  @Test
  void testNullableProperUnion() {
    org.apache.avro.Schema nullableUnion = org.apache.avro.Schema.createUnion(
        SchemaBuilder.builder().nullType(),
        SchemaBuilder.builder().intType(),
        SchemaBuilder.builder().stringType());
    Schema result = AvroToLogicalTypeConverter.toRootSchema(new AvroSchema(nullableUnion));
    assertEquals(Schema.Type.UNION, result.getType());
    assertTrue(result.isNullable());
    assertEquals(2, result.getBranches().size());
  }

  // =========================================================================
  // Legacy-form MapEntry recognition (Flink-emitted and AvroData-emitted).
  //
  // Flink and AvroData (anonymous Connect schemas) use the canonical entry-
  // record name io.confluent.connect.avro.MapEntry. AvroData (named Connect
  // schemas) uses an arbitrary record name with the connect.internal.type=
  // MapEntry prop. The LT reader accepts both as fallbacks alongside its
  // primary LogicalMap-on-array recognition.
  // =========================================================================

  @Test
  void testReadsLegacyCanonicalMapEntry() {
    // Schema as emitted by Flink-Avro and AvroData (anonymous): array of
    // canonical MapEntry record, no logicalType=map marker.
    org.apache.avro.Schema entry = SchemaBuilder.record("MapEntry")
        .namespace("io.confluent.connect.avro")
        .fields()
        .name("key").type().intType().noDefault()
        .name("value").type().stringType().noDefault()
        .endRecord();
    org.apache.avro.Schema arr = SchemaBuilder.array().items(entry);
    Schema result = AvroToLogicalTypeConverter.toRootSchema(new AvroSchema(arr));
    assertEquals(Schema.Type.MAP, result.getType());
    assertEquals(Schema.Type.INT, result.getKeyType().getType());
    assertEquals(Schema.Type.VARCHAR, result.getValueType().getType());
  }

  @Test
  void testReadsLegacyConnectInternalTypeProp() {
    // Schema as emitted by AvroData when the Connect schema has its own name:
    // user-named record with connect.internal.type=MapEntry prop.
    org.apache.avro.Schema entry = SchemaBuilder.record("MyEntry")
        .namespace("com.example")
        .prop("connect.internal.type", "MapEntry")
        .fields()
        .name("key").type().longType().noDefault()
        .name("value").type().booleanType().noDefault()
        .endRecord();
    org.apache.avro.Schema arr = SchemaBuilder.array().items(entry);
    Schema result = AvroToLogicalTypeConverter.toRootSchema(new AvroSchema(arr));
    assertEquals(Schema.Type.MAP, result.getType());
    assertEquals(Schema.Type.BIGINT, result.getKeyType().getType());
    assertEquals(Schema.Type.BOOLEAN, result.getValueType().getType());
  }

  @Test
  void testDeeplyNestedRecordIsRejected() {
    // A finite but deeply nested (non-cyclic) record would recurse until the JVM
    // stack overflows; the depth guard turns it into a ValidationException well
    // before that. Depth is a little past MAX_TYPE_DEPTH (256) so the guard fires
    // after only a few hundred frames — deterministic on any reasonable stack.
    org.apache.avro.Schema inner =
        org.apache.avro.Schema.create(org.apache.avro.Schema.Type.STRING);
    for (int i = 0; i < 300; i++) {
      org.apache.avro.Schema rec =
          org.apache.avro.Schema.createRecord("R" + i, null, "ns", false);
      rec.setFields(Collections.singletonList(
          new org.apache.avro.Schema.Field("f", inner, null, null)));
      inner = rec;
    }
    // Build the wrapper outside the assertion: constructing a 300-deep schema is
    // cheap, so any thrown ValidationException comes from the conversion guard.
    final AvroSchema deep = new AvroSchema(inner);
    assertThrows(ValidationException.class,
        () -> AvroToLogicalTypeConverter.toRootSchema(deep));
  }

  @Test
  void testFieldAliasesPopulatedFromAvro() {
    String json = "{\"type\":\"record\",\"name\":\"M\",\"fields\":["
        + "{\"name\":\"a\",\"type\":\"int\",\"aliases\":[\"a_old\",\"a_older\"]}]}";
    org.apache.avro.Schema parsed = new org.apache.avro.Schema.Parser().parse(json);

    Schema struct = AvroToLogicalTypeConverter.toLogicalType(new AvroSchema(parsed))
        .getRootSchema();

    assertThat(struct.getField("a").getAliases())
        .containsExactlyInAnyOrder("a_old", "a_older");
  }

  @Test
  void testStrayAliasParamOverriddenByNativeAliases() {
    // A hand-authored schema that puts logical.aliases in confluent:params must not win over the
    // authoritative native alias; the native "a_old" replaces the smuggled "stray".
    String json = "{\"type\":\"record\",\"name\":\"M\",\"fields\":["
        + "{\"name\":\"a\",\"type\":\"int\",\"aliases\":[\"a_old\"],"
        + "\"confluent:params\":{\"logical.aliases\":\"stray\"}}]}";
    org.apache.avro.Schema parsed = new org.apache.avro.Schema.Parser().parse(json);

    Schema struct = AvroToLogicalTypeConverter.toLogicalType(new AvroSchema(parsed))
        .getRootSchema();

    assertThat(struct.getField("a").getAliases()).containsExactly("a_old");
  }
}
