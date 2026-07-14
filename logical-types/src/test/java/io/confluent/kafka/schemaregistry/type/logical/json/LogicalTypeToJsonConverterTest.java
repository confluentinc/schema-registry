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

package io.confluent.kafka.schemaregistry.type.logical.json;

import io.confluent.kafka.schemaregistry.json.JsonSchema;
import io.confluent.kafka.schemaregistry.client.rest.entities.SchemaReference;
import io.confluent.kafka.schemaregistry.type.logical.LogicalType;
import io.confluent.kafka.schemaregistry.type.logical.common.LogicalTypeVersion;
import io.confluent.kafka.schemaregistry.type.logical.Schema;
import io.confluent.kafka.schemaregistry.type.logical.Schema.EnumValue;
import io.confluent.kafka.schemaregistry.type.logical.Schema.Field;
import io.confluent.kafka.schemaregistry.type.logical.Schema.UnionBranch;
import io.confluent.kafka.schemaregistry.type.logical.ValidationException;
import org.everit.json.schema.BooleanSchema;
import org.everit.json.schema.CombinedSchema;
import org.everit.json.schema.EmptySchema;
import org.everit.json.schema.EnumSchema;
import org.everit.json.schema.NullSchema;
import org.everit.json.schema.ObjectSchema;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class LogicalTypeToJsonConverterTest {

  @Test
  void testBooleanType() {
    Schema schema = Schema.create(Schema.Type.BOOLEAN).setNullable(false);
    JsonSchema result = LogicalTypeToJsonConverter.fromLogicalType(new LogicalType(schema), "row");
    assertTrue(result.rawSchema() instanceof BooleanSchema);
  }

  @Test
  void testNullableType() {
    Schema schema = Schema.create(Schema.Type.BOOLEAN).setNullable(true);
    JsonSchema result = LogicalTypeToJsonConverter.fromLogicalType(new LogicalType(schema), "row");
    assertTrue(result.rawSchema() instanceof CombinedSchema);
    CombinedSchema combined = (CombinedSchema) result.rawSchema();
    assertEquals(2, combined.getSubschemas().size());
    assertTrue(combined.getSubschemas().stream().anyMatch(s -> s instanceof NullSchema));
    assertTrue(combined.getSubschemas().stream().anyMatch(s -> s instanceof BooleanSchema));
  }

  @Test
  void testEnumType() {
    Schema schema = Schema.createEnum(Arrays.asList(
        new EnumValue("A"), new EnumValue("B"), new EnumValue("C")))
        .setNullable(false);
    JsonSchema result = LogicalTypeToJsonConverter.fromLogicalType(new LogicalType(schema), "row");
    assertTrue(result.rawSchema() instanceof EnumSchema);
  }

  @Test
  void testUnionType() {
    Schema schema = Schema.createUnion(Arrays.asList(
        new UnionBranch("int_val", Schema.create(Schema.Type.INT).setNullable(false)),
        new UnionBranch("str_val", Schema.createString().setNullable(false))))
        .setNullable(false);
    JsonSchema result = LogicalTypeToJsonConverter.fromLogicalType(new LogicalType(schema), "row");
    assertTrue(result.rawSchema() instanceof CombinedSchema);
    CombinedSchema combined = (CombinedSchema) result.rawSchema();
    assertEquals(CombinedSchema.ONE_CRITERION, combined.getCriterion());
    assertEquals(2, combined.getSubschemas().size());
  }

  @Test
  void testStructType() {
    Schema schema = Schema.createStruct(Arrays.asList(
        new Field("name", Schema.createString().setNullable(false), 0),
        new Field("age", Schema.create(Schema.Type.INT).setNullable(false), 1)))
        .setNullable(false);
    JsonSchema result = LogicalTypeToJsonConverter.fromLogicalType(new LogicalType(schema), "row");
    assertTrue(result.rawSchema() instanceof ObjectSchema);
    ObjectSchema obj = (ObjectSchema) result.rawSchema();
    assertEquals(2, obj.getPropertySchemas().size());
  }

  @Test
  void testRoundTripPrimitives() {
    for (Schema.Type type : new Schema.Type[]{
        Schema.Type.BOOLEAN, Schema.Type.INT, Schema.Type.BIGINT,
        Schema.Type.FLOAT, Schema.Type.DOUBLE, Schema.Type.DATE}) {
      Schema original = Schema.create(type).setNullable(false);
      JsonSchema json = LogicalTypeToJsonConverter.fromLogicalType(new LogicalType(original), "row");
      Schema roundTripped = JsonToLogicalTypeConverter.toRootSchema(json);
      assertEquals(original, roundTripped, "Round trip failed for " + type);
    }
  }

  @Test
  void testRoundTripStructDocTagsParams() {
    Map<String, Object> schemaParams = new LinkedHashMap<>();
    schemaParams.put("version", "2");
    Schema struct = Schema.createStruct(Arrays.asList(
        new Field("name", Schema.createString().setNullable(false), 0)))
        .setNullable(false)
        .setDoc("a person record")
        .setTags(Arrays.asList("PII", "SENSITIVE"))
        .setParams(schemaParams);

    JsonSchema json = LogicalTypeToJsonConverter.fromLogicalType(new LogicalType(struct), "Person");
    Schema roundTripped = JsonToLogicalTypeConverter.toRootSchema(json);

    assertEquals("a person record", roundTripped.getDoc());
    assertEquals(Arrays.asList("PII", "SENSITIVE"), roundTripped.getTags());
    assertEquals("2", roundTripped.getParams().get("version"));
  }

  @Test
  void testRoundTripEnumValueDocAndParams() {
    Map<String, Object> params = new LinkedHashMap<>();
    params.put("rgb", "#FF0000");
    Schema enumSchema = Schema.createEnum(Arrays.asList(
        new EnumValue("RED", "the color red", params),
        new EnumValue("GREEN", "the color green", null),
        new EnumValue("BLUE")))
        .setNullable(false);

    JsonSchema json = LogicalTypeToJsonConverter.fromLogicalType(new LogicalType(enumSchema), "Color");
    Schema roundTripped = JsonToLogicalTypeConverter.toRootSchema(json);

    assertEquals("the color red", roundTripped.getEnumValues().get(0).getDoc());
    assertEquals("#FF0000", roundTripped.getEnumValues().get(0).getParams().get("rgb"));
    assertEquals("the color green", roundTripped.getEnumValues().get(1).getDoc());
    assertTrue(roundTripped.getEnumValues().get(1).getParams().isEmpty());
    assertNull(roundTripped.getEnumValues().get(2).getDoc());
  }

  @Test
  void testRoundTripFieldTagsAndParams() {
    Map<String, Object> fieldParams = new LinkedHashMap<>();
    fieldParams.put("sensitivity", "high");
    Schema struct = Schema.createStruct(Arrays.asList(
        new Field("email", Schema.createString().setNullable(false), 0,
            null, false, null, Arrays.asList("PII", "EMAIL"), fieldParams),
        new Field("name", Schema.createString().setNullable(false), 1)))
        .setNullable(false);

    JsonSchema json = LogicalTypeToJsonConverter.fromLogicalType(new LogicalType(struct), "Person");
    Schema roundTripped = JsonToLogicalTypeConverter.toRootSchema(json);

    assertEquals(Arrays.asList("PII", "EMAIL"), roundTripped.getField("email").getTags());
    assertEquals("high", roundTripped.getField("email").getParams().get("sensitivity"));
    assertTrue(roundTripped.getField("name").getTags().isEmpty());
    assertTrue(roundTripped.getField("name").getParams().isEmpty());
  }

  @Test
  void testRoundTripMapWithVarcharKeyLength() {
    Schema map = Schema.createMap(
        Schema.createVarchar(100).setNullable(false),
        Schema.create(Schema.Type.INT).setNullable(false))
        .setNullable(false);

    JsonSchema json = LogicalTypeToJsonConverter.fromLogicalType(new LogicalType(map), "row");
    Schema roundTripped = JsonToLogicalTypeConverter.toRootSchema(json);

    assertEquals(Schema.Type.MAP, roundTripped.getType());
    assertEquals(Schema.Type.VARCHAR, roundTripped.getKeyType().getType());
    assertEquals(100, roundTripped.getKeyType().getLength());
  }

  @Test
  void testRoundTripMapWithCharKeyLength() {
    Schema map = Schema.createMap(
        Schema.createChar(10).setNullable(false),
        Schema.create(Schema.Type.INT).setNullable(false))
        .setNullable(false);

    JsonSchema json = LogicalTypeToJsonConverter.fromLogicalType(new LogicalType(map), "row");
    Schema roundTripped = JsonToLogicalTypeConverter.toRootSchema(json);

    assertEquals(Schema.Type.MAP, roundTripped.getType());
    assertEquals(Schema.Type.CHAR, roundTripped.getKeyType().getType());
    assertEquals(10, roundTripped.getKeyType().getLength());
  }

  @Test
  void testRoundTripEnumDocTagsParams() {
    Map<String, Object> schemaParams = new LinkedHashMap<>();
    schemaParams.put("source", "api");
    Schema enumSchema = Schema.createEnum(Arrays.asList(
        new EnumValue("A"), new EnumValue("B")))
        .setNullable(false)
        .setDoc("status codes")
        .setTags(Arrays.asList("INTERNAL"))
        .setParams(schemaParams);

    JsonSchema json = LogicalTypeToJsonConverter.fromLogicalType(new LogicalType(enumSchema), "Status");
    Schema roundTripped = JsonToLogicalTypeConverter.toRootSchema(json);

    assertEquals("status codes", roundTripped.getDoc());
    assertEquals(Arrays.asList("INTERNAL"), roundTripped.getTags());
    assertEquals("api", roundTripped.getParams().get("source"));
  }

  @Test
  void testVariantType() {
    Schema schema = Schema.create(Schema.Type.VARIANT).setNullable(false);
    JsonSchema result = LogicalTypeToJsonConverter.fromLogicalType(new LogicalType(schema), "row");
    assertTrue(result.rawSchema() instanceof EmptySchema);
  }

  @Test
  void testRoundTripVariant() {
    Schema original = Schema.create(Schema.Type.VARIANT).setNullable(false);
    JsonSchema json = LogicalTypeToJsonConverter.fromLogicalType(new LogicalType(original), "row");
    Schema roundTripped = JsonToLogicalTypeConverter.toRootSchema(json);
    assertEquals(original, roundTripped);
  }

  @Test
  void testRoundTripVariantInStruct() {
    Schema struct = Schema.createStruct(Arrays.asList(
        new Field("data", Schema.create(Schema.Type.VARIANT).setNullable(true), 0),
        new Field("name", Schema.createString().setNullable(false), 1)))
        .setNullable(false);

    JsonSchema json = LogicalTypeToJsonConverter.fromLogicalType(new LogicalType(struct), "TestRecord");
    Schema roundTripped = JsonToLogicalTypeConverter.toRootSchema(json);

    assertEquals(Schema.Type.VARIANT, roundTripped.getField("data").getSchema().getType());
    assertTrue(roundTripped.getField("data").getSchema().isNullable());
    assertEquals(Schema.Type.VARCHAR, roundTripped.getField("name").getSchema().getType());
  }

  @Test
  void testRoundTripNamedTypeRef() {
    Schema addressType = Schema.createStruct(Arrays.asList(
        new Field("street", Schema.createString().setNullable(false), 0),
        new Field("city", Schema.createString().setNullable(false), 1)))
        .setNullable(false);
    Map<String, Schema> namedTypes = new LinkedHashMap<>();
    namedTypes.put("Address", addressType);

    Schema struct = Schema.createStruct(Arrays.asList(
        new Field("name", Schema.createString().setNullable(false), 0),
        new Field("home", Schema.createNamedTypeRef("Address").setNullable(true), 1),
        new Field("work", Schema.createNamedTypeRef("Address").setNullable(true), 2)))
        .setNullable(false);

    JsonSchema json = LogicalTypeToJsonConverter.fromLogicalType(
        new LogicalType(struct, namedTypes), "Person");
    LogicalType roundTripped = JsonToLogicalTypeConverter.toLogicalType(json);

    assertEquals(Schema.Type.NAMED_TYPE_REF,
        roundTripped.getRootSchema().getField("home").getSchema().getType());
    assertEquals("Address",
        roundTripped.getRootSchema().getField("home").getSchema().getQualifiedName());
    assertEquals(Schema.Type.NAMED_TYPE_REF,
        roundTripped.getRootSchema().getField("work").getSchema().getType());
    // Verify named type definition was recovered
    assertEquals(1, roundTripped.getNamedTypes().size());
    Schema recoveredAddress = roundTripped.getNamedTypes().get("Address");
    assertNotNull(recoveredAddress);
    assertEquals(Schema.Type.STRUCT, recoveredAddress.getType());
    assertEquals(2, recoveredAddress.getFields().size());
  }

  @Test
  void testRoundTripNamedStructLossless() {
    Schema addressType = Schema.createStruct(Arrays.asList(
        new Field("street", Schema.createVarchar(200).setNullable(false), 0),
        new Field("city", Schema.createString().setNullable(false), 1),
        new Field("zip", Schema.create(Schema.Type.INT).setNullable(true), 2)))
        .setNullable(false);
    Map<String, Schema> namedTypes = new LinkedHashMap<>();
    namedTypes.put("Address", addressType);

    Schema rootSchema = Schema.createStruct(Arrays.asList(
        new Field("home", Schema.createNamedTypeRef("Address").setNullable(true), 0),
        new Field("work", Schema.createNamedTypeRef("Address").setNullable(true), 1)))
        .setNullable(false);

    LogicalType original = new LogicalType(rootSchema, namedTypes);
    JsonSchema json = LogicalTypeToJsonConverter.fromLogicalType(original, "Person");
    LogicalType roundTripped = JsonToLogicalTypeConverter.toLogicalType(json);

    assertEquals(original.getRootSchema(), roundTripped.getRootSchema());
    assertEquals(original.getNamedTypes(), roundTripped.getNamedTypes());
  }

  @Test
  void testRoundTripNamedEnumLossless() {
    Schema statusType = Schema.createEnum(Arrays.asList(
        new Schema.EnumValue("ACTIVE"),
        new Schema.EnumValue("INACTIVE")))
        .setNullable(false);
    Map<String, Schema> namedTypes = new LinkedHashMap<>();
    namedTypes.put("Status", statusType);

    Schema rootSchema = Schema.createStruct(Arrays.asList(
        new Field("status", Schema.createNamedTypeRef("Status").setNullable(true), 0)))
        .setNullable(false);

    LogicalType original = new LogicalType(rootSchema, namedTypes);
    JsonSchema json = LogicalTypeToJsonConverter.fromLogicalType(original, "Record");
    LogicalType roundTripped = JsonToLogicalTypeConverter.toLogicalType(json);

    assertEquals(original.getRootSchema(), roundTripped.getRootSchema());
    assertEquals(original.getNamedTypes(), roundTripped.getNamedTypes());
  }

  @Test
  void testRoundTripUnionBranchNames() {
    Schema union = Schema.createUnion(Arrays.asList(
        new UnionBranch("amount", Schema.create(Schema.Type.INT).setNullable(true)),
        new UnionBranch("description", Schema.createString().setNullable(true))))
        .setNullable(false);

    JsonSchema json = LogicalTypeToJsonConverter.fromLogicalType(new LogicalType(union), "root");
    Schema roundTripped = JsonToLogicalTypeConverter.toRootSchema(json);

    assertEquals(Schema.Type.UNION, roundTripped.getType());
    assertEquals("amount", roundTripped.getBranches().get(0).getName());
    assertEquals("description", roundTripped.getBranches().get(1).getName());
  }

  @Test
  void testRoundTripUnionBranchDoc() {
    Schema union = Schema.createUnion(Arrays.asList(
        new UnionBranch("amt", Schema.create(Schema.Type.INT).setNullable(true),
            "the amount", null),
        new UnionBranch("desc", Schema.createString().setNullable(true),
            "the description", null)))
        .setNullable(false);

    JsonSchema json = LogicalTypeToJsonConverter.fromLogicalType(new LogicalType(union), "root");
    Schema roundTripped = JsonToLogicalTypeConverter.toRootSchema(json);

    assertEquals("the amount", roundTripped.getBranches().get(0).getDoc());
    assertEquals("the description", roundTripped.getBranches().get(1).getDoc());
  }

  @Test
  void testRoundTripUnionBranchParams() {
    Map<String, Object> params = new LinkedHashMap<>();
    params.put("format", "currency");
    params.put("precision", "2");
    Schema union = Schema.createUnion(Arrays.asList(
        new UnionBranch("amt", Schema.create(Schema.Type.INT).setNullable(true),
            null, params),
        new UnionBranch("desc", Schema.createString().setNullable(true))))
        .setNullable(false);

    JsonSchema json = LogicalTypeToJsonConverter.fromLogicalType(new LogicalType(union), "root");
    Schema roundTripped = JsonToLogicalTypeConverter.toRootSchema(json);

    Map<String, Object> rtParams = roundTripped.getBranches().get(0).getParams();
    assertEquals("currency", rtParams.get("format"));
    assertEquals("2", rtParams.get("precision"));
    assertTrue(roundTripped.getBranches().get(1).getParams().isEmpty());
  }

  @Test
  void testRoundTripExternalNamedTypeRef() {
    String addressJsonSchema = "{\"$defs\":{\"com.example.Address\":{"
        + "\"type\":\"object\","
        + "\"properties\":{\"street\":{\"type\":\"string\"},"
        + "\"city\":{\"type\":\"string\"}}}}}";

    List<SchemaReference> references = Arrays.asList(
        new SchemaReference("com.example.Address", "address-value", 1));
    Map<String, String> resolvedReferences = new LinkedHashMap<>();
    resolvedReferences.put("com.example.Address", addressJsonSchema);

    Schema rootSchema = Schema.createStruct(Arrays.asList(
        new Field("name", Schema.createString().setNullable(false), 0),
        new Field("home",
            Schema.createNamedTypeRef("com.example.Address").setNullable(true), 1)))
        .setNullable(false);

    LogicalType original = new LogicalType(rootSchema, Map.of(),
        references, resolvedReferences);
    JsonSchema json = LogicalTypeToJsonConverter.fromLogicalType(original, "Person");
    LogicalType roundTripped = JsonToLogicalTypeConverter.toLogicalType(json);

    assertEquals(Schema.Type.NAMED_TYPE_REF,
        roundTripped.getRootSchema().getField("home").getSchema().getType());
    assertEquals("com.example.Address",
        roundTripped.getRootSchema().getField("home").getSchema().getQualifiedName());
  }

  @Test
  void testRoundTripMultipleExternalRefs() {
    String addressJson = "{\"$defs\":{\"com.example.Address\":{"
        + "\"type\":\"object\","
        + "\"properties\":{\"city\":{\"type\":\"string\"}}}}}";
    String moneyJson = "{\"$defs\":{\"com.example.Money\":{"
        + "\"type\":\"object\","
        + "\"properties\":{\"amount\":{\"type\":\"number\"}}}}}";

    List<SchemaReference> references = Arrays.asList(
        new SchemaReference("com.example.Address", "address-value", 1),
        new SchemaReference("com.example.Money", "money-value", 1));
    Map<String, String> resolvedReferences = new LinkedHashMap<>();
    resolvedReferences.put("com.example.Address", addressJson);
    resolvedReferences.put("com.example.Money", moneyJson);

    Schema rootSchema = Schema.createStruct(Arrays.asList(
        new Field("home",
            Schema.createNamedTypeRef("com.example.Address").setNullable(true), 0),
        new Field("payment",
            Schema.createNamedTypeRef("com.example.Money").setNullable(true), 1)))
        .setNullable(false);

    LogicalType original = new LogicalType(rootSchema, Map.of(),
        references, resolvedReferences);
    JsonSchema json = LogicalTypeToJsonConverter.fromLogicalType(original, "Order");
    LogicalType roundTripped = JsonToLogicalTypeConverter.toLogicalType(json);

    assertEquals(Schema.Type.NAMED_TYPE_REF,
        roundTripped.getRootSchema().getField("home").getSchema().getType());
    assertEquals("com.example.Address",
        roundTripped.getRootSchema().getField("home").getSchema().getQualifiedName());
    assertEquals(Schema.Type.NAMED_TYPE_REF,
        roundTripped.getRootSchema().getField("payment").getSchema().getType());
    assertEquals("com.example.Money",
        roundTripped.getRootSchema().getField("payment").getSchema().getQualifiedName());
  }

  // =========================================================================
  // Draft 7 external refs — addressable types declared inside `definitions`
  // (the pre-2019-09 keyword for $defs). JsonSchema bridges definitions →
  // $defs at load time so json-sKema's JSON Pointer resolution succeeds.
  // =========================================================================

  @Test
  void testRoundTripExternalNamedTypeRefDraft7() {
    // External doc uses draft-7's `definitions` keyword. JsonSchema bridges
    // it to `$defs` at load time, so json-sKema can resolve the JSON Pointer.
    String addressJsonSchema = "{\"definitions\":{\"com.example.Address\":{"
        + "\"type\":\"object\","
        + "\"properties\":{\"street\":{\"type\":\"string\"},"
        + "\"city\":{\"type\":\"string\"}}}}}";

    List<SchemaReference> references = Arrays.asList(
        new SchemaReference("com.example.Address", "address-value", 1));
    Map<String, String> resolvedReferences = new LinkedHashMap<>();
    resolvedReferences.put("com.example.Address", addressJsonSchema);

    Schema rootSchema = Schema.createStruct(Arrays.asList(
        new Field("name", Schema.createString().setNullable(false), 0),
        new Field("home",
            Schema.createNamedTypeRef("com.example.Address").setNullable(true), 1)))
        .setNullable(false);

    LogicalType original = new LogicalType(rootSchema, Map.of(),
        references, resolvedReferences);
    JsonSchema json = LogicalTypeToJsonConverter.fromLogicalType(original, "Person");
    LogicalType roundTripped = JsonToLogicalTypeConverter.toLogicalType(json);

    assertEquals(Schema.Type.NAMED_TYPE_REF,
        roundTripped.getRootSchema().getField("home").getSchema().getType());
    assertEquals("com.example.Address",
        roundTripped.getRootSchema().getField("home").getSchema().getQualifiedName());
  }

  @Test
  void testRoundTripExternalNestedNamedTypeRefDraft7() {
    // Nested type lives in draft-7's `definitions`; the entry key matches
    // the qualified name. JsonSchema bridges definitions → $defs at load time.
    String outerJsonSchema = "{\"type\":\"object\","
        + "\"definitions\":{\"com.example.Outer.Inner\":{"
        + "\"type\":\"object\","
        + "\"properties\":{\"label\":{\"type\":\"string\"}}}}}";

    List<SchemaReference> references = Arrays.asList(
        new SchemaReference("com.example.Outer", "outer-value", 1));
    Map<String, String> resolvedReferences = new LinkedHashMap<>();
    resolvedReferences.put("com.example.Outer", outerJsonSchema);

    Schema rootSchema = Schema.createStruct(Arrays.asList(
        new Field("inner",
            Schema.createNamedTypeRef("com.example.Outer.Inner").setNullable(true), 0)))
        .setNullable(false);

    LogicalType original = new LogicalType(rootSchema, Map.of(),
        references, resolvedReferences);
    JsonSchema json = LogicalTypeToJsonConverter.fromLogicalType(original, "Holder");
    LogicalType roundTripped = JsonToLogicalTypeConverter.toLogicalType(json);

    Schema inner = roundTripped.getRootSchema().getField("inner").getSchema();
    assertEquals(Schema.Type.NAMED_TYPE_REF, inner.getType());
    assertEquals("com.example.Outer.Inner", inner.getQualifiedName());
  }

  @Test
  void testRoundTripExternalNestedEnumRefDraft7() {
    String externalSchema = "{\"type\":\"object\","
        + "\"definitions\":{\"com.example.Color\":{"
        + "\"type\":\"string\",\"enum\":[\"RED\",\"GREEN\",\"BLUE\"]}}}";

    List<SchemaReference> references = Arrays.asList(
        new SchemaReference("com.example.Outer", "outer-value", 1));
    Map<String, String> resolvedReferences = new LinkedHashMap<>();
    resolvedReferences.put("com.example.Outer", externalSchema);

    Schema rootSchema = Schema.createStruct(Arrays.asList(
        new Field("favorite",
            Schema.createNamedTypeRef("com.example.Color").setNullable(true), 0)))
        .setNullable(false);

    LogicalType original = new LogicalType(rootSchema, Map.of(),
        references, resolvedReferences);
    JsonSchema json = LogicalTypeToJsonConverter.fromLogicalType(original, "Holder");
    LogicalType roundTripped = JsonToLogicalTypeConverter.toLogicalType(json);
    Schema fav = roundTripped.getRootSchema().getField("favorite").getSchema();
    assertEquals(Schema.Type.NAMED_TYPE_REF, fav.getType());
    assertEquals("com.example.Color", fav.getQualifiedName());
  }

  @Test
  void testRoundTripMixedDraft7AndDraft2020Externals() {
    // One external uses draft-7's `definitions` keyword, the other uses
    // 2020-12's `$defs`. Both resolve through the same loader path; the
    // draft-7 doc's `definitions` is bridged to `$defs` in JsonSchema.
    String draft7Doc = "{\"type\":\"object\","
        + "\"definitions\":{\"com.example.Color\":{"
        + "\"type\":\"string\",\"enum\":[\"RED\",\"GREEN\"]}}}";
    String draft2020Doc = "{\"$defs\":{\"com.example.Money\":{"
        + "\"type\":\"object\","
        + "\"properties\":{\"amount\":{\"type\":\"number\"}}}}}";

    List<SchemaReference> references = Arrays.asList(
        new SchemaReference("com.example.Outer", "outer-value", 1),
        new SchemaReference("com.example.Money", "money-value", 1));
    Map<String, String> resolvedReferences = new LinkedHashMap<>();
    resolvedReferences.put("com.example.Outer", draft7Doc);
    resolvedReferences.put("com.example.Money", draft2020Doc);

    Schema rootSchema = Schema.createStruct(Arrays.asList(
        new Field("color",
            Schema.createNamedTypeRef("com.example.Color").setNullable(true), 0),
        new Field("payment",
            Schema.createNamedTypeRef("com.example.Money").setNullable(true), 1)))
        .setNullable(false);

    LogicalType original = new LogicalType(rootSchema, Map.of(),
        references, resolvedReferences);
    JsonSchema json = LogicalTypeToJsonConverter.fromLogicalType(original, "Holder");
    LogicalType roundTripped = JsonToLogicalTypeConverter.toLogicalType(json);

    Schema color = roundTripped.getRootSchema().getField("color").getSchema();
    assertEquals(Schema.Type.NAMED_TYPE_REF, color.getType());
    assertEquals("com.example.Color", color.getQualifiedName());

    Schema payment = roundTripped.getRootSchema().getField("payment").getSchema();
    assertEquals(Schema.Type.NAMED_TYPE_REF, payment.getType());
    assertEquals("com.example.Money", payment.getQualifiedName());
  }

  @Test
  void testRoundTripMixedLocalAndExternalRefs() {
    String externalJson = "{\"$defs\":{\"com.example.Money\":{"
        + "\"type\":\"object\","
        + "\"properties\":{\"amount\":{\"type\":\"number\"}}}}}";

    List<SchemaReference> references = Arrays.asList(
        new SchemaReference("com.example.Money", "money-value", 1));
    Map<String, String> resolvedReferences = new LinkedHashMap<>();
    resolvedReferences.put("com.example.Money", externalJson);

    Schema addressType = Schema.createStruct(Arrays.asList(
        new Field("city", Schema.createString().setNullable(false), 0)))
        .setNullable(false);
    Map<String, Schema> namedTypes = new LinkedHashMap<>();
    namedTypes.put("Address", addressType);

    Schema rootSchema = Schema.createStruct(Arrays.asList(
        new Field("home",
            Schema.createNamedTypeRef("Address").setNullable(true), 0),
        new Field("payment",
            Schema.createNamedTypeRef("com.example.Money").setNullable(true), 1)))
        .setNullable(false);

    LogicalType original = new LogicalType(rootSchema, namedTypes,
        references, resolvedReferences);
    JsonSchema json = LogicalTypeToJsonConverter.fromLogicalType(original, "Order");
    LogicalType roundTripped = JsonToLogicalTypeConverter.toLogicalType(json);

    assertEquals(Schema.Type.NAMED_TYPE_REF,
        roundTripped.getRootSchema().getField("home").getSchema().getType());
    assertEquals("Address",
        roundTripped.getRootSchema().getField("home").getSchema().getQualifiedName());
    assertEquals(Schema.Type.NAMED_TYPE_REF,
        roundTripped.getRootSchema().getField("payment").getSchema().getType());
    assertEquals("com.example.Money",
        roundTripped.getRootSchema().getField("payment").getSchema().getQualifiedName());
    assertNotNull(roundTripped.getNamedTypes().get("Address"));
  }

  @Test
  void testRoundTripExternalNestedNamedTypeRef() {
    // External schema for Outer; the LogicalType references the nested type
    // com.example.Outer.Inner directly via NAMED_TYPE_REF. The $defs entry
    // key matches the qualified name; refs use #/$defs/<name> form.
    String outerJsonSchema = "{\"type\":\"object\","
        + "\"$defs\":{\"com.example.Outer.Inner\":{"
        + "\"type\":\"object\","
        + "\"properties\":{\"label\":{\"type\":\"string\"}}}}}";

    List<SchemaReference> references = Arrays.asList(
        new SchemaReference("com.example.Outer", "outer-value", 1));
    Map<String, String> resolvedReferences = new LinkedHashMap<>();
    resolvedReferences.put("com.example.Outer", outerJsonSchema);

    Schema rootSchema = Schema.createStruct(Arrays.asList(
        new Field("inner",
            Schema.createNamedTypeRef("com.example.Outer.Inner").setNullable(true), 0)))
        .setNullable(false);

    LogicalType original = new LogicalType(rootSchema, Map.of(),
        references, resolvedReferences);
    JsonSchema json = LogicalTypeToJsonConverter.fromLogicalType(original, "Holder");
    LogicalType roundTripped = JsonToLogicalTypeConverter.toLogicalType(json);

    Schema inner = roundTripped.getRootSchema().getField("inner").getSchema();
    assertEquals(Schema.Type.NAMED_TYPE_REF, inner.getType());
    assertEquals("com.example.Outer.Inner", inner.getQualifiedName());
    // References and resolvedReferences should pass through unchanged
    assertEquals(references, roundTripped.getReferences());
    assertEquals(resolvedReferences, roundTripped.getResolvedReferences());
  }

  @Test
  void testRoundTripDefaultValues() {
    Schema rootSchema = Schema.createStruct(Arrays.asList(
        new Field("name", Schema.createString().setNullable(false), 0,
            "Anonymous", true, null, null, null),
        new Field("age", Schema.create(Schema.Type.INT).setNullable(false), 1,
            18, true, null, null, null),
        new Field("score",
            Schema.create(Schema.Type.DOUBLE).setNullable(true), 2,
            0.0, true, null, null, null)))
        .setNullable(false);

    LogicalType original = new LogicalType(rootSchema);
    JsonSchema json = LogicalTypeToJsonConverter.fromLogicalType(original, "Person");
    LogicalType roundTripped = JsonToLogicalTypeConverter.toLogicalType(json);

    assertEquals("Anonymous",
        roundTripped.getRootSchema().getField("name").getDefaultValue());
    assertEquals(18,
        roundTripped.getRootSchema().getField("age").getDefaultValue());
    assertEquals(0.0,
        roundTripped.getRootSchema().getField("score").getDefaultValue());
  }

  @Test
  void testRoundTripNestedDefaultValues() {
    Schema innerStruct = Schema.createStruct(Arrays.asList(
        new Field("street", Schema.createString().setNullable(false), 0,
            "Main St", true, null, null, null),
        new Field("city", Schema.createString().setNullable(false), 1,
            "Springfield", true, null, null, null)))
        .setNullable(false);
    Schema rootSchema = Schema.createStruct(Arrays.asList(
        new Field("addr", innerStruct, 0)))
        .setNullable(false);

    LogicalType original = new LogicalType(rootSchema);
    JsonSchema json = LogicalTypeToJsonConverter.fromLogicalType(original, "Holder");
    LogicalType roundTripped = JsonToLogicalTypeConverter.toLogicalType(json);

    Schema rtInner = roundTripped.getRootSchema().getField("addr").getSchema();
    assertEquals("Main St", rtInner.getField("street").getDefaultValue());
    assertEquals("Springfield", rtInner.getField("city").getDefaultValue());
  }

  @Test
  void testRoundTripTypedDefaultValues() {
    Schema rootSchema = Schema.createStruct(Arrays.asList(
        new Field("dec",
            Schema.createDecimal(10, 2).setNullable(false), 0,
            new java.math.BigDecimal("12.34"), true, null, null, null),
        new Field("dt",
            Schema.create(Schema.Type.DATE).setNullable(false), 1,
            java.time.LocalDate.of(2026, 4, 17), true, null, null, null),
        new Field("blob",
            Schema.createVarbinary(100).setNullable(false), 2,
            new byte[]{1, 2, 3}, true, null, null, null)))
        .setNullable(false);

    LogicalType original = new LogicalType(rootSchema);
    JsonSchema json = LogicalTypeToJsonConverter.fromLogicalType(original, "Holder");
    LogicalType roundTripped = JsonToLogicalTypeConverter.toLogicalType(json);

    assertEquals(new java.math.BigDecimal("12.34"),
        roundTripped.getRootSchema().getField("dec").getDefaultValue());
    assertEquals(java.time.LocalDate.of(2026, 4, 17),
        roundTripped.getRootSchema().getField("dt").getDefaultValue());
    assertArrayEquals(new byte[]{1, 2, 3},
        (byte[]) roundTripped.getRootSchema().getField("blob").getDefaultValue());
  }

  @Test
  void testRoundTripTimestampPrecisionInJson() {
    for (int p : new int[]{0, 3, 6, 9}) {
      Schema rootSchema = Schema.createStruct(Arrays.asList(
          new Field("t", Schema.createTime(p).setNullable(false), 0),
          new Field("ts", Schema.createTimestamp(p).setNullable(false), 1),
          new Field("tsltz", Schema.createTimestampLtz(p).setNullable(false), 2)))
          .setNullable(false);
      LogicalType original = new LogicalType(rootSchema);
      JsonSchema json = LogicalTypeToJsonConverter.fromLogicalType(original, "Holder");
      LogicalType roundTripped = JsonToLogicalTypeConverter.toLogicalType(json);
      Schema rt = roundTripped.getRootSchema();
      assertEquals(p, rt.getField("t").getSchema().getPrecision());
      assertEquals(p, rt.getField("ts").getSchema().getPrecision());
      assertEquals(p, rt.getField("tsltz").getSchema().getPrecision());
    }
  }

  @Test
  void testDefsRecoveredAsSchemaObjects() {
    // Verifies that the everit library preserves $defs entries as Schema objects
    // (not raw maps), so recoverNamedTypesFromDefs can process them
    Schema addressType = Schema.createStruct(Arrays.asList(
        new Field("street", Schema.createString().setNullable(false), 0),
        new Field("city", Schema.createString().setNullable(false), 1)))
        .setNullable(false);
    Schema statusType = Schema.createEnum(Arrays.asList(
        new Schema.EnumValue("ACTIVE"),
        new Schema.EnumValue("INACTIVE")))
        .setNullable(false);
    Map<String, Schema> namedTypes = new LinkedHashMap<>();
    namedTypes.put("Address", addressType);
    namedTypes.put("Status", statusType);

    Schema rootSchema = Schema.createStruct(Arrays.asList(
        new Field("home", Schema.createNamedTypeRef("Address").setNullable(true), 0),
        new Field("status", Schema.createNamedTypeRef("Status").setNullable(true), 1)))
        .setNullable(false);

    LogicalType original = new LogicalType(rootSchema, namedTypes);
    JsonSchema json = LogicalTypeToJsonConverter.fromLogicalType(original, "Person");
    LogicalType roundTripped = JsonToLogicalTypeConverter.toLogicalType(json);

    // Verify both named types were recovered from $defs
    assertEquals(2, roundTripped.getNamedTypes().size());
    assertNotNull(roundTripped.getNamedTypes().get("Address"));
    assertEquals(Schema.Type.STRUCT, roundTripped.getNamedTypes().get("Address").getType());
    assertNotNull(roundTripped.getNamedTypes().get("Status"));
    assertEquals(Schema.Type.ENUM, roundTripped.getNamedTypes().get("Status").getType());
  }

  // =========================================================================
  // Defaults on complex types throw
  // =========================================================================

  private static JsonSchema convertSchema(Schema rootSchema) {
    return LogicalTypeToJsonConverter.fromLogicalType(
        new LogicalType(rootSchema), "Holder");
  }

  private static Schema fieldOfTypeWithDefault(Schema fieldType, Object defaultValue) {
    return Schema.createStruct(Arrays.asList(
        new Field("f", fieldType, 0, defaultValue, true, null, null, null)))
        .setNullable(false);
  }

  @Test
  void testDefaultOnArrayThrows() {
    assertThrows(ValidationException.class, () -> {
        Schema arr = Schema.createArray(Schema.create(Schema.Type.INT)).setNullable(false);
        convertSchema(fieldOfTypeWithDefault(arr, Arrays.asList(1, 2, 3)));
    });
  }

  @Test
  void testDefaultOnMapThrows() {
    assertThrows(ValidationException.class, () -> {
        Schema mapType = Schema.createMap(
            Schema.createString().setNullable(false),
            Schema.create(Schema.Type.INT).setNullable(false)).setNullable(false);
        convertSchema(fieldOfTypeWithDefault(mapType, Map.of("k", 1)));
    });
  }

  @Test
  void testDefaultOnMultisetThrows() {
    assertThrows(ValidationException.class, () -> {
        Schema multisetType = Schema.createMultiset(
            Schema.create(Schema.Type.INT).setNullable(false)).setNullable(false);
        convertSchema(fieldOfTypeWithDefault(multisetType, Map.of(1, 2)));
    });
  }

  @Test
  void testDefaultOnStructThrows() {
    assertThrows(ValidationException.class, () -> {
        Schema structType = Schema.createStruct(Arrays.asList(
            new Field("inner", Schema.createString().setNullable(false), 0)))
            .setNullable(false);
        convertSchema(fieldOfTypeWithDefault(structType, "anything"));
    });
  }

  @Test
  void testRoundTripEnumDefault() {
    Schema enumType = Schema.createEnum(Arrays.asList(
        new Schema.EnumValue("A"), new Schema.EnumValue("B"), new Schema.EnumValue("C")))
        .setNullable(false);
    Schema rootSchema = fieldOfTypeWithDefault(enumType, "B");
    LogicalType roundTripped = JsonToLogicalTypeConverter.toLogicalType(
        convertSchema(rootSchema));
    Field f = roundTripped.getRootSchema().getField("f");
    assertTrue(f.hasDefaultValue());
    assertEquals("B", f.getDefaultValue());
  }

  @Test
  void testRoundTripNullableEnumWithNonNullDefault() {
    Schema enumType = Schema.createEnum(Arrays.asList(
        new Schema.EnumValue("A"), new Schema.EnumValue("B"), new Schema.EnumValue("C")))
        .setNullable(true);
    Schema rootSchema = fieldOfTypeWithDefault(enumType, "B");
    LogicalType roundTripped = JsonToLogicalTypeConverter.toLogicalType(
        convertSchema(rootSchema));
    Field f = roundTripped.getRootSchema().getField("f");
    assertTrue(f.getSchema().isNullable());
    assertTrue(f.hasDefaultValue());
    assertEquals("B", f.getDefaultValue());
  }

  @Test
  void testDefaultOnVariantThrows() {
    assertThrows(ValidationException.class, () -> {
        Schema variantType = Schema.create(Schema.Type.VARIANT).setNullable(false);
        convertSchema(fieldOfTypeWithDefault(variantType, "anything"));
    });
  }

  // =========================================================================
  // MULTISET round-trip
  // =========================================================================

  @Test
  void testRoundTripMultisetOfInt() {
    Schema rootSchema = Schema.createStruct(Arrays.asList(
        new Field("counts",
            Schema.createMultiset(Schema.create(Schema.Type.INT).setNullable(false))
                .setNullable(false), 0)))
        .setNullable(false);
    LogicalType roundTripped = JsonToLogicalTypeConverter.toLogicalType(
        convertSchema(rootSchema));
    Schema rt = roundTripped.getRootSchema().getField("counts").getSchema();
    assertEquals(Schema.Type.MULTISET, rt.getType());
    assertEquals(Schema.Type.INT, rt.getElementType().getType());
  }

  @Test
  void testRoundTripMultisetNullable() {
    Schema rootSchema = Schema.createStruct(Arrays.asList(
        new Field("maybeCounts",
            Schema.createMultiset(Schema.create(Schema.Type.INT).setNullable(false))
                .setNullable(true), 0)))
        .setNullable(false);
    LogicalType roundTripped = JsonToLogicalTypeConverter.toLogicalType(
        convertSchema(rootSchema));
    Schema rt = roundTripped.getRootSchema().getField("maybeCounts").getSchema();
    assertEquals(Schema.Type.MULTISET, rt.getType());
    assertTrue(rt.isNullable());
  }

  // =========================================================================
  // DECIMAL precision/scale variations
  // =========================================================================

  @Test
  void testRoundTripDecimalVariations() {
    int[][] cases = {{5, 0}, {10, 2}, {38, 18}, {38, 0}};
    for (int[] ps : cases) {
      int precision = ps[0];
      int scale = ps[1];
      Schema rootSchema = Schema.createStruct(Arrays.asList(
          new Field("d", Schema.createDecimal(precision, scale).setNullable(false), 0)))
          .setNullable(false);
      LogicalType roundTripped = JsonToLogicalTypeConverter.toLogicalType(
          convertSchema(rootSchema));
      Schema rt = roundTripped.getRootSchema().getField("d").getSchema();
      assertEquals(Schema.Type.DECIMAL, rt.getType());
      assertEquals(precision, rt.getPrecision(), "precision " + precision);
      assertEquals(scale, rt.getScale(), "scale " + scale);
    }
  }

  // =========================================================================
  // Length-constrained primitives round-trip
  // =========================================================================

  @Test
  void testRoundTripCharVarcharLengths() {
    Schema rootSchema = Schema.createStruct(Arrays.asList(
        new Field("c", Schema.createChar(10).setNullable(false), 0),
        new Field("v", Schema.createVarchar(255).setNullable(false), 1)))
        .setNullable(false);
    LogicalType roundTripped = JsonToLogicalTypeConverter.toLogicalType(
        convertSchema(rootSchema));
    Schema c = roundTripped.getRootSchema().getField("c").getSchema();
    Schema v = roundTripped.getRootSchema().getField("v").getSchema();
    assertEquals(Schema.Type.CHAR, c.getType());
    assertEquals(10, c.getLength());
    assertEquals(Schema.Type.VARCHAR, v.getType());
    assertEquals(255, v.getLength());
  }

  @Test
  void testRoundTripBinaryVarbinaryLengths() {
    Schema rootSchema = Schema.createStruct(Arrays.asList(
        new Field("b", Schema.createBinary(8).setNullable(false), 0),
        new Field("vb", Schema.createVarbinary(100).setNullable(false), 1)))
        .setNullable(false);
    LogicalType roundTripped = JsonToLogicalTypeConverter.toLogicalType(
        convertSchema(rootSchema));
    Schema b = roundTripped.getRootSchema().getField("b").getSchema();
    Schema vb = roundTripped.getRootSchema().getField("vb").getSchema();
    assertEquals(Schema.Type.BINARY, b.getType());
    assertEquals(8, b.getLength());
    assertEquals(Schema.Type.VARBINARY, vb.getType());
    assertEquals(100, vb.getLength());
  }

  // =========================================================================
  // Recursive NAMED_TYPE_REF
  // =========================================================================

  @Test
  void testRoundTripRecursiveNamedTypeRef() {
    Schema treeType = Schema.createStruct(Arrays.asList(
        new Field("value", Schema.create(Schema.Type.INT).setNullable(false), 0),
        new Field("child",
            Schema.createNamedTypeRef("Tree").setNullable(true), 1)))
        .setNullable(false);
    Map<String, Schema> namedTypes = new LinkedHashMap<>();
    namedTypes.put("Tree", treeType);
    Schema rootSchema = Schema.createStruct(Arrays.asList(
        new Field("root",
            Schema.createNamedTypeRef("Tree").setNullable(false), 0)))
        .setNullable(false);
    JsonSchema json = LogicalTypeToJsonConverter.fromLogicalType(
        new LogicalType(rootSchema, namedTypes), "Holder");
    LogicalType roundTripped = JsonToLogicalTypeConverter.toLogicalType(json);
    Schema rootRef = roundTripped.getRootSchema().getField("root").getSchema();
    assertEquals(Schema.Type.NAMED_TYPE_REF, rootRef.getType());
    assertEquals("Tree", rootRef.getQualifiedName());
    Schema treeDef = roundTripped.getNamedTypes().get("Tree");
    assertNotNull(treeDef);
    Schema childField = treeDef.getField("child").getSchema();
    assertEquals(Schema.Type.NAMED_TYPE_REF, childField.getType());
    assertEquals("Tree", childField.getQualifiedName());
  }

  // =========================================================================
  // ARRAY of complex types
  // =========================================================================

  @Test
  void testRoundTripArrayOfStruct() {
    Schema elementType = Schema.createStruct(Arrays.asList(
        new Field("x", Schema.create(Schema.Type.INT).setNullable(false), 0),
        new Field("y", Schema.createString().setNullable(false), 1)))
        .setNullable(false);
    Schema rootSchema = Schema.createStruct(Arrays.asList(
        new Field("items",
            Schema.createArray(elementType).setNullable(false), 0)))
        .setNullable(false);
    LogicalType roundTripped = JsonToLogicalTypeConverter.toLogicalType(
        convertSchema(rootSchema));
    Schema rt = roundTripped.getRootSchema().getField("items").getSchema();
    assertEquals(Schema.Type.ARRAY, rt.getType());
    assertEquals(Schema.Type.STRUCT, rt.getElementType().getType());
    assertEquals(2, rt.getElementType().getFields().size());
  }

  @Test
  void testRoundTripArrayOfArray() {
    Schema rootSchema = Schema.createStruct(Arrays.asList(
        new Field("matrix",
            Schema.createArray(
                Schema.createArray(
                    Schema.create(Schema.Type.INT).setNullable(false))
                    .setNullable(false))
                .setNullable(false), 0)))
        .setNullable(false);
    LogicalType roundTripped = JsonToLogicalTypeConverter.toLogicalType(
        convertSchema(rootSchema));
    Schema rt = roundTripped.getRootSchema().getField("matrix").getSchema();
    assertEquals(Schema.Type.ARRAY, rt.getType());
    assertEquals(Schema.Type.ARRAY, rt.getElementType().getType());
    assertEquals(Schema.Type.INT, rt.getElementType().getElementType().getType());
  }

  @Test
  void testRoundTripArrayOfMap() {
    Schema mapType = Schema.createMap(
        Schema.createString().setNullable(false),
        Schema.create(Schema.Type.INT).setNullable(false)).setNullable(false);
    Schema rootSchema = Schema.createStruct(Arrays.asList(
        new Field("histograms",
            Schema.createArray(mapType).setNullable(false), 0)))
        .setNullable(false);
    LogicalType roundTripped = JsonToLogicalTypeConverter.toLogicalType(
        convertSchema(rootSchema));
    Schema rt = roundTripped.getRootSchema().getField("histograms").getSchema();
    assertEquals(Schema.Type.ARRAY, rt.getType());
    assertEquals(Schema.Type.MAP, rt.getElementType().getType());
  }

  @Test
  void testRoundTripArrayOfNamedTypeRef() {
    Schema addressType = Schema.createStruct(Arrays.asList(
        new Field("city", Schema.createString().setNullable(false), 0)))
        .setNullable(false);
    Map<String, Schema> namedTypes = new LinkedHashMap<>();
    namedTypes.put("Address", addressType);
    Schema rootSchema = Schema.createStruct(Arrays.asList(
        new Field("addrs",
            Schema.createArray(
                Schema.createNamedTypeRef("Address").setNullable(false))
                .setNullable(false), 0)))
        .setNullable(false);
    JsonSchema json = LogicalTypeToJsonConverter.fromLogicalType(
        new LogicalType(rootSchema, namedTypes), "Holder");
    LogicalType roundTripped = JsonToLogicalTypeConverter.toLogicalType(json);
    Schema rt = roundTripped.getRootSchema().getField("addrs").getSchema();
    assertEquals(Schema.Type.ARRAY, rt.getType());
    assertEquals(Schema.Type.NAMED_TYPE_REF, rt.getElementType().getType());
    assertEquals("Address", rt.getElementType().getQualifiedName());
  }

  // =========================================================================
  // MAP of complex value types
  // =========================================================================

  @Test
  void testRoundTripMapOfStruct() {
    Schema valueType = Schema.createStruct(Arrays.asList(
        new Field("x", Schema.create(Schema.Type.INT).setNullable(false), 0)))
        .setNullable(false);
    Schema rootSchema = Schema.createStruct(Arrays.asList(
        new Field("byKey",
            Schema.createMap(Schema.createString().setNullable(false), valueType)
                .setNullable(false), 0)))
        .setNullable(false);
    LogicalType roundTripped = JsonToLogicalTypeConverter.toLogicalType(
        convertSchema(rootSchema));
    Schema rt = roundTripped.getRootSchema().getField("byKey").getSchema();
    assertEquals(Schema.Type.MAP, rt.getType());
    assertEquals(Schema.Type.STRUCT, rt.getValueType().getType());
  }

  @Test
  void testRoundTripMapOfArray() {
    Schema rootSchema = Schema.createStruct(Arrays.asList(
        new Field("groupedItems",
            Schema.createMap(
                Schema.createString().setNullable(false),
                Schema.createArray(
                    Schema.create(Schema.Type.INT).setNullable(false))
                    .setNullable(false))
                .setNullable(false), 0)))
        .setNullable(false);
    LogicalType roundTripped = JsonToLogicalTypeConverter.toLogicalType(
        convertSchema(rootSchema));
    Schema rt = roundTripped.getRootSchema().getField("groupedItems").getSchema();
    assertEquals(Schema.Type.MAP, rt.getType());
    assertEquals(Schema.Type.ARRAY, rt.getValueType().getType());
  }

  @Test
  void testRoundTripMapOfNamedTypeRef() {
    Schema addressType = Schema.createStruct(Arrays.asList(
        new Field("city", Schema.createString().setNullable(false), 0)))
        .setNullable(false);
    Map<String, Schema> namedTypes = new LinkedHashMap<>();
    namedTypes.put("Address", addressType);
    Schema rootSchema = Schema.createStruct(Arrays.asList(
        new Field("addrByKey",
            Schema.createMap(
                Schema.createString().setNullable(false),
                Schema.createNamedTypeRef("Address").setNullable(false))
                .setNullable(false), 0)))
        .setNullable(false);
    JsonSchema json = LogicalTypeToJsonConverter.fromLogicalType(
        new LogicalType(rootSchema, namedTypes), "Holder");
    LogicalType roundTripped = JsonToLogicalTypeConverter.toLogicalType(json);
    Schema rt = roundTripped.getRootSchema().getField("addrByKey").getSchema();
    assertEquals(Schema.Type.MAP, rt.getType());
    assertEquals(Schema.Type.NAMED_TYPE_REF, rt.getValueType().getType());
  }

  @Test
  void testRoundTripMapWithIntKeyAndStructValue() {
    Schema valueType = Schema.createStruct(Arrays.asList(
        new Field("x", Schema.create(Schema.Type.INT).setNullable(false), 0)))
        .setNullable(false);
    Schema rootSchema = Schema.createStruct(Arrays.asList(
        new Field("byId",
            Schema.createMap(
                Schema.create(Schema.Type.INT).setNullable(false), valueType)
                .setNullable(false), 0)))
        .setNullable(false);
    LogicalType roundTripped = JsonToLogicalTypeConverter.toLogicalType(
        convertSchema(rootSchema));
    Schema rt = roundTripped.getRootSchema().getField("byId").getSchema();
    assertEquals(Schema.Type.MAP, rt.getType());
    assertEquals(Schema.Type.INT, rt.getKeyType().getType());
    assertEquals(Schema.Type.STRUCT, rt.getValueType().getType());
  }

  // =========================================================================
  // Field iteration order preserved
  // =========================================================================

  @Test
  void testRoundTripFieldIterationOrderPreserved() {
    Schema rootSchema = Schema.createStruct(Arrays.asList(
        new Field("c", Schema.createString().setNullable(false), 0),
        new Field("a", Schema.createString().setNullable(false), 1),
        new Field("b", Schema.createString().setNullable(false), 2)))
        .setNullable(false);
    LogicalType roundTripped = JsonToLogicalTypeConverter.toLogicalType(
        convertSchema(rootSchema));
    java.util.List<Field> fields = roundTripped.getRootSchema().getFields();
    assertEquals(3, fields.size());
    assertEquals("c", fields.get(0).getName());
    assertEquals("a", fields.get(1).getName());
    assertEquals("b", fields.get(2).getName());
  }

  // =========================================================================
  // Schema-level metadata at root
  // =========================================================================

  @Test
  void testRoundTripRootSchemaMetadata() {
    Schema rootSchema = Schema.createStruct(Arrays.asList(
        new Field("x", Schema.create(Schema.Type.INT).setNullable(false), 0)))
        .setNullable(false)
        .setDoc("Root struct doc")
        .setTags(Arrays.asList("PII", "internal"))
        .setParams(Map.of("owner", "alice"));
    LogicalType roundTripped = JsonToLogicalTypeConverter.toLogicalType(
        convertSchema(rootSchema));
    Schema rt = roundTripped.getRootSchema();
    assertEquals("Root struct doc", rt.getDoc());
    assertEquals(Arrays.asList("PII", "internal"), rt.getTags());
    assertEquals("alice", rt.getParams().get("owner"));
  }

  // =========================================================================
  // Top-level UNION
  // =========================================================================

  @Test
  void testRoundTripTopLevelUnion() {
    Schema rootSchema = Schema.createUnion(Arrays.asList(
        new UnionBranch("a", Schema.create(Schema.Type.INT).setNullable(false)),
        new UnionBranch("b", Schema.createString().setNullable(false))))
        .setNullable(false);
    LogicalType roundTripped = JsonToLogicalTypeConverter.toLogicalType(
        convertSchema(rootSchema));
    Schema rt = roundTripped.getRootSchema();
    assertEquals(Schema.Type.UNION, rt.getType());
    assertEquals(2, rt.getBranches().size());
    assertEquals("a", rt.getBranches().get(0).getName());
    assertEquals("b", rt.getBranches().get(1).getName());
    assertEquals(Schema.Type.INT, rt.getBranches().get(0).getSchema().getType());
    assertEquals(Schema.Type.VARCHAR, rt.getBranches().get(1).getSchema().getType());
  }

  @Test
  void testRoundTripExternalRefDuplicateFullNameThrows() {
    // Two distinct external schemas each declare a $defs entry keyed
    // "com.example.Color". Registering them via putConverted detects the
    // collision and throws.
    String first = "{\"type\":\"object\","
        + "\"$defs\":{\"com.example.Color\":{"
        + "\"type\":\"string\",\"enum\":[\"RED\",\"GREEN\"]}}}";
    String second = "{\"type\":\"object\","
        + "\"$defs\":{\"com.example.Color\":{"
        + "\"type\":\"string\",\"enum\":[\"BLUE\",\"YELLOW\"]}}}";

    List<SchemaReference> references = Arrays.asList(
        new SchemaReference("com.example.Outer1", "outer1", 1),
        new SchemaReference("com.example.Outer2", "outer2", 1));
    Map<String, String> resolvedReferences = new LinkedHashMap<>();
    resolvedReferences.put("com.example.Outer1", first);
    resolvedReferences.put("com.example.Outer2", second);

    Schema rootSchema = Schema.createStruct(Arrays.asList(
        new Field("favorite",
            Schema.createNamedTypeRef("com.example.Color").setNullable(true), 0)))
        .setNullable(false);

    LogicalType original = new LogicalType(rootSchema, Map.of(),
        references, resolvedReferences);

    assertThrows(ValidationException.class, () ->
        LogicalTypeToJsonConverter.fromLogicalType(original, "Holder"));
  }

  // =========================================================================
  // Nested external enum NAMED_TYPE_REF
  // =========================================================================

  @Test
  void testRoundTripExternalNestedEnumRef() {
    // External JSON schema with a $defs entry keyed by qualified name —
    // our convention for addressable nested types.
    String externalSchema = "{\"type\":\"object\","
        + "\"$defs\":{\"com.example.Color\":{"
        + "\"type\":\"string\",\"enum\":[\"RED\",\"GREEN\",\"BLUE\"]}}}";

    List<SchemaReference> references = Arrays.asList(
        new SchemaReference("com.example.Outer", "outer-value", 1));
    Map<String, String> resolvedReferences = new LinkedHashMap<>();
    resolvedReferences.put("com.example.Outer", externalSchema);

    Schema rootSchema = Schema.createStruct(Arrays.asList(
        new Field("favorite",
            Schema.createNamedTypeRef("com.example.Color").setNullable(true), 0)))
        .setNullable(false);

    LogicalType original = new LogicalType(rootSchema, Map.of(),
        references, resolvedReferences);
    JsonSchema json = LogicalTypeToJsonConverter.fromLogicalType(original, "Holder");
    LogicalType roundTripped = JsonToLogicalTypeConverter.toLogicalType(json);
    Schema fav = roundTripped.getRootSchema().getField("favorite").getSchema();
    assertEquals(Schema.Type.NAMED_TYPE_REF, fav.getType());
    assertEquals("com.example.Color", fav.getQualifiedName());
  }

  // testRoundTripDefaultNullOnNullablePrimitive: removed.
  // JSON Schema's `default` keyword being absent vs explicitly null isn't
  // distinguishable through the current writer's emit path (no value emitted).

  // =========================================================================
  // MULTISET inside collections
  // =========================================================================

  @Test
  void testRoundTripArrayOfMultiset() {
    Schema multisetType = Schema.createMultiset(
        Schema.create(Schema.Type.INT).setNullable(false))
        .setNullable(false);
    Schema rootSchema = Schema.createStruct(Arrays.asList(
        new Field("counts",
            Schema.createArray(multisetType).setNullable(false), 0)))
        .setNullable(false);
    LogicalType roundTripped = JsonToLogicalTypeConverter.toLogicalType(
        convertSchema(rootSchema));
    Schema rt = roundTripped.getRootSchema().getField("counts").getSchema();
    assertEquals(Schema.Type.ARRAY, rt.getType());
    assertEquals(Schema.Type.MULTISET, rt.getElementType().getType());
  }

  @Test
  void testRoundTripMapOfMultiset() {
    Schema multisetType = Schema.createMultiset(
        Schema.create(Schema.Type.INT).setNullable(false))
        .setNullable(false);
    Schema rootSchema = Schema.createStruct(Arrays.asList(
        new Field("byKey",
            Schema.createMap(
                Schema.createString().setNullable(false), multisetType)
                .setNullable(false), 0)))
        .setNullable(false);
    LogicalType roundTripped = JsonToLogicalTypeConverter.toLogicalType(
        convertSchema(rootSchema));
    Schema rt = roundTripped.getRootSchema().getField("byKey").getSchema();
    assertEquals(Schema.Type.MAP, rt.getType());
    assertEquals(Schema.Type.MULTISET, rt.getValueType().getType());
  }

  // =========================================================================
  // Nullable primitive types
  // =========================================================================

  @Test
  void testRoundTripNullablePrimitives() {
    Schema rootSchema = Schema.createStruct(Arrays.asList(
        new Field("nb", Schema.create(Schema.Type.BOOLEAN).setNullable(true), 0),
        new Field("ni", Schema.create(Schema.Type.INT).setNullable(true), 1),
        new Field("nl", Schema.create(Schema.Type.BIGINT).setNullable(true), 2),
        new Field("nf", Schema.create(Schema.Type.FLOAT).setNullable(true), 3),
        new Field("nd", Schema.create(Schema.Type.DOUBLE).setNullable(true), 4),
        new Field("ns", Schema.createString().setNullable(true), 5),
        new Field("nbn", Schema.createBytes().setNullable(true), 6),
        new Field("ndec", Schema.createDecimal(10, 2).setNullable(true), 7),
        new Field("ndt", Schema.create(Schema.Type.DATE).setNullable(true), 8),
        new Field("nts", Schema.createTimestamp(6).setNullable(true), 9)))
        .setNullable(false);
    LogicalType roundTripped = JsonToLogicalTypeConverter.toLogicalType(
        convertSchema(rootSchema));
    Schema rt = roundTripped.getRootSchema();
    for (Field f : rt.getFields()) {
      assertTrue(f.getSchema().isNullable(), "field " + f.getName() + " should be nullable");
    }
  }

  // =========================================================================
  // MULTISET of complex types
  // =========================================================================

  @Test
  void testRoundTripMultisetOfStruct() {
    Schema elementType = Schema.createStruct(Arrays.asList(
        new Field("x", Schema.create(Schema.Type.INT).setNullable(false), 0)))
        .setNullable(false);
    Schema rootSchema = Schema.createStruct(Arrays.asList(
        new Field("counts",
            Schema.createMultiset(elementType).setNullable(false), 0)))
        .setNullable(false);
    LogicalType roundTripped = JsonToLogicalTypeConverter.toLogicalType(
        convertSchema(rootSchema));
    Schema rt = roundTripped.getRootSchema().getField("counts").getSchema();
    assertEquals(Schema.Type.MULTISET, rt.getType());
    assertEquals(Schema.Type.STRUCT, rt.getElementType().getType());
  }

  @Test
  void testRoundTripMultisetOfNamedTypeRef() {
    Schema addressType = Schema.createStruct(Arrays.asList(
        new Field("city", Schema.createString().setNullable(false), 0)))
        .setNullable(false);
    Map<String, Schema> namedTypes = new LinkedHashMap<>();
    namedTypes.put("Address", addressType);
    Schema rootSchema = Schema.createStruct(Arrays.asList(
        new Field("addrs",
            Schema.createMultiset(
                Schema.createNamedTypeRef("Address").setNullable(false))
                .setNullable(false), 0)))
        .setNullable(false);
    JsonSchema json = LogicalTypeToJsonConverter.fromLogicalType(
        new LogicalType(rootSchema, namedTypes), "Holder");
    LogicalType roundTripped = JsonToLogicalTypeConverter.toLogicalType(json);
    Schema rt = roundTripped.getRootSchema().getField("addrs").getSchema();
    assertEquals(Schema.Type.MULTISET, rt.getType());
    assertEquals(Schema.Type.NAMED_TYPE_REF, rt.getElementType().getType());
  }

  // =========================================================================
  // VARIANT in non-trivial positions
  // =========================================================================

  @Test
  void testRoundTripVariantInArray() {
    Schema rootSchema = Schema.createStruct(Arrays.asList(
        new Field("vs",
            Schema.createArray(
                Schema.create(Schema.Type.VARIANT).setNullable(false))
                .setNullable(false), 0)))
        .setNullable(false);
    LogicalType roundTripped = JsonToLogicalTypeConverter.toLogicalType(
        convertSchema(rootSchema));
    Schema rt = roundTripped.getRootSchema().getField("vs").getSchema();
    assertEquals(Schema.Type.ARRAY, rt.getType());
    assertEquals(Schema.Type.VARIANT, rt.getElementType().getType());
  }

  @Test
  void testRoundTripVariantInMap() {
    Schema rootSchema = Schema.createStruct(Arrays.asList(
        new Field("vm",
            Schema.createMap(
                Schema.createString().setNullable(false),
                Schema.create(Schema.Type.VARIANT).setNullable(false))
                .setNullable(false), 0)))
        .setNullable(false);
    LogicalType roundTripped = JsonToLogicalTypeConverter.toLogicalType(
        convertSchema(rootSchema));
    Schema rt = roundTripped.getRootSchema().getField("vm").getSchema();
    assertEquals(Schema.Type.MAP, rt.getType());
    assertEquals(Schema.Type.VARIANT, rt.getValueType().getType());
  }

  @Test
  void testRoundTripVariantInUnionBranch() {
    Schema rootSchema = Schema.createStruct(Arrays.asList(
        new Field("u",
            Schema.createUnion(Arrays.asList(
                new UnionBranch("vbranch",
                    Schema.create(Schema.Type.VARIANT).setNullable(false)),
                new UnionBranch("sbranch",
                    Schema.createString().setNullable(false))))
                .setNullable(false), 0)))
        .setNullable(false);
    LogicalType roundTripped = JsonToLogicalTypeConverter.toLogicalType(
        convertSchema(rootSchema));
    Schema rt = roundTripped.getRootSchema().getField("u").getSchema();
    assertEquals(Schema.Type.UNION, rt.getType());
    assertEquals(Schema.Type.VARIANT, rt.getBranches().get(0).getSchema().getType());
  }

  // =========================================================================
  // ENUM in non-trivial positions
  // =========================================================================

  @Test
  void testRoundTripEnumInArray() {
    Schema enumType = Schema.createEnum(Arrays.asList(
        new Schema.EnumValue("A"), new Schema.EnumValue("B"), new Schema.EnumValue("C")))
        .setNullable(false);
    Map<String, Schema> namedTypes = new LinkedHashMap<>();
    namedTypes.put("Color", enumType);
    Schema rootSchema = Schema.createStruct(Arrays.asList(
        new Field("colors",
            Schema.createArray(
                Schema.createNamedTypeRef("Color").setNullable(false))
                .setNullable(false), 0)))
        .setNullable(false);
    JsonSchema json = LogicalTypeToJsonConverter.fromLogicalType(
        new LogicalType(rootSchema, namedTypes), "Holder");
    LogicalType roundTripped = JsonToLogicalTypeConverter.toLogicalType(json);
    Schema rt = roundTripped.getRootSchema().getField("colors").getSchema();
    assertEquals(Schema.Type.ARRAY, rt.getType());
    assertEquals(Schema.Type.NAMED_TYPE_REF, rt.getElementType().getType());
  }

  @Test
  void testRoundTripEnumInMapValue() {
    Schema enumType = Schema.createEnum(Arrays.asList(
        new Schema.EnumValue("RED"), new Schema.EnumValue("GREEN")))
        .setNullable(false);
    Map<String, Schema> namedTypes = new LinkedHashMap<>();
    namedTypes.put("Color", enumType);
    Schema rootSchema = Schema.createStruct(Arrays.asList(
        new Field("colorMap",
            Schema.createMap(
                Schema.createString().setNullable(false),
                Schema.createNamedTypeRef("Color").setNullable(false))
                .setNullable(false), 0)))
        .setNullable(false);
    JsonSchema json = LogicalTypeToJsonConverter.fromLogicalType(
        new LogicalType(rootSchema, namedTypes), "Holder");
    LogicalType roundTripped = JsonToLogicalTypeConverter.toLogicalType(json);
    Schema rt = roundTripped.getRootSchema().getField("colorMap").getSchema();
    assertEquals(Schema.Type.MAP, rt.getType());
    assertEquals(Schema.Type.NAMED_TYPE_REF, rt.getValueType().getType());
  }

  @Test
  void testRoundTripEnumInUnionBranch() {
    Schema enumType = Schema.createEnum(Arrays.asList(
        new Schema.EnumValue("YES"), new Schema.EnumValue("NO")))
        .setNullable(false);
    Map<String, Schema> namedTypes = new LinkedHashMap<>();
    namedTypes.put("Answer", enumType);
    Schema rootSchema = Schema.createStruct(Arrays.asList(
        new Field("u",
            Schema.createUnion(Arrays.asList(
                new UnionBranch("ebranch",
                    Schema.createNamedTypeRef("Answer").setNullable(false)),
                new UnionBranch("sbranch",
                    Schema.createString().setNullable(false))))
                .setNullable(false), 0)))
        .setNullable(false);
    JsonSchema json = LogicalTypeToJsonConverter.fromLogicalType(
        new LogicalType(rootSchema, namedTypes), "Holder");
    LogicalType roundTripped = JsonToLogicalTypeConverter.toLogicalType(json);
    Schema rt = roundTripped.getRootSchema().getField("u").getSchema();
    assertEquals(Schema.Type.UNION, rt.getType());
    assertEquals(Schema.Type.NAMED_TYPE_REF,
        rt.getBranches().get(0).getSchema().getType());
  }

  // =========================================================================
  // UNION nested in collections
  // =========================================================================

  @Test
  void testRoundTripUnionInArray() {
    Schema unionType = Schema.createUnion(Arrays.asList(
        new UnionBranch("a", Schema.create(Schema.Type.INT).setNullable(false)),
        new UnionBranch("b", Schema.createString().setNullable(false))))
        .setNullable(false);
    Schema rootSchema = Schema.createStruct(Arrays.asList(
        new Field("us",
            Schema.createArray(unionType).setNullable(false), 0)))
        .setNullable(false);
    LogicalType roundTripped = JsonToLogicalTypeConverter.toLogicalType(
        convertSchema(rootSchema));
    Schema rt = roundTripped.getRootSchema().getField("us").getSchema();
    assertEquals(Schema.Type.ARRAY, rt.getType());
    assertEquals(Schema.Type.UNION, rt.getElementType().getType());
    assertEquals(2, rt.getElementType().getBranches().size());
  }

  @Test
  void testRoundTripUnionInMapValue() {
    Schema unionType = Schema.createUnion(Arrays.asList(
        new UnionBranch("a", Schema.create(Schema.Type.INT).setNullable(false)),
        new UnionBranch("b", Schema.createString().setNullable(false))))
        .setNullable(false);
    Schema rootSchema = Schema.createStruct(Arrays.asList(
        new Field("um",
            Schema.createMap(
                Schema.createString().setNullable(false), unionType)
                .setNullable(false), 0)))
        .setNullable(false);
    LogicalType roundTripped = JsonToLogicalTypeConverter.toLogicalType(
        convertSchema(rootSchema));
    Schema rt = roundTripped.getRootSchema().getField("um").getSchema();
    assertEquals(Schema.Type.MAP, rt.getType());
    assertEquals(Schema.Type.UNION, rt.getValueType().getType());
  }

  // =========================================================================
  // Nullable complex types
  // =========================================================================

  @Test
  void testRoundTripNullableComplexTypes() {
    Schema namedTypeRef = Schema.createNamedTypeRef("Inner").setNullable(true);
    Map<String, Schema> namedTypes = new LinkedHashMap<>();
    namedTypes.put("Inner", Schema.createStruct(Arrays.asList(
        new Field("x", Schema.create(Schema.Type.INT).setNullable(false), 0)))
        .setNullable(false));
    Schema rootSchema = Schema.createStruct(Arrays.asList(
        new Field("nullableArr",
            Schema.createArray(Schema.create(Schema.Type.INT).setNullable(false))
                .setNullable(true), 0),
        new Field("nullableMap",
            Schema.createMap(Schema.createString().setNullable(false),
                Schema.create(Schema.Type.INT).setNullable(false)).setNullable(true), 1),
        new Field("nullableStruct",
            Schema.createStruct(Arrays.asList(
                new Field("s", Schema.createString().setNullable(false), 0)))
                .setNullable(true), 2),
        new Field("nullableRef", namedTypeRef, 3)))
        .setNullable(false);
    LogicalType roundTripped = JsonToLogicalTypeConverter.toLogicalType(
        LogicalTypeToJsonConverter.fromLogicalType(
            new LogicalType(rootSchema, namedTypes), "Holder"));
    Schema rt = roundTripped.getRootSchema();
    assertTrue(rt.getField("nullableArr").getSchema().isNullable());
    assertTrue(rt.getField("nullableMap").getSchema().isNullable());
    assertTrue(rt.getField("nullableStruct").getSchema().isNullable());
    assertTrue(rt.getField("nullableRef").getSchema().isNullable());
  }

  // =========================================================================
  // TINYINT / SMALLINT round-trip
  // =========================================================================

  @Test
  void testRoundTripTinyintSmallint() {
    Schema rootSchema = Schema.createStruct(Arrays.asList(
        new Field("tiny", Schema.create(Schema.Type.TINYINT).setNullable(false), 0),
        new Field("small", Schema.create(Schema.Type.SMALLINT).setNullable(false), 1)))
        .setNullable(false);
    LogicalType roundTripped = JsonToLogicalTypeConverter.toLogicalType(
        convertSchema(rootSchema));
    Schema rt = roundTripped.getRootSchema();
    assertEquals(Schema.Type.TINYINT, rt.getField("tiny").getSchema().getType());
    assertEquals(Schema.Type.SMALLINT, rt.getField("small").getSchema().getType());
  }

  // =========================================================================
  // Exhaustive primitives
  // =========================================================================

  @Test
  void testRoundTripPrimitivesExhaustive() {
    Schema[] primitives = new Schema[] {
        Schema.create(Schema.Type.BOOLEAN).setNullable(false),
        Schema.create(Schema.Type.TINYINT).setNullable(false),
        Schema.create(Schema.Type.SMALLINT).setNullable(false),
        Schema.create(Schema.Type.INT).setNullable(false),
        Schema.create(Schema.Type.BIGINT).setNullable(false),
        Schema.create(Schema.Type.FLOAT).setNullable(false),
        Schema.create(Schema.Type.DOUBLE).setNullable(false),
        Schema.createDecimal(15, 5).setNullable(false),
        Schema.createString().setNullable(false),
        Schema.createVarchar(50).setNullable(false),
        Schema.createChar(10).setNullable(false),
        Schema.createBytes().setNullable(false),
        Schema.createVarbinary(50).setNullable(false),
        Schema.createBinary(10).setNullable(false),
        Schema.create(Schema.Type.DATE).setNullable(false),
        Schema.createTime(3).setNullable(false),
        Schema.createTimestamp(6).setNullable(false),
        Schema.createTimestampLtz(6).setNullable(false),
    };
    for (Schema p : primitives) {
      Schema original = Schema.createStruct(Arrays.asList(
          new Field("f", p, 0))).setNullable(false);
      Schema roundTripped = JsonToLogicalTypeConverter.toRootSchema(
          convertSchema(original));
      assertEquals(original, roundTripped, "Round trip failed for " + p.getType());
    }
  }

  // =========================================================================
  // Default-value round-trip — all primitive types
  // =========================================================================

  @Test
  void testRoundTripDefaultValuesAllPrimitives() {
    Schema rootSchema = Schema.createStruct(Arrays.asList(
        new Field("b", Schema.create(Schema.Type.BOOLEAN).setNullable(false), 0,
            Boolean.TRUE, true, null, null, null),
        new Field("ti", Schema.create(Schema.Type.TINYINT).setNullable(false), 1,
            (byte) 7, true, null, null, null),
        new Field("si", Schema.create(Schema.Type.SMALLINT).setNullable(false), 2,
            (short) 1234, true, null, null, null),
        new Field("i", Schema.create(Schema.Type.INT).setNullable(false), 3,
            42, true, null, null, null),
        new Field("bi", Schema.create(Schema.Type.BIGINT).setNullable(false), 4,
            123456789012L, true, null, null, null),
        new Field("fl", Schema.create(Schema.Type.FLOAT).setNullable(false), 5,
            1.5f, true, null, null, null),
        new Field("db", Schema.create(Schema.Type.DOUBLE).setNullable(false), 6,
            2.5d, true, null, null, null),
        new Field("ch", Schema.createChar(10).setNullable(false), 7,
            "hello", true, null, null, null),
        new Field("vc", Schema.createString().setNullable(false), 8,
            "world", true, null, null, null),
        new Field("bin", Schema.createBinary(3).setNullable(false), 9,
            new byte[]{1, 2, 3}, true, null, null, null),
        new Field("vb", Schema.createVarbinary(10).setNullable(false), 10,
            new byte[]{4, 5, 6}, true, null, null, null),
        new Field("dec", Schema.createDecimal(10, 2).setNullable(false), 11,
            new java.math.BigDecimal("12.34"), true, null, null, null),
        new Field("dt", Schema.create(Schema.Type.DATE).setNullable(false), 12,
            java.time.LocalDate.of(2026, 4, 17), true, null, null, null),
        new Field("tm", Schema.createTime(3).setNullable(false), 13,
            java.time.LocalTime.of(12, 34, 56), true, null, null, null),
        new Field("ts", Schema.createTimestamp(6).setNullable(false), 14,
            java.time.LocalDateTime.of(2026, 4, 17, 12, 34, 56), true, null, null, null),
        new Field("tsltz", Schema.createTimestampLtz(3).setNullable(false), 15,
            java.time.Instant.ofEpochMilli(1700000000000L), true, null, null, null)))
        .setNullable(false);

    LogicalType original = new LogicalType(rootSchema);
    LogicalType roundTripped = JsonToLogicalTypeConverter.toLogicalType(
        LogicalTypeToJsonConverter.fromLogicalType(original, "Holder"));
    Schema rt = roundTripped.getRootSchema();
    assertEquals(Boolean.TRUE, rt.getField("b").getDefaultValue());
    assertEquals((byte) 7, rt.getField("ti").getDefaultValue());
    assertEquals((short) 1234, rt.getField("si").getDefaultValue());
    assertEquals(42, rt.getField("i").getDefaultValue());
    assertEquals(123456789012L, rt.getField("bi").getDefaultValue());
    assertEquals(1.5f, rt.getField("fl").getDefaultValue());
    assertEquals(2.5d, rt.getField("db").getDefaultValue());
    assertEquals("hello", rt.getField("ch").getDefaultValue());
    assertEquals("world", rt.getField("vc").getDefaultValue());
    assertArrayEquals(new byte[]{1, 2, 3}, (byte[]) rt.getField("bin").getDefaultValue());
    assertArrayEquals(new byte[]{4, 5, 6}, (byte[]) rt.getField("vb").getDefaultValue());
    assertEquals(new java.math.BigDecimal("12.34"), rt.getField("dec").getDefaultValue());
    assertEquals(java.time.LocalDate.of(2026, 4, 17), rt.getField("dt").getDefaultValue());
    assertEquals(java.time.LocalTime.of(12, 34, 56), rt.getField("tm").getDefaultValue());
    assertEquals(java.time.LocalDateTime.of(2026, 4, 17, 12, 34, 56),
        rt.getField("ts").getDefaultValue());
    assertEquals(java.time.Instant.ofEpochMilli(1700000000000L),
        rt.getField("tsltz").getDefaultValue());
  }

  @Test
  void testRoundTripDefaultValuesNumericEdges() {
    Schema rootSchema = Schema.createStruct(Arrays.asList(
        new Field("imin", Schema.create(Schema.Type.INT).setNullable(false), 0,
            Integer.MIN_VALUE, true, null, null, null),
        new Field("imax", Schema.create(Schema.Type.INT).setNullable(false), 1,
            Integer.MAX_VALUE, true, null, null, null),
        new Field("lmin", Schema.create(Schema.Type.BIGINT).setNullable(false), 2,
            Long.MIN_VALUE, true, null, null, null),
        new Field("lmax", Schema.create(Schema.Type.BIGINT).setNullable(false), 3,
            Long.MAX_VALUE, true, null, null, null),
        new Field("negDec", Schema.createDecimal(12, 4).setNullable(false), 4,
            new java.math.BigDecimal("-12345.6789"), true, null, null, null),
        new Field("smallDec", Schema.createDecimal(8, 6).setNullable(false), 5,
            new java.math.BigDecimal("0.000001"), true, null, null, null)))
        .setNullable(false);

    LogicalType original = new LogicalType(rootSchema);
    LogicalType roundTripped = JsonToLogicalTypeConverter.toLogicalType(
        LogicalTypeToJsonConverter.fromLogicalType(original, "Holder"));
    Schema rt = roundTripped.getRootSchema();
    assertEquals(Integer.MIN_VALUE, rt.getField("imin").getDefaultValue());
    assertEquals(Integer.MAX_VALUE, rt.getField("imax").getDefaultValue());
    assertEquals(Long.MIN_VALUE, rt.getField("lmin").getDefaultValue());
    assertEquals(Long.MAX_VALUE, rt.getField("lmax").getDefaultValue());
    assertEquals(new java.math.BigDecimal("-12345.6789"),
        rt.getField("negDec").getDefaultValue());
    assertEquals(new java.math.BigDecimal("0.000001"),
        rt.getField("smallDec").getDefaultValue());
  }

  // =========================================================================
  // Map with various non-string key types
  // =========================================================================

  @Test
  void testRoundTripMapWithBigintKey() {
    Schema mapType = Schema.createMap(
        Schema.create(Schema.Type.BIGINT).setNullable(false),
        Schema.createString().setNullable(false))
        .setNullable(false);
    Schema rootSchema = Schema.createStruct(Arrays.asList(
        new Field("m", mapType, 0)))
        .setNullable(false);
    LogicalType roundTripped = JsonToLogicalTypeConverter.toLogicalType(
        convertSchema(rootSchema));
    Schema rt = roundTripped.getRootSchema().getField("m").getSchema();
    assertEquals(Schema.Type.MAP, rt.getType());
    assertEquals(Schema.Type.BIGINT, rt.getKeyType().getType());
  }

  @Test
  void testRoundTripMapWithBooleanKey() {
    Schema mapType = Schema.createMap(
        Schema.create(Schema.Type.BOOLEAN).setNullable(false),
        Schema.createString().setNullable(false))
        .setNullable(false);
    Schema rootSchema = Schema.createStruct(Arrays.asList(
        new Field("m", mapType, 0)))
        .setNullable(false);
    LogicalType roundTripped = JsonToLogicalTypeConverter.toLogicalType(
        convertSchema(rootSchema));
    Schema rt = roundTripped.getRootSchema().getField("m").getSchema();
    assertEquals(Schema.Type.MAP, rt.getType());
    assertEquals(Schema.Type.BOOLEAN, rt.getKeyType().getType());
  }

  // =========================================================================
  // Multi-level external references (root -> external A -> external B)
  // =========================================================================

  @Test
  void testRoundTripMultiLevelExternalRefs() {
    // External B defines com.example.Foo as a $defs entry
    String externalB = "{\"type\":\"object\","
        + "\"$defs\":{\"com.example.Foo\":{"
        + "\"type\":\"object\","
        + "\"properties\":{\"x\":{\"type\":\"integer\"}}}}}";
    // External A defines com.example.Bar as a $defs entry
    String externalA = "{\"type\":\"object\","
        + "\"$defs\":{\"com.example.Bar\":{"
        + "\"type\":\"object\","
        + "\"properties\":{\"foo\":{\"type\":\"object\"}}}}}";

    List<SchemaReference> references = Arrays.asList(
        new SchemaReference("com.example.B", "b-value", 1),
        new SchemaReference("com.example.A", "a-value", 1));
    Map<String, String> resolvedReferences = new LinkedHashMap<>();
    resolvedReferences.put("com.example.B", externalB);
    resolvedReferences.put("com.example.A", externalA);

    Schema rootSchema = Schema.createStruct(Arrays.asList(
        new Field("bar",
            Schema.createNamedTypeRef("com.example.Bar").setNullable(true), 0)))
        .setNullable(false);

    LogicalType original = new LogicalType(rootSchema, Map.of(),
        references, resolvedReferences);
    JsonSchema json = LogicalTypeToJsonConverter.fromLogicalType(original, "Holder");
    LogicalType roundTripped = JsonToLogicalTypeConverter.toLogicalType(json);
    Schema barRef = roundTripped.getRootSchema().getField("bar").getSchema();
    assertEquals(Schema.Type.NAMED_TYPE_REF, barRef.getType());
    assertEquals("com.example.Bar", barRef.getQualifiedName());
  }

  // =========================================================================
  // Doc/tags/params on STRUCT used as a union branch
  // =========================================================================

  @Test
  void testRoundTripStructInUnionBranchPreservesMetadata() {
    Map<String, Object> innerParams = new LinkedHashMap<>();
    innerParams.put("k", "v");
    Schema innerStruct = Schema.createStruct(Arrays.asList(
        new Field("x", Schema.create(Schema.Type.INT).setNullable(false), 0)))
        .setNullable(false)
        .setDoc("inner doc")
        .setTags(Arrays.asList("inner-tag"))
        .setParams(innerParams);
    Schema union = Schema.createUnion(Arrays.asList(
        new Schema.UnionBranch("a", innerStruct),
        new Schema.UnionBranch("b", Schema.createString().setNullable(false))))
        .setNullable(false);
    Schema rootSchema = Schema.createStruct(Arrays.asList(
        new Field("u", union, 0)))
        .setNullable(false);
    LogicalType roundTripped = JsonToLogicalTypeConverter.toLogicalType(
        convertSchema(rootSchema));
    Schema rtBranch = roundTripped.getRootSchema().getField("u").getSchema()
        .getBranches().get(0).getSchema();
    assertEquals(Schema.Type.STRUCT, rtBranch.getType());
    assertEquals("inner doc", rtBranch.getDoc());
    assertEquals(Arrays.asList("inner-tag"), rtBranch.getTags());
    assertEquals("v", rtBranch.getParams().get("k"));
  }

  // =========================================================================
  // VARIANT at root
  // =========================================================================

  @Test
  void testRoundTripVariantAtRoot() {
    Schema rootVariant = Schema.create(Schema.Type.VARIANT).setNullable(false);
    Schema roundTripped = JsonToLogicalTypeConverter.toRootSchema(
        LogicalTypeToJsonConverter.fromLogicalType(new LogicalType(rootVariant), "Holder"));
    assertEquals(Schema.Type.VARIANT, roundTripped.getType());
    assertFalse(roundTripped.isNullable());
  }

  // =========================================================================
  // Tags + params combined on the same field/struct
  // =========================================================================

  @Test
  void testRoundTripFieldWithBothTagsAndParams() {
    Map<String, Object> fieldParams = new LinkedHashMap<>();
    fieldParams.put("sensitivity", "high");
    fieldParams.put("owner", "alice");
    Schema rootSchema = Schema.createStruct(Arrays.asList(
        new Field("email", Schema.createString().setNullable(false), 0,
            null, false, null, Arrays.asList("PII", "EMAIL"), fieldParams)))
        .setNullable(false);
    LogicalType roundTripped = JsonToLogicalTypeConverter.toLogicalType(
        convertSchema(rootSchema));
    Field f = roundTripped.getRootSchema().getField("email");
    assertEquals(Arrays.asList("PII", "EMAIL"), f.getTags());
    assertEquals("high", f.getParams().get("sensitivity"));
    assertEquals("alice", f.getParams().get("owner"));
  }

  @Test
  void testRoundTripStructWithBothSchemaTagsAndParams() {
    Map<String, Object> schemaParams = new LinkedHashMap<>();
    schemaParams.put("version", "2");
    schemaParams.put("source", "api");
    Schema rootSchema = Schema.createStruct(Arrays.asList(
        new Field("x", Schema.create(Schema.Type.INT).setNullable(false), 0)))
        .setNullable(false)
        .setTags(Arrays.asList("PII", "INTERNAL"))
        .setParams(schemaParams);
    LogicalType roundTripped = JsonToLogicalTypeConverter.toLogicalType(
        convertSchema(rootSchema));
    Schema rt = roundTripped.getRootSchema();
    assertEquals(Arrays.asList("PII", "INTERNAL"), rt.getTags());
    assertEquals("2", rt.getParams().get("version"));
    assertEquals("api", rt.getParams().get("source"));
  }

  // =========================================================================
  // Defaults on UNION-typed fields
  // =========================================================================

  @Test
  void testRoundTripUnionDefault() {
    Schema unionType = Schema.createUnion(Arrays.asList(
        new Schema.UnionBranch("i", Schema.create(Schema.Type.INT).setNullable(false)),
        new Schema.UnionBranch("s", Schema.createString().setNullable(false))))
        .setNullable(false);
    Schema rootSchema = Schema.createStruct(Arrays.asList(
        new Field("u", unionType, 0, 5, true, null, null, null)))
        .setNullable(false);
    LogicalType roundTripped = JsonToLogicalTypeConverter.toLogicalType(
        LogicalTypeToJsonConverter.fromLogicalType(
            new LogicalType(rootSchema), "Holder"));
    Field f = roundTripped.getRootSchema().getField("u");
    assertTrue(f.hasDefaultValue());
    assertEquals(5, f.getDefaultValue());
  }

  @Test
  void testRoundTripNullableUnionWithNonNullDefault() {
    Schema unionType = Schema.createUnion(Arrays.asList(
        new Schema.UnionBranch("i", Schema.create(Schema.Type.INT).setNullable(false)),
        new Schema.UnionBranch("s", Schema.createString().setNullable(false))))
        .setNullable(true);
    Schema rootSchema = Schema.createStruct(Arrays.asList(
        new Field("u", unionType, 0, 5, true, null, null, null)))
        .setNullable(false);
    LogicalType roundTripped = JsonToLogicalTypeConverter.toLogicalType(
        LogicalTypeToJsonConverter.fromLogicalType(
            new LogicalType(rootSchema), "Holder"));
    Field f = roundTripped.getRootSchema().getField("u");
    assertTrue(f.getSchema().isNullable());
    assertTrue(f.hasDefaultValue());
    assertEquals(5, f.getDefaultValue());
  }

  // =========================================================================
  // MAP with union-typed key
  // =========================================================================

  @Test
  void testRoundTripMapWithUnionKey() {
    Schema unionType = Schema.createUnion(Arrays.asList(
        new Schema.UnionBranch("i", Schema.create(Schema.Type.INT).setNullable(false)),
        new Schema.UnionBranch("s", Schema.createString().setNullable(false))))
        .setNullable(false);
    Schema mapType = Schema.createMap(
        unionType, Schema.createString().setNullable(false))
        .setNullable(false);
    Schema rootSchema = Schema.createStruct(Arrays.asList(
        new Field("m", mapType, 0)))
        .setNullable(false);
    LogicalType roundTripped = JsonToLogicalTypeConverter.toLogicalType(
        convertSchema(rootSchema));
    Schema rt = roundTripped.getRootSchema().getField("m").getSchema();
    assertEquals(Schema.Type.MAP, rt.getType());
    assertEquals(Schema.Type.UNION, rt.getKeyType().getType());
    assertEquals(2, rt.getKeyType().getBranches().size());
    assertEquals(Schema.Type.VARCHAR, rt.getValueType().getType());
  }

  // =========================================================================
  // Multiset of union (multiset-of-struct and multiset-of-named-type-ref
  // already covered above)
  // =========================================================================

  @Test
  void testRoundTripMultisetOfUnion() {
    Schema unionType = Schema.createUnion(Arrays.asList(
        new Schema.UnionBranch("i", Schema.create(Schema.Type.INT).setNullable(false)),
        new Schema.UnionBranch("s", Schema.createString().setNullable(false))))
        .setNullable(false);
    Schema rootSchema = Schema.createStruct(Arrays.asList(
        new Field("ms",
            Schema.createMultiset(unionType).setNullable(false), 0)))
        .setNullable(false);
    LogicalType roundTripped = JsonToLogicalTypeConverter.toLogicalType(
        convertSchema(rootSchema));
    Schema rt = roundTripped.getRootSchema().getField("ms").getSchema();
    assertEquals(Schema.Type.MULTISET, rt.getType());
    assertEquals(Schema.Type.UNION, rt.getElementType().getType());
    assertEquals(2, rt.getElementType().getBranches().size());
  }

  // =========================================================================
  // Namespace round-trip
  // =========================================================================

  @Test
  void testRoundTripNamespace() {
    Schema rootSchema = Schema.createStruct(Arrays.asList(
        new Field("x", Schema.create(Schema.Type.INT).setNullable(false), 0)))
        .setNullable(false);
    LogicalType original = new LogicalType("com.example", rootSchema, Map.of(),
        java.util.List.of(), Map.of());
    LogicalType roundTripped = JsonToLogicalTypeConverter.toLogicalType(
        LogicalTypeToJsonConverter.fromLogicalType(original, "Holder"));
    assertEquals("com.example", roundTripped.getNamespace());
  }

  @Test
  void testRoundTripNoNamespaceIsNull() {
    Schema rootSchema = Schema.createStruct(Arrays.asList(
        new Field("x", Schema.create(Schema.Type.INT).setNullable(false), 0)))
        .setNullable(false);
    LogicalType original = new LogicalType(rootSchema);
    LogicalType roundTripped = JsonToLogicalTypeConverter.toLogicalType(
        LogicalTypeToJsonConverter.fromLogicalType(original, "Holder"));
    assertNull(roundTripped.getNamespace());
  }

  // =========================================================================
  // Reserved-keyword field names
  // =========================================================================

  @Test
  void testRoundTripReservedKeywordFieldNames() {
    Schema rootSchema = Schema.createStruct(Arrays.asList(
        new Field("class", Schema.create(Schema.Type.INT).setNullable(false), 0),
        new Field("enum", Schema.createString().setNullable(false), 1),
        new Field("default", Schema.create(Schema.Type.BOOLEAN).setNullable(false), 2),
        new Field("package", Schema.createString().setNullable(false), 3),
        new Field("type", Schema.create(Schema.Type.BIGINT).setNullable(false), 4)))
        .setNullable(false);
    LogicalType roundTripped = JsonToLogicalTypeConverter.toLogicalType(
        convertSchema(rootSchema));
    Schema rt = roundTripped.getRootSchema();
    assertEquals(5, rt.getFields().size());
    assertEquals(Schema.Type.INT, rt.getField("class").getSchema().getType());
    assertEquals(Schema.Type.VARCHAR, rt.getField("enum").getSchema().getType());
    assertEquals(Schema.Type.BOOLEAN, rt.getField("default").getSchema().getType());
    assertEquals(Schema.Type.VARCHAR, rt.getField("package").getSchema().getType());
    assertEquals(Schema.Type.BIGINT, rt.getField("type").getSchema().getType());
  }

  // =========================================================================
  // Field.position round-trip
  // =========================================================================

  @Test
  void testRoundTripFieldPosition() {
    Schema rootSchema = Schema.createStruct(Arrays.asList(
        new Field("a", Schema.create(Schema.Type.INT).setNullable(false), 0),
        new Field("b", Schema.createString().setNullable(false), 1),
        new Field("c", Schema.create(Schema.Type.BOOLEAN).setNullable(false), 2)))
        .setNullable(false);
    LogicalType roundTripped = JsonToLogicalTypeConverter.toLogicalType(
        convertSchema(rootSchema));
    Schema rt = roundTripped.getRootSchema();
    assertEquals(0, rt.getField("a").getPosition());
    assertEquals(1, rt.getField("b").getPosition());
    assertEquals(2, rt.getField("c").getPosition());
  }

  // =========================================================================
  // Mutually recursive named types (A -> B -> A)
  // =========================================================================

  @Test
  void testRoundTripMutuallyRecursiveNamedTypes() {
    Schema typeA = Schema.createStruct(Arrays.asList(
        new Field("aVal", Schema.create(Schema.Type.INT).setNullable(false), 0),
        new Field("toB",
            Schema.createNamedTypeRef("B").setNullable(true), 1)))
        .setNullable(false);
    Schema typeB = Schema.createStruct(Arrays.asList(
        new Field("bVal", Schema.createString().setNullable(false), 0),
        new Field("toA",
            Schema.createNamedTypeRef("A").setNullable(true), 1)))
        .setNullable(false);
    Map<String, Schema> namedTypes = new LinkedHashMap<>();
    namedTypes.put("A", typeA);
    namedTypes.put("B", typeB);
    Schema rootSchema = Schema.createStruct(Arrays.asList(
        new Field("root",
            Schema.createNamedTypeRef("A").setNullable(false), 0)))
        .setNullable(false);
    LogicalType roundTripped = JsonToLogicalTypeConverter.toLogicalType(
        LogicalTypeToJsonConverter.fromLogicalType(
            new LogicalType(rootSchema, namedTypes), "Holder"));

    Schema rootRef = roundTripped.getRootSchema().getField("root").getSchema();
    assertEquals(Schema.Type.NAMED_TYPE_REF, rootRef.getType());
    assertEquals("A", rootRef.getQualifiedName());

    Schema rtA = roundTripped.getNamedTypes().get("A");
    assertNotNull(rtA);
    Schema toBRef = rtA.getField("toB").getSchema();
    assertEquals(Schema.Type.NAMED_TYPE_REF, toBRef.getType());
    assertEquals("B", toBRef.getQualifiedName());

    Schema rtB = roundTripped.getNamedTypes().get("B");
    assertNotNull(rtB);
    Schema toARef = rtB.getField("toA").getSchema();
    assertEquals(Schema.Type.NAMED_TYPE_REF, toARef.getType());
    assertEquals("A", toARef.getQualifiedName());
  }

  // =========================================================================
  // Standalone TIME precision (Avro has its own; JSON needed parity)
  // =========================================================================

  @Test
  void testRoundTripTimePrecision() {
    for (int p = 0; p <= 9; p++) {
      Schema rootSchema = Schema.createStruct(Arrays.asList(
          new Field("t", Schema.createTime(p).setNullable(false), 0)))
          .setNullable(false);
      LogicalType roundTripped = JsonToLogicalTypeConverter.toLogicalType(
          convertSchema(rootSchema));
      Schema rt = roundTripped.getRootSchema().getField("t").getSchema();
      assertEquals(Schema.Type.TIME, rt.getType(), "TIME(" + p + ")");
      assertEquals(p, rt.getPrecision(), "TIME precision " + p);
    }
  }

  // =========================================================================
  // All-precision timestamp round-trip (0..9, including sub-millisecond)
  // =========================================================================

  @Test
  void testRoundTripTimestampPrecisionAllValues() {
    for (int p = 0; p <= 9; p++) {
      Schema rootSchema = Schema.createStruct(Arrays.asList(
          new Field("t", Schema.createTime(p).setNullable(false), 0),
          new Field("ts", Schema.createTimestamp(p).setNullable(false), 1),
          new Field("tsltz", Schema.createTimestampLtz(p).setNullable(false), 2)))
          .setNullable(false);
      LogicalType roundTripped = JsonToLogicalTypeConverter.toLogicalType(
          convertSchema(rootSchema));
      Schema rt = roundTripped.getRootSchema();
      assertEquals(p, rt.getField("t").getSchema().getPrecision(), "TIME precision " + p);
      assertEquals(p, rt.getField("ts").getSchema().getPrecision(), "TIMESTAMP precision " + p);
      assertEquals(p, rt.getField("tsltz").getSchema().getPrecision(), "TIMESTAMP_LTZ precision " + p);
    }
  }

  // =========================================================================
  // Default-value handling on nullable fields (null and non-null)
  // =========================================================================

  @Test
  void testRoundTripDefaultNullOnNullablePrimitive() {
    // Everit treats default(null) as "no default set" (hasDefaultValue() is
    // value != null). The writer therefore can't distinguish "default = null"
    // from "no default" for nullable fields. Silent loss is the documented
    // behavior for this format.
    Schema rootSchema = Schema.createStruct(Arrays.asList(
        new Field("f", Schema.create(Schema.Type.INT).setNullable(true), 0,
            null, true, null, null, null)))
        .setNullable(false);
    LogicalType roundTripped = JsonToLogicalTypeConverter.toLogicalType(
        convertSchema(rootSchema));
    Field f = roundTripped.getRootSchema().getField("f");
    assertTrue(f.getSchema().isNullable());
    assertFalse(f.hasDefaultValue());
    assertNull(f.getDefaultValue());
  }

  @Test
  void testRoundTripNullableFieldWithNonNullDefault() {
    Schema rootSchema = Schema.createStruct(Arrays.asList(
        new Field("f", Schema.create(Schema.Type.INT).setNullable(true), 0,
            42, true, null, null, null)))
        .setNullable(false);
    LogicalType roundTripped = JsonToLogicalTypeConverter.toLogicalType(
        convertSchema(rootSchema));
    Field f = roundTripped.getRootSchema().getField("f");
    assertTrue(f.getSchema().isNullable());
    assertTrue(f.hasDefaultValue());
    assertEquals(42, f.getDefaultValue());
  }

  @Test
  void testNullableUnionEmittedAsFlatOneOf() {
    // A nullable proper UNION<INT, STRING> should emit a flat
    // oneOf:[null, int, string] — not nested oneOf:[null, oneOf:[int, string]].
    // Mirrors the Avro converter's union-collapse behavior.
    Schema union = Schema.createUnion(Arrays.asList(
        new Schema.UnionBranch("i", Schema.create(Schema.Type.INT).setNullable(false)),
        new Schema.UnionBranch("s", Schema.createString().setNullable(false))))
        .setNullable(true);
    Schema rootSchema = Schema.createStruct(Arrays.asList(
        new Field("u", union, 0)))
        .setNullable(false);
    JsonSchema jsonSchema = LogicalTypeToJsonConverter.fromLogicalType(
        new LogicalType(rootSchema), "Holder");
    org.everit.json.schema.ObjectSchema obj =
        (org.everit.json.schema.ObjectSchema) jsonSchema.rawSchema();
    org.everit.json.schema.Schema fieldSchema = obj.getPropertySchemas().get("u");
    assertTrue(fieldSchema instanceof org.everit.json.schema.CombinedSchema);
    org.everit.json.schema.CombinedSchema combined =
        (org.everit.json.schema.CombinedSchema) fieldSchema;
    assertEquals(3, combined.getSubschemas().size());
    long nullCount = combined.getSubschemas().stream()
        .filter(s -> s instanceof org.everit.json.schema.NullSchema)
        .count();
    assertEquals(1, nullCount, "exactly one NullSchema member");
    long unionCount = combined.getSubschemas().stream()
        .filter(s -> s instanceof org.everit.json.schema.CombinedSchema)
        .count();
    assertEquals(0, unionCount, "no nested oneOf");

    // Round-trip preserves branch identity (confluent:union metadata migrated).
    LogicalType rt = JsonToLogicalTypeConverter.toLogicalType(jsonSchema);
    Schema rtUnion = rt.getRootSchema().getField("u").getSchema();
    assertEquals(Schema.Type.UNION, rtUnion.getType());
    assertTrue(rtUnion.isNullable());
    assertEquals(2, rtUnion.getBranches().size());
    assertEquals("i", rtUnion.getBranches().get(0).getName());
    assertEquals("s", rtUnion.getBranches().get(1).getName());
  }

  // =========================================================================
  // Field-level doc round-trip
  // =========================================================================

  @Test
  void testRoundTripFieldDoc() {
    Schema rootSchema = Schema.createStruct(Arrays.asList(
        new Field("a", Schema.createString().setNullable(false), 0,
            null, false, "name of the user", null, null),
        new Field("b", Schema.create(Schema.Type.INT).setNullable(false), 1,
            null, false, "age in years", null, null)))
        .setNullable(false);
    LogicalType roundTripped = JsonToLogicalTypeConverter.toLogicalType(
        convertSchema(rootSchema));
    Schema rt = roundTripped.getRootSchema();
    assertEquals("name of the user", rt.getField("a").getDoc());
    assertEquals("age in years", rt.getField("b").getDoc());
  }

  // =========================================================================
  // Empty + single-field struct
  // =========================================================================

  @Test
  void testEmptyStructRoundTrip() {
    Schema empty = Schema.createStruct(java.util.Collections.emptyList())
        .setNullable(false);
    Schema rt = JsonToLogicalTypeConverter.toRootSchema(convertSchema(empty));
    assertEquals(Schema.Type.STRUCT, rt.getType());
    assertTrue(rt.getFields().isEmpty());
  }

  @Test
  void testRoundTripSingleFieldStruct() {
    Schema single = Schema.createStruct(Arrays.asList(
        new Field("only", Schema.createString().setNullable(false), 0)))
        .setNullable(false);
    Schema rt = JsonToLogicalTypeConverter.toRootSchema(convertSchema(single));
    assertEquals(1, rt.getFields().size());
    assertEquals("only", rt.getFields().get(0).getName());
    assertEquals(Schema.Type.VARCHAR, rt.getFields().get(0).getSchema().getType());
  }

  // =========================================================================
  // Multi-branch unions (>2 branches)
  // =========================================================================

  @Test
  void testRoundTripUnionWithManyBranches() {
    Schema union = Schema.createUnion(Arrays.asList(
        new Schema.UnionBranch("i", Schema.create(Schema.Type.INT).setNullable(false)),
        new Schema.UnionBranch("s", Schema.createString().setNullable(false)),
        new Schema.UnionBranch("b", Schema.create(Schema.Type.BOOLEAN).setNullable(false)),
        new Schema.UnionBranch("d", Schema.create(Schema.Type.DOUBLE).setNullable(false)),
        new Schema.UnionBranch("by", Schema.createBytes().setNullable(false))))
        .setNullable(false);
    Schema rootSchema = Schema.createStruct(Arrays.asList(
        new Field("u", union, 0)))
        .setNullable(false);
    LogicalType roundTripped = JsonToLogicalTypeConverter.toLogicalType(
        convertSchema(rootSchema));
    Schema rt = roundTripped.getRootSchema().getField("u").getSchema();
    assertEquals(Schema.Type.UNION, rt.getType());
    assertEquals(5, rt.getBranches().size());
    assertEquals("i", rt.getBranches().get(0).getName());
    assertEquals("s", rt.getBranches().get(1).getName());
    assertEquals("b", rt.getBranches().get(2).getName());
    assertEquals("d", rt.getBranches().get(3).getName());
    assertEquals("by", rt.getBranches().get(4).getName());
  }

  // =========================================================================
  // Deeply nested type combinations (3 levels)
  // =========================================================================

  @Test
  void testRoundTripDeeplyNestedArrayMapStruct() {
    Schema inner = Schema.createStruct(Arrays.asList(
        new Field("x", Schema.create(Schema.Type.INT).setNullable(false), 0)))
        .setNullable(false);
    Schema mapType = Schema.createMap(
        Schema.createString().setNullable(false), inner).setNullable(false);
    Schema arrayOfMap = Schema.createArray(mapType).setNullable(false);
    Schema rootSchema = Schema.createStruct(Arrays.asList(
        new Field("nested", arrayOfMap, 0)))
        .setNullable(false);
    LogicalType roundTripped = JsonToLogicalTypeConverter.toLogicalType(
        convertSchema(rootSchema));
    Schema rt = roundTripped.getRootSchema().getField("nested").getSchema();
    assertEquals(Schema.Type.ARRAY, rt.getType());
    assertEquals(Schema.Type.MAP, rt.getElementType().getType());
    assertEquals(Schema.Type.STRUCT, rt.getElementType().getValueType().getType());
    assertEquals(Schema.Type.INT,
        rt.getElementType().getValueType().getField("x").getSchema().getType());
  }

  // =========================================================================
  // Nested-struct doc/tags/params round-trip
  // =========================================================================

  @Test
  void testRoundTripNestedStructDocTagsParams() {
    Map<String, Object> innerParams = new LinkedHashMap<>();
    innerParams.put("inner-key", "inner-val");
    Schema inner = Schema.createStruct(Arrays.asList(
        new Field("v", Schema.create(Schema.Type.INT).setNullable(false), 0)))
        .setNullable(false)
        .setDoc("inner doc")
        .setTags(Arrays.asList("inner-tag"))
        .setParams(innerParams);
    Schema rootSchema = Schema.createStruct(Arrays.asList(
        new Field("inner", inner, 0)))
        .setNullable(false);
    LogicalType roundTripped = JsonToLogicalTypeConverter.toLogicalType(
        convertSchema(rootSchema));
    Schema rt = roundTripped.getRootSchema().getField("inner").getSchema();
    assertEquals("inner doc", rt.getDoc());
    assertEquals(Arrays.asList("inner-tag"), rt.getTags());
    assertEquals("inner-val", rt.getParams().get("inner-key"));
  }

  // =========================================================================
  // V1 emission mode (Flink-compatible)
  // =========================================================================

  @Test
  void testV1ThrowsOnUnion() {
    Schema union = Schema.createUnion(Arrays.asList(
        new UnionBranch("a", Schema.create(Schema.Type.INT).setNullable(false)),
        new UnionBranch("b", Schema.createString().setNullable(false))))
        .setNullable(false);
    Schema rootSchema = Schema.createStruct(Arrays.asList(
        new Field("u", union, 0))).setNullable(false);
    ValidationException ex = assertThrows(ValidationException.class, () ->
        LogicalTypeToJsonConverter.fromLogicalType(
            new LogicalType(rootSchema), "Holder", LogicalTypeVersion.V1));
    assertTrue(ex.getMessage().contains("UNION"));
  }

  @Test
  void testV1ThrowsOnVariant() {
    Schema rootSchema = Schema.createStruct(Arrays.asList(
        new Field("v", Schema.create(Schema.Type.VARIANT).setNullable(false), 0)))
        .setNullable(false);
    ValidationException ex = assertThrows(ValidationException.class, () ->
        LogicalTypeToJsonConverter.fromLogicalType(
            new LogicalType(rootSchema), "Holder", LogicalTypeVersion.V1));
    assertTrue(ex.getMessage().contains("VARIANT"));
  }

  @Test
  void testV1ThrowsOnEnum() {
    Schema enumSchema = Schema.createEnum(Arrays.asList(
        new EnumValue("RED"), new EnumValue("GREEN"), new EnumValue("BLUE")))
        .setNullable(false);
    Schema rootSchema = Schema.createStruct(Arrays.asList(
        new Field("color", enumSchema, 0))).setNullable(false);
    ValidationException ex = assertThrows(ValidationException.class, () ->
        LogicalTypeToJsonConverter.fromLogicalType(
            new LogicalType(rootSchema), "Holder", LogicalTypeVersion.V1));
    assertTrue(ex.getMessage().contains("ENUM"));
  }

  @Test
  void testV1EditionMetadata() {
    Schema rootSchema = Schema.createStruct(Arrays.asList(
        new Field("x", Schema.create(Schema.Type.INT).setNullable(false), 0)))
        .setNullable(false);
    JsonSchema v1 = LogicalTypeToJsonConverter.fromLogicalType(
        new LogicalType(rootSchema), "Holder", LogicalTypeVersion.V1);
    JsonSchema v2 = LogicalTypeToJsonConverter.fromLogicalType(
        new LogicalType(rootSchema), "Holder", LogicalTypeVersion.V2);
    assertEquals("1", v1.metadata().getProperties().get("confluent:edition"));
    assertEquals("2", v2.metadata().getProperties().get("confluent:edition"));
  }

  /**
   * {@code logical.key.length} / {@code logical.key.type} for a CHAR-keyed MAP are
   * emitted as top-level schema properties (in {@code unprocessedProperties}), not
   * nested inside {@code confluent:params}.
   */
  @Test
  void testLogicalMapKeyMetadataAtTopLevel() {
    Schema mapSchema = Schema.createMap(
        Schema.createChar(8).setNullable(false),
        Schema.create(Schema.Type.INT).setNullable(true)).setNullable(false);
    Schema rootSchema = Schema.createStruct(Arrays.asList(
        new Field("m", mapSchema, 0)))
        .setNullable(false);
    org.everit.json.schema.Schema schema = LogicalTypeToJsonConverter.fromLogicalType(
        new LogicalType(rootSchema), "Holder", LogicalTypeVersion.V2).rawSchema();

    org.everit.json.schema.Schema mapField =
        ((ObjectSchema) schema).getPropertySchemas().get("m");
    java.util.Map<String, Object> props = mapField.getUnprocessedProperties();
    assertEquals(8, props.get(CommonConstants.LOGICAL_KEY_LENGTH_PROP),
        "logical.key.length must be a top-level prop on the map schema");
    assertEquals("CHAR", props.get(CommonConstants.LOGICAL_KEY_TYPE_PROP),
        "logical.key.type must be a top-level prop on the map schema");
    assertNull(props.get("confluent:params"),
        "confluent:params must not be emitted when only logical.* metadata exists");
  }

  /**
   * The dotted-nesting convention is a proto-only structural concern. For JSON
   * Schema, a localNamedType keyed {@code Outer.Inner} just becomes a
   * top-level entry under {@code $defs} keyed by the dotted name. No nesting
   * structure is invented in the JSON output.
   */
  @Test
  void testNestedDottedNameEmittedAsFlatDef() {
    Schema innerType = Schema.createStruct(Arrays.asList(
        new Field("x", Schema.create(Schema.Type.INT).setNullable(false), 0)))
        .setNullable(false);
    Schema outerType = Schema.createStruct(Arrays.asList(
        new Field("name", Schema.createString().setNullable(false), 0),
        new Field("inner",
            Schema.createNamedTypeRef("Outer.Inner").setNullable(false), 1)))
        .setNullable(false);
    Map<String, Schema> namedTypes = new LinkedHashMap<>();
    namedTypes.put("Outer", outerType);
    namedTypes.put("Outer.Inner", innerType);
    Schema root = Schema.createNamedTypeRef("Outer").setNullable(false);

    String jsonStr = LogicalTypeToJsonConverter.fromLogicalType(
        new LogicalType(root, namedTypes), "ignored",
        LogicalTypeVersion.V2).canonicalString();

    // Dotted name lives flat under $defs (or `definitions` for older drafts);
    // no structural nesting is invented in the JSON Schema output.
    org.json.JSONObject root2020 = new org.json.JSONObject(jsonStr);
    org.json.JSONObject defs = root2020.optJSONObject("$defs");
    if (defs == null) {
      defs = root2020.optJSONObject("definitions");
    }
    assertNotNull(defs, "expected $defs or definitions in the JSON output");
    assertTrue(defs.has("Outer.Inner"),
        "expected `Outer.Inner` to appear as a flat $defs key, got keys: "
            + defs.keySet());
  }
}
