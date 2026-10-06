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

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.confluent.kafka.schemaregistry.json.JsonSchema;
import io.confluent.kafka.schemaregistry.type.logical.LogicalType;
import io.confluent.kafka.schemaregistry.type.logical.LogicalTypeToDdlConverter;
import io.confluent.kafka.schemaregistry.type.logical.Schema;
import io.confluent.kafka.schemaregistry.type.logical.ValidationException;
import io.confluent.kafka.schemaregistry.type.logical.common.LogicalTypeVersion;
import org.everit.json.schema.BooleanSchema;
import org.everit.json.schema.CombinedSchema;
import org.everit.json.schema.EmptySchema;
import org.everit.json.schema.EnumSchema;
import org.everit.json.schema.NullSchema;
import org.everit.json.schema.NumberSchema;
import org.everit.json.schema.ObjectSchema;
import org.everit.json.schema.StringSchema;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTimeoutPreemptively;
import static org.junit.jupiter.api.Assertions.assertTrue;

class JsonToLogicalTypeConverterTest {

  @Test
  void namedLeafRootTitleRoundTripsThroughJson() {
    // JSON -> LT -> JSON: a root object with a title unwraps to a bare STRUCT carrying its name on
    // read, and the writer re-emits that name as the root object's title.
    String jsonSchema = "{\"type\":\"object\",\"title\":\"Order\","
        + "\"properties\":{\"id\":{\"type\":\"integer\",\"connect.type\":\"int32\"}}}";
    LogicalType lt = JsonToLogicalTypeConverter.toLogicalType(new JsonSchema(jsonSchema));
    assertEquals(Schema.Type.STRUCT, lt.getRootSchema().getType());
    assertEquals("Order", lt.getName());
    JsonSchema out = LogicalTypeToJsonConverter.fromLogicalType(lt, "IGNORED");
    assertEquals("Order", out.rawSchema().getTitle());

    // The DDL projection declares the root as a named STRUCT.
    String ddl = LogicalTypeToDdlConverter.toDdl(lt);
    assertThat(ddl).contains("STRUCT Order (");
  }

  @Test
  void testBooleanType() {
    org.everit.json.schema.Schema jsonSchema = BooleanSchema.builder().build();
    Schema result = JsonToLogicalTypeConverter.toRootSchema(new JsonSchema(jsonSchema));
    assertEquals(Schema.Type.BOOLEAN, result.getType());
    assertFalse(result.isNullable());
  }

  @Test
  void testIntegerType() {
    org.everit.json.schema.Schema jsonSchema = NumberSchema.builder()
        .unprocessedProperties(Collections.singletonMap("connect.type", "int32"))
        .build();
    Schema result = JsonToLogicalTypeConverter.toRootSchema(new JsonSchema(jsonSchema));
    assertEquals(Schema.Type.INT, result.getType());
  }

  @Test
  void testStringType() {
    org.everit.json.schema.Schema jsonSchema = StringSchema.builder().build();
    Schema result = JsonToLogicalTypeConverter.toRootSchema(new JsonSchema(jsonSchema));
    assertEquals(Schema.Type.VARCHAR, result.getType());
  }

  @Test
  void testNullableType() {
    org.everit.json.schema.Schema jsonSchema = CombinedSchema.builder()
        .criterion(CombinedSchema.ONE_CRITERION)
        .subschema(NullSchema.INSTANCE)
        .subschema(BooleanSchema.builder().build())
        .build();
    Schema result = JsonToLogicalTypeConverter.toRootSchema(new JsonSchema(jsonSchema));
    assertEquals(Schema.Type.BOOLEAN, result.getType());
    assertTrue(result.isNullable());
  }

  @Test
  void testEnumType() {
    org.everit.json.schema.Schema jsonSchema = EnumSchema.builder()
        .possibleValues(Arrays.asList("RED", "GREEN", "BLUE"))
        .build();
    Schema result = JsonToLogicalTypeConverter.toRootSchema(new JsonSchema(jsonSchema));
    assertEquals(Schema.Type.ENUM, result.getType());
    assertEquals(3, result.getEnumValues().size());
    assertEquals("RED", result.getEnumValues().get(0).getSymbol());
  }

  private static Schema rootOf(String json) {
    return JsonToLogicalTypeConverter.toRootSchema(new JsonSchema(json));
  }

  private static Schema v1RootOf(String json) {
    return JsonToLogicalTypeConverter
        .toLogicalType(new JsonSchema(json), LogicalTypeVersion.V1).getRootSchema();
  }

  @Test
  void constConvertsExactlyLikeASingleValueEnumInBothEditions() {
    String meta = "\"description\":\"d\",\"confluent:tags\":[\"t\"],"
        + "\"confluent:enum\":[{\"doc\":\"only\"}]";
    Schema fromConst = rootOf("{\"const\":\"x\"," + meta + "}");

    assertEquals(Schema.Type.ENUM, fromConst.getType());
    assertEquals("x", fromConst.getEnumValues().get(0).getSymbol());
    assertEquals("only", fromConst.getEnumValues().get(0).getDoc());
    assertEquals("d", fromConst.getDoc());
    assertEquals(rootOf("{\"enum\":[\"x\"]," + meta + "}"), fromConst);
    assertEquals(v1RootOf("{\"enum\":[\"x\"]," + meta + "}"),
        v1RootOf("{\"const\":\"x\"," + meta + "}"));
  }

  @Test
  void everyPermittedValueBecomesAStringSymbolInBothEditions() {
    // Old Flink read every JSON enum as a string column; V2 keeps that so the editions agree.
    String[][] cases = {
        {"{\"enum\":[1,2]}", "1,2"},
        {"{\"const\":42}", "42"},
        {"{\"const\":true}", "true"},
    };
    for (String[] c : cases) {
      Schema v2 = rootOf(c[0]);
      assertEquals(Schema.Type.ENUM, v2.getType(), c[0]);
      assertEquals(Arrays.asList(c[1].split(",")), symbolsOf(v2), c[0]);
      assertEquals(v2, v1RootOf(c[0]), c[0]);
    }
  }

  private static List<String> symbolsOf(Schema enumSchema) {
    return enumSchema.getEnumValues().stream()
        .map(Schema.EnumValue::getSymbol)
        .collect(Collectors.toList());
  }

  @Test
  void aNullValueOfABareEnumMakesItNullableInsteadOfBecomingASymbol() {
    Schema result = rootOf("{\"enum\":[\"a\",null,\"b\"],"
        + "\"confluent:enum\":[{\"doc\":\"A\"},{},{\"doc\":\"B\"}]}");
    assertEquals(Schema.Type.ENUM, result.getType());
    assertEquals(Arrays.asList("a", "b"), symbolsOf(result));
    assertEquals("B", result.getEnumValues().get(1).getDoc());
    assertTrue(result.isNullable());
    assertEquals(result, v1RootOf("{\"enum\":[\"a\",null,\"b\"],"
        + "\"confluent:enum\":[{\"doc\":\"A\"},{},{\"doc\":\"B\"}]}"));
  }

  @Test
  void aNullOnlyEnumOrConstIsRejectedInBothEditions() {
    String[] schemas = {
        "{\"const\":null}",
        "{\"enum\":[null]}",
        "{\"type\":\"string\",\"const\":null}",
        "{\"type\":[\"string\",\"null\"],\"enum\":[null]}",
    };
    for (String schema : schemas) {
      assertThatThrownBy(() -> rootOf(schema)).as(schema)
          .isInstanceOf(ValidationException.class).hasMessageContaining("non-null value");
      assertThatThrownBy(() -> v1RootOf(schema)).as(schema)
          .isInstanceOf(ValidationException.class).hasMessageContaining("non-null value");
    }
  }

  @Test
  void aStringTypedConstOrEnumReadsAsTheBareOneInBothEditions() {
    // everit wraps `type` + `const`/`enum` in a synthetic allOf and hangs the description off it.
    String[] drafts = {"", "\"$schema\":\"https://json-schema.org/draft/2020-12/schema\","};
    for (String draft : drafts) {
      String typedConst = "{" + draft + "\"type\":\"string\",\"const\":\"x\",\"description\":\"d\"}";
      assertEquals(rootOf("{\"enum\":[\"x\"],\"description\":\"d\"}"), rootOf(typedConst), draft);
      assertEquals(rootOf(typedConst), v1RootOf(typedConst), draft);

      String typedEnum = "{" + draft + "\"type\":\"string\",\"enum\":[\"a\",\"b\"]}";
      assertEquals(rootOf("{\"enum\":[\"a\",\"b\"]}"), rootOf(typedEnum), draft);
      assertEquals(rootOf(typedEnum), v1RootOf(typedEnum), draft);
    }
  }

  @Test
  void onlyTheTypeDecidesTheNullabilityOfATypedConstOrEnum() {
    // A value must match both `type` and `enum`, so a null value is dead unless the type is null.
    String[] drafts = {"", "\"$schema\":\"https://json-schema.org/draft/2020-12/schema\","};
    for (String draft : drafts) {
      Schema nullableType = rootOf(
          "{" + draft + "\"type\":[\"string\",\"null\"],\"enum\":[\"a\",\"b\"],"
              + "\"description\":\"d\"}");
      assertEquals(Schema.Type.ENUM, nullableType.getType(), draft);
      assertEquals(Arrays.asList("a", "b"), symbolsOf(nullableType), draft);
      assertEquals("d", nullableType.getDoc(), draft);
      assertTrue(nullableType.isNullable(), draft);
      assertEquals(nullableType, rootOf("{" + draft
          + "\"type\":[\"string\",\"null\"],\"enum\":[\"a\",\"b\",null],\"description\":\"d\"}"),
          draft);

      Schema stringType = rootOf("{" + draft + "\"type\":\"string\",\"enum\":[\"a\",null]}");
      assertEquals(Arrays.asList("a"), symbolsOf(stringType), draft);
      assertFalse(stringType.isNullable(), draft);
    }
  }

  @Test
  void aTypedEnumKeepsEachValuesDocAlignedPastADroppedNull() {
    String schema = "{\"type\":[\"string\",\"null\"],\"enum\":[\"a\",null,\"b\",\"c\"],"
        + "\"confluent:enum\":[{\"doc\":\"A\"},{},{\"doc\":\"B\"},{\"doc\":\"C\"}]}";
    for (Schema result : new Schema[] {rootOf(schema), v1RootOf(schema)}) {
      assertEquals(Arrays.asList("a", "b", "c"), symbolsOf(result));
      assertEquals(Arrays.asList("A", "B", "C"), result.getEnumValues().stream()
          .map(Schema.EnumValue::getDoc).collect(Collectors.toList()));
    }
  }

  @Test
  void aNullMemberBehindARefConvertsAsAnInlineOneInBothEditions() {
    String body = "{\"type\":\"object\",\"properties\":{"
        + "\"o\":{\"oneOf\":[%1$s,"
        + "{\"type\":\"object\",\"properties\":{\"a\":{\"type\":\"string\"}}}]},"
        + "\"u\":{\"oneOf\":[%1$s,{\"type\":\"string\"},{\"type\":\"integer\"}]},"
        + "\"k\":{\"oneOf\":[%1$s,{\"const\":\"x\"}]}}%2$s}";
    String inline = String.format(body, "{\"type\":\"null\"}", "");
    String ref = String.format(body, "{\"$ref\":\"#/$defs/N\"}",
        ",\"$defs\":{\"N\":{\"type\":\"null\"}}");
    assertEquals(rootOf(inline).toDdl(), rootOf(ref).toDdl());
    assertEquals(v1RootOf(inline).toDdl(), v1RootOf(ref).toDdl());
  }

  @Test
  void aNullDefinitionConvertsUnderAModernDraft() {
    // 2020-12 converts every $defs entry up front: a null one, used or not, has no type to give.
    String body = "{\"$schema\":\"https://json-schema.org/draft/2020-12/schema\","
        + "\"type\":\"object\",\"properties\":{\"o\":{\"oneOf\":[%s,{\"type\":\"string\"}]}}%s}";
    String inline = String.format(body, "{\"type\":\"null\"}", "");
    String ref = String.format(body, "{\"$ref\":\"#/$defs/N\"}",
        ",\"$defs\":{\"N\":{\"type\":\"null\"}}");
    assertEquals(rootOf(inline).toDdl(), rootOf(ref).toDdl());
  }

  @Test
  void anUnusedUnconvertibleDefinitionDoesNotFailAModernDraft() {
    // 2020-12 converts every $defs entry up front: one the logical type cannot express fails only
    // where it is used, as draft-07's on-demand conversion does.
    String body = "{\"$schema\":\"https://json-schema.org/draft/2020-12/schema\","
        + "\"type\":\"object\",\"properties\":{\"a\":{\"type\":\"string\"}%s}"
        + ",\"$defs\":{\"U\":%s}}";
    String plain = rootOf(String.format(body, "", "{\"type\":\"string\"}")).toDdl();
    String[] unconvertible = {
        "{\"not\":{\"type\":\"string\"}}",
        "{\"if\":{\"type\":\"string\"},\"then\":{\"minLength\":1}}",
        "{\"const\":null}",
        "{\"allOf\":[{\"type\":\"null\"}]}",
        "{\"type\":\"array\",\"prefixItems\":[{\"type\":\"string\"}]}"};
    for (String u : unconvertible) {
      assertEquals(plain, rootOf(String.format(body, "", u)).toDdl(), u);
      assertThatThrownBy(() -> rootOf(String.format(body, ",\"b\":{\"$ref\":\"#/$defs/U\"}", u)))
          .as(u).isInstanceOf(ValidationException.class);
    }
  }

  @Test
  void aDefinitionThatFailedLeavesNoPlaceholder() {
    // U reaches V first, and V fails: what that attempt left, V's empty placeholder included, is
    // undone, so b's use of V fails rather than reading an empty struct.
    String schema = "{\"$schema\":\"https://json-schema.org/draft/2020-12/schema\","
        + "\"type\":\"object\",\"properties\":{\"b\":{\"$ref\":\"#/$defs/V\"}},"
        + "\"$defs\":{\"U\":{\"$ref\":\"#/$defs/V\"},\"V\":{\"not\":{\"type\":\"string\"}}}}";
    assertThatThrownBy(() -> rootOf(schema)).isInstanceOf(ValidationException.class);
  }

  @Test
  void aLengthLimitedStringTypedConstOrEnumKeepsItsLengthInBothEditions() {
    String[][] cases = {
        {"{\"type\":\"string\",\"maxLength\":5,\"enum\":[\"a\",\"b\"]}", "VARCHAR", "false"},
        {"{\"type\":\"string\",\"minLength\":1,\"maxLength\":1,\"const\":\"x\"}", "CHAR", "false"},
        {"{\"type\":[\"string\",\"null\"],\"maxLength\":5,\"enum\":[\"a\"]}", "VARCHAR", "true"},
    };
    for (String[] c : cases) {
      Schema result = rootOf(c[0]);
      assertEquals(Schema.Type.valueOf(c[1]), result.getType(), c[0]);
      assertEquals(Boolean.parseBoolean(c[2]), result.isNullable(), c[0]);
      assertEquals(result, v1RootOf(c[0]), c[0]);
    }
    assertEquals(Schema.createVarchar(5).setNullable(false),
        rootOf("{\"type\":\"string\",\"maxLength\":5,\"enum\":[\"a\",\"b\"]}"));

    // minLength alone bounds nothing (it reads as VARCHAR(MAX)), so the enum is kept.
    String minOnly = "{\"type\":\"string\",\"minLength\":0,\"const\":\"x\"}";
    assertEquals(rootOf("{\"enum\":[\"x\"]}"), rootOf(minOnly));
    assertEquals(rootOf(minOnly), v1RootOf(minOnly));
  }

  @Test
  void aNonStringTypedConstOrEnumIsItsDeclaredTypeInBothEditions() {
    String[][] cases = {
        {"{\"type\":\"boolean\",\"const\":true}", "BOOLEAN"},
        {"{\"type\":\"boolean\",\"enum\":[true]}", "BOOLEAN"},
        {"{\"type\":\"integer\",\"const\":1}", "BIGINT"},
        {"{\"type\":\"integer\",\"enum\":[1,2]}", "BIGINT"},
        {"{\"type\":[\"integer\",\"null\"],\"enum\":[1,null]}", "BIGINT"},
    };
    for (String[] c : cases) {
      assertEquals(Schema.Type.valueOf(c[1]), rootOf(c[0]).getType(), c[0]);
      assertEquals(rootOf(c[0]), v1RootOf(c[0]), c[0]);
    }
  }

  @Test
  void testUnionType() {
    org.everit.json.schema.Schema jsonSchema = CombinedSchema.builder()
        .criterion(CombinedSchema.ONE_CRITERION)
        .subschema(NumberSchema.builder()
            .unprocessedProperties(Collections.singletonMap("connect.type", "int32"))
            .build())
        .subschema(StringSchema.builder().build())
        .build();
    Schema result = JsonToLogicalTypeConverter.toRootSchema(new JsonSchema(jsonSchema));
    assertEquals(Schema.Type.UNION, result.getType());
    assertEquals(2, result.getBranches().size());
  }

  @Test
  void testObjectType() {
    org.everit.json.schema.Schema jsonSchema = ObjectSchema.builder()
        .addPropertySchema("name", StringSchema.builder().build())
        .addPropertySchema("age", NumberSchema.builder()
            .unprocessedProperties(Collections.singletonMap("connect.type", "int32"))
            .build())
        .addRequiredProperty("name")
        .additionalProperties(false)
        .build();
    Schema result = JsonToLogicalTypeConverter.toRootSchema(new JsonSchema(jsonSchema));
    assertEquals(Schema.Type.STRUCT, result.getType());
    assertEquals(2, result.getFields().size());
  }

  @Test
  void testVariantType() {
    org.everit.json.schema.Schema jsonSchema = EmptySchema.builder().build();
    Schema result = JsonToLogicalTypeConverter.toRootSchema(new JsonSchema(jsonSchema));
    assertEquals(Schema.Type.VARIANT, result.getType());
    assertFalse(result.isNullable());
  }

  @Test
  void testArrayType() {
    org.everit.json.schema.Schema jsonSchema = org.everit.json.schema.ArraySchema.builder()
        .allItemSchema(StringSchema.builder().build())
        .build();
    Schema result = JsonToLogicalTypeConverter.toRootSchema(new JsonSchema(jsonSchema));
    assertEquals(Schema.Type.ARRAY, result.getType());
    assertEquals(Schema.Type.VARCHAR, result.getElementType().getType());
  }

  @Test
  void testTypeMappings() {
    // Matrix-driven coverage. Each TypeMapping in CommonMappings goes
    // LT -> JSON -> LT and the result must equal the original. Adding a new
    // primitive Schema.Type without registering a mapping here surfaces as a
    // failing test until coverage is added.
    for (CommonMappings.TypeMapping mapping : CommonMappings.get()) {
      Schema original = mapping.asRootStruct();
      Schema rt = JsonToLogicalTypeConverter.toRootSchema(
          LogicalTypeToJsonConverter.fromLogicalType(
              new io.confluent.kafka.schemaregistry.type.logical.LogicalType(original),
              "Holder"));
      assertEquals(original, rt, "Round trip failed for " + mapping);
    }
  }

  @Test
  void testNullableProperUnion() {
    // Three-way union of null + two non-null types becomes a nullable UNION
    // with 2 non-null branches.
    org.everit.json.schema.Schema jsonSchema = CombinedSchema.builder()
        .criterion(CombinedSchema.ONE_CRITERION)
        .subschema(NullSchema.INSTANCE)
        .subschema(NumberSchema.builder()
            .unprocessedProperties(Collections.singletonMap("connect.type", "int32"))
            .build())
        .subschema(StringSchema.builder().build())
        .build();
    Schema result = JsonToLogicalTypeConverter.toRootSchema(new JsonSchema(jsonSchema));
    assertEquals(Schema.Type.UNION, result.getType());
    assertTrue(result.isNullable());
    assertEquals(2, result.getBranches().size());
  }

  @Test
  void testUnionWithManyBranches() {
    // 4-branch union (no null) — order preserved.
    org.everit.json.schema.Schema jsonSchema = CombinedSchema.builder()
        .criterion(CombinedSchema.ONE_CRITERION)
        .subschema(NumberSchema.builder()
            .unprocessedProperties(Collections.singletonMap("connect.type", "int32"))
            .build())
        .subschema(StringSchema.builder().build())
        .subschema(BooleanSchema.builder().build())
        .subschema(NumberSchema.builder()
            .unprocessedProperties(Collections.singletonMap("connect.type", "float64"))
            .build())
        .build();
    Schema result = JsonToLogicalTypeConverter.toRootSchema(new JsonSchema(jsonSchema));
    assertEquals(Schema.Type.UNION, result.getType());
    assertEquals(4, result.getBranches().size());
  }

  @Test
  void testUnionBranchNamesUseTitleInV2ButSynthesizeInV1() {
    // A 2-branch union whose subschemas set `title`. V2 uses the titles as the
    // branch names; V1 (the Flink-byte-compat edition) ignores titles and
    // synthesizes connect_union_field_<index>, matching the old Flink converter
    // and keeping union RowType field names stable across the upgrade.
    org.everit.json.schema.Schema jsonSchema = CombinedSchema.builder()
        .criterion(CombinedSchema.ONE_CRITERION)
        .subschema(StringSchema.builder().title("myString").build())
        .subschema(BooleanSchema.builder().title("myBool").build())
        .build();

    Schema v1 = JsonToLogicalTypeConverter
        .toLogicalType(new JsonSchema(jsonSchema), LogicalTypeVersion.V1).getRootSchema();
    assertEquals(Schema.Type.UNION, v1.getType());
    assertEquals("connect_union_field_0", v1.getBranches().get(0).getName());
    assertEquals("connect_union_field_1", v1.getBranches().get(1).getName());

    Schema v2 = JsonToLogicalTypeConverter.toRootSchema(new JsonSchema(jsonSchema));
    assertEquals(Schema.Type.UNION, v2.getType());
    assertEquals("myString", v2.getBranches().get(0).getName());
    assertEquals("myBool", v2.getBranches().get(1).getName());
  }

  // A oneOf of the given branches, each an object of the given properties.
  private static String oneOf(String... branches) {
    return "{\"oneOf\":[" + String.join(",", branches) + "]}";
  }

  private static String object(String... properties) {
    return "{\"type\":\"object\",\"properties\":{" + String.join(",", properties) + "}}";
  }

  private static List<String> branchNamesOf(Schema union) {
    return union.getBranches().stream().map(Schema.UnionBranch::getName)
        .collect(Collectors.toList());
  }

  @Test
  void aTaggedUnionsBranchesAreNamedByTheirTagsInV2() {
    // One key, one string value per branch, as a one-value enum, a const or a typed const: the
    // tag names the branch over its title. V1 keeps synthesizing names.
    String json = oneOf(
        "{\"type\":\"object\",\"title\":\"CardPayment\",\"properties\":"
            + "{\"kind\":{\"enum\":[\"card\"]},\"n\":{\"type\":\"string\"}}}",
        object("\"kind\":{\"const\":\"bank\"}", "\"i\":{\"type\":\"string\"}"),
        object("\"kind\":{\"type\":\"string\",\"const\":\"cash\"}"),
        object("\"kind\":{\"type\":[\"string\",\"null\"],\"const\":\"gift\"}"));
    assertThat(branchNamesOf(rootOf(json))).containsExactly("card", "bank", "cash", "gift");
    assertThat(branchNamesOf(v1RootOf(json))).containsExactly("connect_union_field_0",
        "connect_union_field_1", "connect_union_field_2", "connect_union_field_3");
  }

  @Test
  void aTagIsFoundThroughAReferenceOrAnAllOf() {
    String json = "{\"oneOf\":[{\"$ref\":\"#/definitions/Card\"},"
        + "{\"allOf\":[" + object("\"kind\":{\"const\":\"bank\"}") + ","
        + object("\"i\":{\"type\":\"string\"}") + "]}],"
        + "\"definitions\":{\"Card\":" + object("\"kind\":{\"const\":\"card\"}",
            "\"n\":{\"type\":\"string\"}") + "}}";
    assertThat(branchNamesOf(rootOf(json))).containsExactly("card", "bank");
  }

  @Test
  void aHintedBranchKeepsItsHintBesideTaggedOnes() {
    String json = "{\"oneOf\":[" + object("\"kind\":{\"const\":\"card\"}") + ","
        + object("\"kind\":{\"const\":\"bank\"}") + "],"
        + "\"confluent:union\":[{\"name\":\"Primary\"},{}]}";
    assertThat(branchNamesOf(rootOf(json))).containsExactly("Primary", "bank");
  }

  @Test
  void aUnionNotCleanlyTaggedKeepsItsTitlesOrPositions() {
    String card = "{\"type\":\"object\",\"title\":\"Card\",\"properties\":{%s}}";
    String bank = "{\"type\":\"object\",\"properties\":{%s}}";
    String[][] cases = {
        // A branch with no tag.
        {"\"kind\":{\"const\":\"card\"}", "\"i\":{\"type\":\"string\"}"},
        // A branch with two.
        {"\"kind\":{\"const\":\"card\"},\"sub\":{\"const\":\"x\"}",
            "\"kind\":{\"const\":\"bank\"}"},
        // Tags under different keys.
        {"\"kind\":{\"const\":\"card\"}", "\"type\":{\"const\":\"bank\"}"},
        // One value for both.
        {"\"kind\":{\"const\":\"card\"}", "\"kind\":{\"const\":\"card\"}"},
        // A tag that is no string.
        {"\"kind\":{\"const\":\"card\"}", "\"kind\":{\"const\":1}"},
    };
    for (String[] branches : cases) {
      String json = oneOf(String.format(card, branches[0]), String.format(bank, branches[1]));
      assertThat(branchNamesOf(rootOf(json))).as(json)
          .containsExactly("Card", "connect_union_field_1");
    }
    // A tag naming a branch as another's hint does: the union keeps its hints and titles.
    String json = "{\"oneOf\":[" + String.format(card, "\"kind\":{\"const\":\"card\"}")
        + "," + String.format(bank, "\"kind\":{\"const\":\"bank\"}") + "],"
        + "\"confluent:union\":[{},{\"name\":\"card\"}]}";
    assertThat(branchNamesOf(rootOf(json))).containsExactly("Card", "card");
  }

  @Test
  void tagNamesRoundTripThroughJson() {
    // Written back as confluent:union hints, which the next conversion reads first.
    String json = oneOf(object("\"kind\":{\"const\":\"card\"}", "\"n\":{\"type\":\"string\"}"),
        object("\"kind\":{\"const\":\"bank\"}", "\"i\":{\"type\":\"string\"}"));
    LogicalType lt = JsonToLogicalTypeConverter.toLogicalType(new JsonSchema(json));
    JsonSchema out = LogicalTypeToJsonConverter.fromLogicalType(lt, "Payment");
    assertThat(branchNamesOf(JsonToLogicalTypeConverter.toRootSchema(out)))
        .containsExactly("card", "bank");
  }

  @Test
  void testSingletonOneOfCollapsesToMemberType() {
    // oneOf:[T] is semantically equivalent to T in JSON Schema (the value
    // satisfies exactly one schema, but there's only one option). Reader
    // collapses it to the member type — matches Avro behavior.
    org.everit.json.schema.Schema jsonSchema = CombinedSchema.builder()
        .criterion(CombinedSchema.ONE_CRITERION)
        .subschema(StringSchema.builder().build())
        .build();
    Schema result = JsonToLogicalTypeConverter.toRootSchema(new JsonSchema(jsonSchema));
    assertEquals(Schema.Type.VARCHAR, result.getType());
    assertFalse(result.isNullable());
  }

  @Test
  void testSingletonOneOfWithNullCollapsesToNullableMember() {
    // oneOf:[null, T] also collapses (equivalent to nullable T). Existing
    // behavior — verified explicitly here for parity with the singleton case.
    org.everit.json.schema.Schema jsonSchema = CombinedSchema.builder()
        .criterion(CombinedSchema.ONE_CRITERION)
        .subschema(NullSchema.INSTANCE)
        .subschema(StringSchema.builder().build())
        .build();
    Schema result = JsonToLogicalTypeConverter.toRootSchema(new JsonSchema(jsonSchema));
    assertEquals(Schema.Type.VARCHAR, result.getType());
    assertTrue(result.isNullable());
  }

  @Test
  void testEmptyObjectIsStruct() {
    // An object schema with no properties round-trips as an empty STRUCT.
    org.everit.json.schema.Schema jsonSchema = ObjectSchema.builder()
        .additionalProperties(false)
        .build();
    Schema result = JsonToLogicalTypeConverter.toRootSchema(new JsonSchema(jsonSchema));
    assertEquals(Schema.Type.STRUCT, result.getType());
    assertTrue(result.getFields().isEmpty());
  }

  @Test
  void testSingletonObjectOneOfIsUnionInV1ButCollapsesInV2() {
    // Mirrors getNullableReference: a single-member oneOf of an object stays a
    // 1-branch union under V1 (old Flink's union-wrapper) but collapses to the
    // member under V2 (canonical).
    org.everit.json.schema.Schema oneOf = CombinedSchema.builder()
        .criterion(CombinedSchema.ONE_CRITERION)
        .subschema(ObjectSchema.builder()
            .addPropertySchema("x", NumberSchema.builder().requiresInteger(true).build())
            .build())
        .build();

    Schema v1 = JsonToLogicalTypeConverter
        .toLogicalType(new JsonSchema(oneOf), LogicalTypeVersion.V1).getRootSchema();
    assertEquals(Schema.Type.UNION, v1.getType());
    assertEquals(1, v1.getBranches().size());
    assertEquals(Schema.Type.STRUCT, v1.getBranches().get(0).getSchema().getType());

    Schema v2 = JsonToLogicalTypeConverter.toRootSchema(new JsonSchema(oneOf));
    assertEquals(Schema.Type.STRUCT, v2.getType());
  }

  @Test
  void testSingletonOneOfFieldIsUnionInV1ButCollapsesInV2() {
    // Mirrors testSchemaReference (nested references): a field whose type is a
    // single-member oneOf stays a union field under V1, collapses under V2.
    org.everit.json.schema.Schema root = ObjectSchema.builder()
        .addPropertySchema("f1", CombinedSchema.builder()
            .criterion(CombinedSchema.ONE_CRITERION)
            .subschema(ObjectSchema.builder()
                .addPropertySchema("x", StringSchema.builder().build()).build())
            .build())
        .addRequiredProperty("f1")
        .build();

    Schema f1V1 = JsonToLogicalTypeConverter
        .toLogicalType(new JsonSchema(root), LogicalTypeVersion.V1)
        .getRootSchema().getField("f1").getSchema();
    assertEquals(Schema.Type.UNION, f1V1.getType());

    Schema f1V2 = JsonToLogicalTypeConverter.toRootSchema(new JsonSchema(root))
        .getField("f1").getSchema();
    assertEquals(Schema.Type.STRUCT, f1V2.getType());
  }

  @Test
  void testSingletonOneOfOfReferenceIsUnionInV1() {
    // Mirrors testRoundTrip[5] / the reference cases: a single-member oneOf of a
    // $ref stays a 1-branch union (wrapping a NAMED_TYPE_REF) under V1 and
    // collapses to the NAMED_TYPE_REF under V2.
    String json = "{\"type\":\"object\","
        + "\"properties\":{\"f1\":{\"oneOf\":[{\"$ref\":\"#/definitions/T\"}]}},"
        + "\"required\":[\"f1\"],"
        + "\"definitions\":{\"T\":{\"type\":\"object\","
        + "\"properties\":{\"x\":{\"type\":\"string\"}}}}}";

    Schema f1V1 = JsonToLogicalTypeConverter
        .toLogicalType(new JsonSchema(json), LogicalTypeVersion.V1)
        .getRootSchema().getField("f1").getSchema();
    assertEquals(Schema.Type.UNION, f1V1.getType());

    Schema f1V2 = JsonToLogicalTypeConverter.toRootSchema(new JsonSchema(json))
        .getField("f1").getSchema();
    assertEquals(Schema.Type.NAMED_TYPE_REF, f1V2.getType());
  }

  @Test
  void testReferenceToNullableObjectStaysNullable() {
    // Mirrors getNonRequiredReference: a required field referencing a nullable
    // object (type:[object,null]) must keep the referenced type nullable, so the
    // Flink projection reads it as a nullable row rather than a union.
    String json = "{\"$schema\":\"http://json-schema.org/draft-07/schema#\","
        + "\"type\":\"object\","
        + "\"properties\":{\"ref1\":{\"$ref\":\"#/definitions/opt\"}},"
        + "\"required\":[\"ref1\"],"
        + "\"definitions\":{\"opt\":{\"type\":[\"object\",\"null\"],"
        + "\"properties\":{\"x\":{\"type\":\"string\"}}}}}";

    LogicalType lt = JsonToLogicalTypeConverter.toLogicalType(
        new JsonSchema(json), LogicalTypeVersion.V1);
    Schema opt = lt.getNamedTypes().get("opt");
    assertTrue(opt != null && opt.isNullable(),
        "referenced nullable object must remain nullable: " + lt.getNamedTypes());
  }

  @Test
  void ifThenElseIsRejectedRatherThanDroppingTheConditionalBranchesProperties() {
    // everit parses this as allOf[ObjectSchema, ConditionalSchema]. Without the guard the
    // ConditionalSchema falls through simplifyAllOfSchema's type chain and is discarded, yielding
    // STRUCT(a BIGINT) -- so `b`, required only when a == 1, has no column and every record carrying
    // it loses that value with no error anywhere.
    assertThatThrownBy(() -> convert("{\"type\":\"object\","
        + "\"properties\":{\"a\":{\"type\":\"integer\"}},"
        + "\"if\":{\"properties\":{\"a\":{\"const\":1}}},"
        + "\"then\":{\"required\":[\"b\"],\"properties\":{\"b\":{\"type\":\"string\"}}}}"))
        .isInstanceOf(ValidationException.class);
  }

  @Test
  void ifThenElseIsRejectedEvenWhenTheBranchesDeclareNoNewProperties() {
    // Uniform rejection. Making the guard depend on the branch contents would mean a later edit to
    // those branches silently changed whether the schema converts.
    assertThatThrownBy(() -> convert("{\"type\":\"object\","
        + "\"properties\":{\"a\":{\"type\":\"integer\"}},"
        + "\"if\":{\"required\":[\"a\"]},\"then\":{\"required\":[\"a\"]}}"))
        .isInstanceOf(ValidationException.class);
  }

  @Test
  void notIsRejectedBecauseItIsHowAConditionalWouldBeRewritten() {
    // On its own `not` costs no column, only a constraint -- which is why it was initially out of
    // scope. It is in scope because JSON Schema has no implication operator, so any conditional can
    // be rewritten using it, and rejecting only the sugar would be trivially bypassed.
    assertThatThrownBy(() -> convert("{\"type\":\"object\","
        + "\"properties\":{\"a\":{\"type\":\"integer\"}},"
        + "\"not\":{\"required\":[\"a\"]}}"))
        .isInstanceOf(ValidationException.class);
  }

  @Test
  void theDesugaredFormOfAConditionalIsStillNotRejected() {
    // OPEN BYPASS, pinned so it is not mistaken for closed. simplifyAllOfSchema inspects only the
    // *immediate* subschemas of the allOf, so nesting the negation one level deeper inside an anyOf
    // escapes the guard and drops `b` exactly as the sugar would. Naming more types will not fix it;
    // the root cause is the method discarding any subschema it cannot merge.
    LogicalType type = convert("{\"allOf\":["
        + "{\"type\":\"object\",\"properties\":{\"a\":{\"type\":\"integer\"}}},"
        + "{\"anyOf\":["
        + "{\"not\":{\"properties\":{\"a\":{\"const\":1}}}},"
        + "{\"required\":[\"b\"],\"properties\":{\"b\":{\"type\":\"string\"}}}]}]}");
    assertEquals("STRUCT(a BIGINT) NOT NULL", type.getRootSchema().toDdl());
  }

  @Test
  void anAnyOfOfObjectShapesInsideAnAllOfStillLosesItsProperties() {
    // The same root cause with no conditional involved at all: `b` and `c` are both dropped. A merge
    // gap rather than a condition -- the right fix is to collect the branch properties as nullable
    // columns, not to reject the schema.
    LogicalType type = convert("{\"allOf\":["
        + "{\"type\":\"object\",\"properties\":{\"a\":{\"type\":\"integer\"}}},"
        + "{\"anyOf\":["
        + "{\"type\":\"object\",\"properties\":{\"b\":{\"type\":\"string\"}}},"
        + "{\"type\":\"object\",\"properties\":{\"c\":{\"type\":\"boolean\"}}}]}]}");
    assertEquals("STRUCT(a BIGINT) NOT NULL", type.getRootSchema().toDdl());
  }

  @Test
  void dependentRequiredIsStillAcceptedBecauseBothPropertiesKeepTheirColumns() {
    LogicalType type = convert("{\"type\":\"object\",\"properties\":{"
        + "\"a\":{\"type\":\"integer\"},\"b\":{\"type\":\"string\"}},"
        + "\"dependentRequired\":{\"a\":[\"b\"]}}");
    assertEquals("STRUCT(a BIGINT, b STRING) NOT NULL", type.getRootSchema().toDdl());
  }

  @Test
  void anOrdinaryAllOfMergeStillWorks() {
    // The guard sits inside allOf simplification, so this is the regression that matters most.
    LogicalType type = convert("{\"allOf\":["
        + "{\"type\":\"object\",\"properties\":{\"a\":{\"type\":\"integer\"}}},"
        + "{\"type\":\"object\",\"properties\":{\"b\":{\"type\":\"string\"}}}]}");
    assertEquals("STRUCT(a BIGINT, b STRING) NOT NULL", type.getRootSchema().toDdl());
  }

  private static LogicalType convert(String jsonText) {
    return JsonToLogicalTypeConverter.toLogicalType(new JsonSchema(jsonText));
  }

  @Test
  void rootObjectTitleCarriedAsName() {
    // A root object is inline (no namedTypes key), so its `title` would be lost; carry it as the
    // LogicalType root name.
    LogicalType lt = convert(
        "{\"type\":\"object\",\"title\":\"Order\","
            + "\"properties\":{\"id\":{\"type\":\"string\"}}}");

    assertThat(lt.getRootSchema().getType()).isEqualTo(Schema.Type.STRUCT);
    assertThat(lt.getName()).isEqualTo("Order");
  }

  @Test
  void aNullEnumMemberMakesTheEnumNullable() {
    Schema p = convert("{\"type\":\"object\",\"properties\":"
        + "{\"p\":{\"enum\":[\"a\",\"b\",null]}}}").getRootSchema().getField("p").getSchema();

    assertThat(p.getType()).isEqualTo(Schema.Type.ENUM);
    assertThat(p.isNullable()).isTrue();
    assertThat(p.getEnumValues()).extracting(Schema.EnumValue::getSymbol).containsExactly("a", "b");
  }

  @Test
  void anEnumOfOnlyNullIsRejected() {
    assertThatThrownBy(() -> convert("{\"type\":\"object\",\"properties\":"
        + "{\"p\":{\"enum\":[null]}}}")).isInstanceOf(ValidationException.class);
  }

  @Test
  void aMapWithNoValueSchemaIsRejected() {
    assertThatThrownBy(() -> convert("{\"type\":\"object\",\"properties\":"
        + "{\"m\":{\"type\":\"object\",\"connect.type\":\"map\"}}}"))
        .isInstanceOf(ValidationException.class);
  }

  @Test
  void malformedConverterMetadataIsRejectedByName() {
    // Metadata of an unexpected shape is a schema the converter cannot read, named as such; a
    // ClassCastException would reach the provenance endpoint as a 500.
    String two = "[{\"type\":\"object\",\"properties\":{\"a\":{\"type\":\"string\"}}},"
        + "{\"type\":\"object\",\"properties\":{\"b\":{\"type\":\"integer\"}}}]";
    String[] properties = {
        "\"u\":{\"oneOf\":" + two + ",\"confluent:union\":{\"x\":1}}",
        "\"u\":{\"oneOf\":" + two + ",\"confluent:union\":[{\"name\":1},{\"name\":2}]}",
        "\"e\":{\"enum\":[\"A\",\"B\"],\"confluent:enum\":[\"a\"]}",
        "\"n\":{\"type\":\"integer\",\"connect.type\":1}",
        "\"n\":{\"type\":\"string\",\"connect.type\":null}",
        "\"n\":{\"type\":\"number\",\"title\":\"org.apache.kafka.connect.data.Decimal\","
            + "\"connect.type\":\"bytes\",\"connect.parameters\":{\"scale\":\"x\"}}",
        "\"n\":{\"type\":\"integer\",\"title\":\"org.apache.kafka.connect.data.Timestamp\","
            + "\"connect.type\":\"int64\",\"flink.precision\":\"3\"}",
        "\"n\":{\"type\":\"string\",\"connect.type\":\"bytes\",\"flink.maxLength\":\"5\"}",
        "\"a\":{\"type\":\"string\",\"connect.index\":\"1\"},\"b\":{\"type\":\"string\"}",
        "\"m\":{\"type\":\"array\",\"connect.type\":\"map\",\"items\":{\"type\":\"object\"}}"};
    for (String property : properties) {
      assertThatThrownBy(() -> JsonToLogicalTypeConverter.toLogicalType(new JsonSchema(
          "{\"type\":\"object\",\"properties\":{" + property + "}}")))
          .as(property).isInstanceOf(ValidationException.class);
    }
  }


  @Test
  void aModernDraftDefaultBesideARefIsTheFieldsDefault() {
    // 2019-09 and later honour a default beside $ref; it is read against the type the reference
    // names, not the reference itself.
    LogicalType lt = JsonToLogicalTypeConverter.toLogicalType(new JsonSchema("{\"$schema\":"
        + "\"https://json-schema.org/draft/2020-12/schema\",\"type\":\"object\","
        + "\"properties\":{\"t\":{\"$ref\":\"#/$defs/T\",\"default\":\"sib\"}},"
        + "\"$defs\":{\"T\":{\"type\":\"string\"}}}"));
    Schema.Field t = lt.getRootSchema().getFields().get(0);
    assertEquals("sib", t.getDefaultValue());
  }

  @Test
  void aDefinitionReferringOnlyToItselfConvertsWithoutLooping() {
    // D refers only to itself, directly or through Q: a recursive type, still converted.
    String direct = "{\"type\":\"object\",\"properties\":{\"d\":{\"$ref\":\"#/definitions/D\"}},"
        + "\"definitions\":{\"D\":{\"$ref\":\"#/definitions/D\"}}}";
    String indirect = "{\"type\":\"object\",\"properties\":{\"d\":{\"$ref\":\"#/definitions/D\"}},"
        + "\"definitions\":{\"D\":{\"$ref\":\"#/definitions/Q\"},"
        + "\"Q\":{\"$ref\":\"#/definitions/D\"}}}";
    for (String schema : Arrays.asList(direct, indirect)) {
      LogicalType lt = assertTimeoutPreemptively(Duration.ofSeconds(10), () ->
          JsonToLogicalTypeConverter.toLogicalType(new JsonSchema(schema), LogicalTypeVersion.V1));
      assertThat(lt.getNamedTypes()).isNotEmpty();
    }
  }
}
