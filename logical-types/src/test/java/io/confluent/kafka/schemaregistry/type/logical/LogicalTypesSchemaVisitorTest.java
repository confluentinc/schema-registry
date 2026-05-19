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

package io.confluent.kafka.schemaregistry.type.logical;

import io.confluent.kafka.schemaregistry.type.logical.generated.LogicalTypesParser;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class LogicalTypesSchemaVisitorTest {

  // --- helpers ---

  private Schema parseTypeExpr(String input) {
    LogicalTypesParser parser = LogicalTypesParserFactory.newParser(input);
    LogicalTypesSchemaVisitor visitor = new LogicalTypesSchemaVisitor();
    return (Schema) visitor.visit(parser.typeExpr());
  }

  private LogicalTypesSchemaVisitor parseScript(String input) {
    LogicalTypesSchemaVisitor visitor = new LogicalTypesSchemaVisitor();
    visitor.visit(LogicalTypesParserFactory.parse(input));
    return visitor;
  }

  // =========================================================================
  // Primitive types
  // =========================================================================

  @Test
  void testBoolean() {
    Schema s = parseTypeExpr("BOOLEAN");
    assertEquals(Schema.Type.BOOLEAN, s.getType());
    assertTrue(s.isNullable());
  }

  @Test
  void testTinyint() {
    Schema s = parseTypeExpr("TINYINT");
    assertEquals(Schema.Type.TINYINT, s.getType());
  }

  @Test
  void testSmallint() {
    Schema s = parseTypeExpr("SMALLINT");
    assertEquals(Schema.Type.SMALLINT, s.getType());
  }

  @Test
  void testInt() {
    Schema s = parseTypeExpr("INT");
    assertEquals(Schema.Type.INT, s.getType());
  }

  @Test
  void testInteger() {
    Schema s = parseTypeExpr("INTEGER");
    assertEquals(Schema.Type.INT, s.getType());
  }

  @Test
  void testBigint() {
    Schema s = parseTypeExpr("BIGINT");
    assertEquals(Schema.Type.BIGINT, s.getType());
  }

  @Test
  void testFloat() {
    Schema s = parseTypeExpr("FLOAT");
    assertEquals(Schema.Type.FLOAT, s.getType());
  }

  @Test
  void testDouble() {
    Schema s = parseTypeExpr("DOUBLE");
    assertEquals(Schema.Type.DOUBLE, s.getType());
  }

  @Test
  void testDoublePrecision() {
    Schema s = parseTypeExpr("DOUBLE PRECISION");
    assertEquals(Schema.Type.DOUBLE, s.getType());
  }

  @Test
  void testReal() {
    Schema s = parseTypeExpr("REAL");
    assertEquals(Schema.Type.FLOAT, s.getType());
  }

  @Test
  void testCharacterAlias() {
    Schema s = parseTypeExpr("CHARACTER(10)");
    assertEquals(Schema.Type.CHAR, s.getType());
    assertEquals(10, s.getLength());
  }

  @Test
  void testCharacterVarying() {
    Schema s = parseTypeExpr("CHARACTER VARYING(100)");
    assertEquals(Schema.Type.VARCHAR, s.getType());
    assertEquals(100, s.getLength());
  }

  @Test
  void testBinaryVarying() {
    Schema s = parseTypeExpr("BINARY VARYING(256)");
    assertEquals(Schema.Type.VARBINARY, s.getType());
    assertEquals(256, s.getLength());
  }

  @Test
  void testString() {
    Schema s = parseTypeExpr("STRING");
    assertEquals(Schema.Type.VARCHAR, s.getType());
  }

  @Test
  void testBytes() {
    Schema s = parseTypeExpr("BYTES");
    assertEquals(Schema.Type.VARBINARY, s.getType());
  }

  @Test
  void testDate() {
    Schema s = parseTypeExpr("DATE");
    assertEquals(Schema.Type.DATE, s.getType());
  }

  @Test
  void testVariant() {
    Schema s = parseTypeExpr("VARIANT");
    assertEquals(Schema.Type.VARIANT, s.getType());
  }

  @Test
  void testCaseInsensitive() {
    Schema s = parseTypeExpr("boolean");
    assertEquals(Schema.Type.BOOLEAN, s.getType());

    s = parseTypeExpr("Bigint");
    assertEquals(Schema.Type.BIGINT, s.getType());
  }

  // =========================================================================
  // Nullability
  // =========================================================================

  @Test
  void testDefaultNullable() {
    Schema s = parseTypeExpr("INT");
    assertTrue(s.isNullable());
  }

  @Test
  void testNotNull() {
    Schema s = parseTypeExpr("INT NOT NULL");
    assertFalse(s.isNullable());
    assertEquals(Schema.Type.INT, s.getType());
  }

  @Test
  void testExplicitNull() {
    Schema s = parseTypeExpr("INT NULL");
    assertTrue(s.isNullable());
  }

  @Test
  void testArrayElementNotNull() {
    Schema s = parseTypeExpr("ARRAY<INT NOT NULL>");
    assertEquals(Schema.Type.ARRAY, s.getType());
    assertTrue(s.isNullable());
    assertFalse(s.getElementType().isNullable());
  }

  @Test
  void testArrayNotNull() {
    Schema s = parseTypeExpr("ARRAY<INT> NOT NULL");
    assertFalse(s.isNullable());
    assertTrue(s.getElementType().isNullable());
  }

  @Test
  void testMapKeyValueNullability() {
    Schema s = parseTypeExpr("MAP<STRING NOT NULL, INT NULL>");
    assertEquals(Schema.Type.MAP, s.getType());
    assertFalse(s.getKeyType().isNullable());
    assertTrue(s.getValueType().isNullable());
  }

  @Test
  void testRowFieldNullability() {
    Schema s = parseTypeExpr("ROW(id BIGINT NOT NULL, name STRING)");
    List<Schema.Field> fields = s.getFields();
    assertFalse(fields.get(0).getSchema().isNullable());
    assertTrue(fields.get(1).getSchema().isNullable());
  }

  @Test
  void testPostfixArrayNotNull() {
    Schema s = parseTypeExpr("INT NOT NULL ARRAY");
    assertEquals(Schema.Type.ARRAY, s.getType());
    assertTrue(s.isNullable());
    assertFalse(s.getElementType().isNullable());
  }

  @Test
  void testNotNullToDdl() {
    assertEquals("INT NOT NULL", parseTypeExpr("INT NOT NULL").toDdl());
    assertEquals("INT", parseTypeExpr("INT").toDdl());
    assertEquals("INT", parseTypeExpr("INT NULL").toDdl());
    assertEquals("ARRAY<INT NOT NULL>", parseTypeExpr("ARRAY<INT NOT NULL>").toDdl());
    assertEquals("ARRAY<INT> NOT NULL", parseTypeExpr("ARRAY<INT> NOT NULL").toDdl());
  }

  @Test
  void testNullabilityEquality() {
    assertEquals(parseTypeExpr("INT"), parseTypeExpr("INT NULL"));
    assertFalse(parseTypeExpr("INT").equals(parseTypeExpr("INT NOT NULL")));
  }

  // =========================================================================
  // Parameterized primitive types
  // =========================================================================

  @Test
  void testDecimalNoParams() {
    Schema s = parseTypeExpr("DECIMAL");
    assertEquals(Schema.Type.DECIMAL, s.getType());
    assertEquals(Schema.DEFAULT_DECIMAL_PRECISION, s.getPrecision());
    // Scale stays NO_PARAM ("scale omitted") — coercing to 0 here would
    // lose the user's intent and round-trip `DECIMAL` as `DECIMAL(p, 0)`.
    assertEquals(Schema.NO_PARAM, s.getScale());
  }

  @Test
  void testDecimalWithPrecision() {
    Schema s = parseTypeExpr("DECIMAL(10)");
    assertEquals(Schema.Type.DECIMAL, s.getType());
    assertEquals(10, s.getPrecision());
    // Scale stays NO_PARAM so toDdl can round-trip `DECIMAL(10)`.
    assertEquals(Schema.NO_PARAM, s.getScale());
  }

  @Test
  void testDecimalWithPrecisionAndScale() {
    Schema s = parseTypeExpr("DECIMAL(10, 2)");
    assertEquals(Schema.Type.DECIMAL, s.getType());
    assertEquals(10, s.getPrecision());
    assertEquals(2, s.getScale());
  }

  @Test
  void testDecimalDdlRoundTripsScaleOmitted() {
    // Round-trip: parsed `DECIMAL(10)` → schema → toDdl must yield
    // `DECIMAL(10)`, not the previously-emitted `DECIMAL(10, 0)` (the
    // constructor used to coerce NO_PARAM scale to DEFAULT_DECIMAL_SCALE).
    assertEquals("DECIMAL(10)", parseTypeExpr("DECIMAL(10)").toDdl());
    assertEquals("DECIMAL(10, 2)", parseTypeExpr("DECIMAL(10, 2)").toDdl());
    // Bare `DECIMAL` still coerces precision to DEFAULT_DECIMAL_PRECISION
    // (separate concern); round-trip emits `DECIMAL(10)` rather than
    // `DECIMAL`.
    assertEquals("DECIMAL(10)", parseTypeExpr("DECIMAL").toDdl());
  }

  @Test
  void testDecAliasForDecimal() {
    Schema s = parseTypeExpr("DEC(5, 3)");
    assertEquals(Schema.Type.DECIMAL, s.getType());
    assertEquals(5, s.getPrecision());
    assertEquals(3, s.getScale());
  }

  @Test
  void testNumericAliasForDecimal() {
    Schema s = parseTypeExpr("NUMERIC(18)");
    assertEquals(Schema.Type.DECIMAL, s.getType());
    assertEquals(18, s.getPrecision());
  }

  @Test
  void testVarcharNoLength() {
    Schema s = parseTypeExpr("VARCHAR");
    assertEquals(Schema.Type.VARCHAR, s.getType());
    assertEquals(Schema.DEFAULT_LENGTH, s.getLength());
  }

  @Test
  void testVarcharWithLength() {
    Schema s = parseTypeExpr("VARCHAR(255)");
    assertEquals(Schema.Type.VARCHAR, s.getType());
    assertEquals(255, s.getLength());
  }

  @Test
  void testCharWithLength() {
    Schema s = parseTypeExpr("CHAR(10)");
    assertEquals(Schema.Type.CHAR, s.getType());
    assertEquals(10, s.getLength());
  }

  @Test
  void testBinaryNoLength() {
    Schema s = parseTypeExpr("BINARY");
    assertEquals(Schema.Type.BINARY, s.getType());
    assertEquals(Schema.DEFAULT_LENGTH, s.getLength());
  }

  @Test
  void testBinaryWithLength() {
    Schema s = parseTypeExpr("BINARY(16)");
    assertEquals(Schema.Type.BINARY, s.getType());
    assertEquals(16, s.getLength());
  }

  @Test
  void testVarbinaryWithLength() {
    Schema s = parseTypeExpr("VARBINARY(1024)");
    assertEquals(Schema.Type.VARBINARY, s.getType());
    assertEquals(1024, s.getLength());
  }

  @Test
  void testTimeNoPrecision() {
    Schema s = parseTypeExpr("TIME");
    assertEquals(Schema.Type.TIME, s.getType());
    assertEquals(Schema.DEFAULT_TIME_PRECISION, s.getPrecision());
  }

  @Test
  void testTimeWithPrecision() {
    Schema s = parseTypeExpr("TIME(3)");
    assertEquals(Schema.Type.TIME, s.getType());
    assertEquals(3, s.getPrecision());
  }

  @Test
  void testTimestampNoPrecision() {
    Schema s = parseTypeExpr("TIMESTAMP");
    assertEquals(Schema.Type.TIMESTAMP, s.getType());
    assertEquals(Schema.DEFAULT_TIMESTAMP_PRECISION, s.getPrecision());
  }

  @Test
  void testTimestampWithPrecision() {
    Schema s = parseTypeExpr("TIMESTAMP(6)");
    assertEquals(Schema.Type.TIMESTAMP, s.getType());
    assertEquals(6, s.getPrecision());
  }

  @Test
  void testTimestampWithoutTimeZone() {
    Schema s = parseTypeExpr("TIMESTAMP(3) WITHOUT TIME ZONE");
    assertEquals(Schema.Type.TIMESTAMP, s.getType());
    assertEquals(3, s.getPrecision());
  }

  @Test
  void testTimestampWithLocalTimeZone() {
    Schema s = parseTypeExpr("TIMESTAMP(6) WITH LOCAL TIME ZONE");
    assertEquals(Schema.Type.TIMESTAMP_LTZ, s.getType());
    assertEquals(6, s.getPrecision());
  }

  @Test
  void testTimestampLtz() {
    Schema s = parseTypeExpr("TIMESTAMP_LTZ(9)");
    assertEquals(Schema.Type.TIMESTAMP_LTZ, s.getType());
    assertEquals(9, s.getPrecision());
  }

  // =========================================================================
  // Complex types
  // =========================================================================

  @Test
  void testPrefixArray() {
    Schema s = parseTypeExpr("ARRAY<INT>");
    assertEquals(Schema.Type.ARRAY, s.getType());
    assertEquals(Schema.Type.INT, s.getElementType().getType());
  }

  @Test
  void testPostfixArray() {
    Schema s = parseTypeExpr("INT ARRAY");
    assertEquals(Schema.Type.ARRAY, s.getType());
    assertEquals(Schema.Type.INT, s.getElementType().getType());
  }

  @Test
  void testNestedArray() {
    Schema s = parseTypeExpr("ARRAY<ARRAY<STRING>>");
    assertEquals(Schema.Type.ARRAY, s.getType());
    assertEquals(Schema.Type.ARRAY, s.getElementType().getType());
    assertEquals(Schema.Type.VARCHAR, s.getElementType().getElementType().getType());
  }

  @Test
  void testPrefixMultiset() {
    Schema s = parseTypeExpr("MULTISET<BIGINT>");
    assertEquals(Schema.Type.MULTISET, s.getType());
    assertEquals(Schema.Type.BIGINT, s.getElementType().getType());
  }

  @Test
  void testPostfixMultiset() {
    Schema s = parseTypeExpr("VARCHAR(100) MULTISET");
    assertEquals(Schema.Type.MULTISET, s.getType());
    assertEquals(Schema.Type.VARCHAR, s.getElementType().getType());
    assertEquals(100, s.getElementType().getLength());
  }

  @Test
  void testMap() {
    Schema s = parseTypeExpr("MAP<STRING, INT>");
    assertEquals(Schema.Type.MAP, s.getType());
    assertEquals(Schema.Type.VARCHAR, s.getKeyType().getType());
    assertEquals(Schema.Type.INT, s.getValueType().getType());
  }

  @Test
  void testMapWithComplexValue() {
    Schema s = parseTypeExpr("MAP<STRING, ARRAY<DOUBLE>>");
    assertEquals(Schema.Type.MAP, s.getType());
    assertEquals(Schema.Type.VARCHAR, s.getKeyType().getType());
    assertEquals(Schema.Type.ARRAY, s.getValueType().getType());
    assertEquals(Schema.Type.DOUBLE, s.getValueType().getElementType().getType());
  }

  @Test
  void testRow() {
    Schema s = parseTypeExpr("ROW(name STRING, age INT)");
    assertEquals(Schema.Type.STRUCT, s.getType());
    List<Schema.Field> fields = s.getFields();
    assertEquals(2, fields.size());
    assertEquals("name", fields.get(0).getName());
    assertEquals(Schema.Type.VARCHAR, fields.get(0).getSchema().getType());
    assertEquals(0, fields.get(0).getPosition());
    assertEquals("age", fields.get(1).getName());
    assertEquals(Schema.Type.INT, fields.get(1).getSchema().getType());
    assertEquals(1, fields.get(1).getPosition());
  }

  @Test
  void testRowAngleBrackets() {
    Schema s = parseTypeExpr("ROW<name STRING, age INT>");
    assertEquals(Schema.Type.STRUCT, s.getType());
    assertEquals(2, s.getFields().size());
  }

  @Test
  void testRowWithDefault() {
    Schema s = parseTypeExpr("ROW(status STRING DEFAULT 'active')");
    Schema.Field f = s.getFields().get(0);
    assertTrue(f.hasDefaultValue());
    assertEquals("active", f.getDefaultValue());
  }

  @Test
  void testRowWithComment() {
    Schema s = parseTypeExpr("ROW(id INT COMMENT 'primary key')");
    assertEquals("primary key", s.getFields().get(0).getDoc());
  }

  @Test
  void testRowWithShortComment() {
    Schema s = parseTypeExpr("ROW(id INT 'primary key')");
    assertEquals("primary key", s.getFields().get(0).getDoc());
  }

  @Test
  void testRowWithTags() {
    Schema s = parseTypeExpr("ROW(email STRING TAGS('PII', 'SENSITIVE'))");
    List<String> tags = s.getFields().get(0).getTags();
    assertEquals(2, tags.size());
    assertEquals("PII", tags.get(0));
    assertEquals("SENSITIVE", tags.get(1));
  }

  @Test
  void testRowWithParams() {
    Schema s = parseTypeExpr(
        "ROW(data BYTES WITH('encoding' = 'base64', 'maxSize' = '1024'))");
    Map<String, Object> props = s.getFields().get(0).getParams();
    assertEquals("base64", props.get("encoding"));
    assertEquals("1024", props.get("maxSize"));
  }

  @Test
  void testUnion() {
    Schema s = parseTypeExpr("UNION(text STRING, number INT, flag BOOLEAN)");
    assertEquals(Schema.Type.UNION, s.getType());
    List<Schema.UnionBranch> branches = s.getBranches();
    assertEquals(3, branches.size());
    assertEquals("text", branches.get(0).getName());
    assertEquals(Schema.Type.VARCHAR, branches.get(0).getSchema().getType());
    assertEquals("number", branches.get(1).getName());
    assertEquals(Schema.Type.INT, branches.get(1).getSchema().getType());
    assertEquals("flag", branches.get(2).getName());
    assertEquals(Schema.Type.BOOLEAN, branches.get(2).getSchema().getType());
  }

  @Test
  void testUnionAngleBracketSyntax() {
    // Mirrors ROW<...> — UNION accepts both <...> and (...) delimiter forms.
    Schema s = parseTypeExpr("UNION<text STRING, number INT, flag BOOLEAN>");
    assertEquals(Schema.Type.UNION, s.getType());
    List<Schema.UnionBranch> branches = s.getBranches();
    assertEquals(3, branches.size());
    assertEquals("text", branches.get(0).getName());
    assertEquals(Schema.Type.VARCHAR, branches.get(0).getSchema().getType());
    assertEquals("number", branches.get(1).getName());
    assertEquals(Schema.Type.INT, branches.get(1).getSchema().getType());
    assertEquals("flag", branches.get(2).getName());
    assertEquals(Schema.Type.BOOLEAN, branches.get(2).getSchema().getType());
  }

  @Test
  void testNamedTypeRef() {
    Schema s = parseTypeExpr("my.custom.Type");
    assertEquals(Schema.Type.NAMED_TYPE_REF, s.getType());
    assertEquals("my.custom.Type", s.getQualifiedName());
  }

  // =========================================================================
  // Full script tests
  // =========================================================================

  @Test
  void testDeclareNamespace() {
    LogicalTypesSchemaVisitor v = parseScript(
        "NAMESPACE com.example.events;"
        + "TYPE INT"
    );
    assertEquals("com.example.events", v.getNamespace());
  }

  @Test
  void testCreateTypeStruct() {
    LogicalTypesSchemaVisitor v = parseScript(
        "ROW Address ("
        + "  street VARCHAR(200),"
        + "  city STRING,"
        + "  zip CHAR(5)"
        + ");"
        + "TYPE INT"
    );
    Schema address = v.getNamedTypes().get("Address");
    assertNotNull(address);
    assertEquals(Schema.Type.STRUCT, address.getType());
    assertEquals(3, address.getFields().size());
    assertEquals("street", address.getFields().get(0).getName());
    assertEquals(200, address.getFields().get(0).getSchema().getLength());
  }

  @Test
  void testReferenceType() {
    LogicalTypesSchemaVisitor v = parseScript(
        "REFERENCE TYPE com.example.Money;"
        + "REFERENCE TYPE com.example.Address;"
        + "ROW MyOrder ("
        + "  amount Money,"
        + "  addr Address"
        + ");"
        + "TYPE MyOrder"
    );
    assertEquals(2, v.getReferencedTypes().size());
    assertEquals("com.example.Money", v.getReferencedTypes().get(0));
    assertEquals("com.example.Address", v.getReferencedTypes().get(1));
    Schema order = v.getNamedTypes().get("MyOrder");
    assertNotNull(order);
    assertEquals(Schema.Type.NAMED_TYPE_REF,
        order.getField("amount").getSchema().getType());
    assertEquals("Money",
        order.getField("amount").getSchema().getQualifiedName());
    // No AS SYNONYM FOR clauses → externalImports stays empty.
    assertTrue(v.getExternalImports().isEmpty());
  }

  @Test
  void testReferenceTypeWithFrom() {
    // Mix of canonical (no AS SYNONYM FOR) and synthetic-wrapper (AS SYNONYM FOR) externals.
    LogicalTypesSchemaVisitor v = parseScript(
        "REFERENCE TYPE Inner;"
        + "REFERENCE TYPE Ref1 AS SYNONYM FOR 'ext.Outer';"
        + "REFERENCE TYPE Ref2 AS SYNONYM FOR 'https://example.com/foo#/properties/bar';"
        + "ROW Holder ("
        + "  i Inner,"
        + "  one Ref1,"
        + "  two Ref2"
        + ");"
        + "TYPE Holder"
    );
    assertEquals(java.util.Arrays.asList("Inner", "Ref1", "Ref2"),
        v.getReferencedTypes());
    Map<String, String> imports = v.getExternalImports();
    assertEquals(2, imports.size(),
        "Inner has no AS SYNONYM FOR clause; Ref1 and Ref2 do");
    assertEquals("ext.Outer", imports.get("Ref1"));
    assertEquals("https://example.com/foo#/properties/bar", imports.get("Ref2"));

    // The bundled LT carries the bindings.
    LogicalType lt = v.toLogicalType();
    assertEquals(imports, lt.getExternalImports());
    assertTrue(lt.isExternal("Inner"));
    assertTrue(lt.isExternal("Ref1"));
  }

  @Test
  void testReferenceTypeFromEmptyStringRejected() {
    assertThrows(ValidationException.class, () -> parseScript(
        "REFERENCE TYPE Ref1 AS SYNONYM FOR '';"
        + "TYPE INT"
    ));
  }

  @Test
  void testDuplicateReferenceTypeBareRejected() {
    // Two bare REFERENCE TYPE declarations for the same name. The second
    // is redundant; the visitor rejects rather than silently coalescing.
    assertThrows(ValidationException.class, () -> parseScript(
        "REFERENCE TYPE Foo;"
        + "REFERENCE TYPE Foo;"
        + "TYPE INT"
    ));
  }

  @Test
  void testDuplicateReferenceTypeWithDifferentFromRejected() {
    // Two REFERENCE TYPEs with the same name but different AS SYNONYM FOR URIs. Last-
    // write-wins on externalImports would silently pick one URI, hiding the
    // user's mistake. Reject explicitly.
    assertThrows(ValidationException.class, () -> parseScript(
        "REFERENCE TYPE Foo AS SYNONYM FOR 'a';"
        + "REFERENCE TYPE Foo AS SYNONYM FOR 'b';"
        + "TYPE INT"
    ));
  }

  @Test
  void testDuplicateReferenceTypeMixedFromRejected() {
    // Bare declaration followed by AS SYNONYM FOR-clause declaration for the same
    // name. Ambiguous intent — reject.
    assertThrows(ValidationException.class, () -> parseScript(
        "REFERENCE TYPE Foo;"
        + "REFERENCE TYPE Foo AS SYNONYM FOR 'a';"
        + "TYPE INT"
    ));
  }

  @Test
  void testCreateTypeEnum() {
    LogicalTypesSchemaVisitor v = parseScript(
        "ENUM Color ('RED', 'GREEN', 'BLUE');"
        + "TYPE INT"
    );
    Schema color = v.getNamedTypes().get("Color");
    assertNotNull(color);
    assertEquals(Schema.Type.ENUM, color.getType());
    List<Schema.EnumValue> values = color.getEnumValues();
    assertEquals(3, values.size());
    assertEquals("RED", values.get(0).getSymbol());
    assertEquals("GREEN", values.get(1).getSymbol());
    assertEquals("BLUE", values.get(2).getSymbol());
  }

  @Test
  void testCreateTypeStructWithDoc() {
    LogicalTypesSchemaVisitor v = parseScript(
        "ROW Address (street STRING, city STRING)"
        + " COMMENT 'a postal address';"
        + "TYPE INT"
    );
    Schema address = v.getNamedTypes().get("Address");
    assertEquals("a postal address", address.getDoc());
  }

  @Test
  void testCreateTypeStructWithTags() {
    LogicalTypesSchemaVisitor v = parseScript(
        "ROW Person (name STRING)"
        + " TAGS('PII', 'SENSITIVE');"
        + "TYPE INT"
    );
    Schema person = v.getNamedTypes().get("Person");
    assertEquals(2, person.getTags().size());
    assertEquals("PII", person.getTags().get(0));
    assertEquals("SENSITIVE", person.getTags().get(1));
  }

  @Test
  void testCreateTypeStructWithParams() {
    LogicalTypesSchemaVisitor v = parseScript(
        "ROW Config (key STRING, value STRING)"
        + " WITH('version' = '2', 'format' = 'json');"
        + "TYPE INT"
    );
    Schema config = v.getNamedTypes().get("Config");
    assertEquals("2", config.getParams().get("version"));
    assertEquals("json", config.getParams().get("format"));
  }

  @Test
  void testCreateTypeEnumWithDoc() {
    LogicalTypesSchemaVisitor v = parseScript(
        "ENUM Status ('ACTIVE', 'INACTIVE')"
        + " COMMENT 'user status';"
        + "TYPE INT"
    );
    Schema status = v.getNamedTypes().get("Status");
    assertEquals("user status", status.getDoc());
  }

  // =========================================================================
  // ROW/ENUM declarations + trailing TYPE — named-type registration
  // =========================================================================

  @Test
  void testRegisterTypeNamedStruct() {
    LogicalTypesSchemaVisitor v = parseScript(
        "ROW Person (id BIGINT NOT NULL, name STRING);"
        + "TYPE Person"
    );
    // Named type registered.
    Schema person = v.getNamedTypes().get("Person");
    assertNotNull(person);
    assertEquals(Schema.Type.STRUCT, person.getType());
    assertEquals(2, person.getFields().size());
    // Root is a NAMED_TYPE_REF pointing at Person.
    Schema root = v.getRootSchema();
    assertEquals(Schema.Type.NAMED_TYPE_REF, root.getType());
    assertEquals("Person", root.getQualifiedName());
  }

  @Test
  void testRegisterTypeNamedEnum() {
    LogicalTypesSchemaVisitor v = parseScript(
        "ENUM Color ('RED', 'GREEN', 'BLUE');"
        + "TYPE Color"
    );
    Schema color = v.getNamedTypes().get("Color");
    assertNotNull(color);
    assertEquals(Schema.Type.ENUM, color.getType());
    assertEquals(3, color.getEnumValues().size());
    Schema root = v.getRootSchema();
    assertEquals(Schema.Type.NAMED_TYPE_REF, root.getType());
    assertEquals("Color", root.getQualifiedName());
  }

  @Test
  void testRegisterTypeNamedStructWithMetadata() {
    LogicalTypesSchemaVisitor v = parseScript(
        "ROW Person (name STRING)"
        + " COMMENT 'a person'"
        + " TAGS('PII')"
        + " WITH('owner' = 'team-x');"
        + "TYPE Person"
    );
    Schema person = v.getNamedTypes().get("Person");
    assertEquals("a person", person.getDoc());
    assertEquals(1, person.getTags().size());
    assertEquals("PII", person.getTags().get(0));
    assertEquals("team-x", person.getParams().get("owner"));
  }

  @Test
  void testRegisterTypeNamedEnumWithMetadata() {
    LogicalTypesSchemaVisitor v = parseScript(
        "ENUM Status ('ACTIVE', 'INACTIVE')"
        + " COMMENT 'user status'"
        + " WITH('version' = '1');"
        + "TYPE Status"
    );
    Schema status = v.getNamedTypes().get("Status");
    assertEquals("user status", status.getDoc());
    assertEquals("1", status.getParams().get("version"));
  }

  @Test
  void testRegisterTypeNamedStructInNamespace() {
    LogicalTypesSchemaVisitor v = parseScript(
        "NAMESPACE com.example;"
        + "ROW Person (name STRING);"
        + "TYPE Person"
    );
    // Name is qualified by the declared namespace.
    assertNotNull(v.getNamedTypes().get("com.example.Person"));
    assertEquals("com.example.Person", v.getRootSchema().getQualifiedName());
  }

  @Test
  void testCreateTypeDuplicate() {
    assertThrows(ValidationException.class, () -> {
        parseScript(
            "ROW Foo (x INT);"
            + "ROW Foo (y STRING);"
            + "TYPE Foo"
        );
    });
  }

  @Test
  void testCreateTypePrimitiveIsParseError() {
    // Named-type declarations are limited to ROW / ENUM. Naming a primitive
    // (e.g. trying to give INT an alias) has no grammar shape and is a parse
    // error.
    try {
      parseScript("ROW Foo INT; TYPE Foo");
      assertTrue(false, "Expected parse error for named primitive");
    } catch (RuntimeException expected) {
      // ok — ANTLR throws on parse failure
    }
  }

  @Test
  void testRegisterTypeNotNull() {
    LogicalTypesSchemaVisitor v = parseScript(
        "ROW Person (name STRING);"
        + "TYPE Person NOT NULL"
    );
    Schema root = v.getRootSchema();
    assertEquals(Schema.Type.NAMED_TYPE_REF, root.getType());
    assertEquals("Person", root.getQualifiedName());
    assertFalse(root.isNullable());
    // Named type body is registered (with default nullability for its kind).
    assertNotNull(v.getNamedTypes().get("Person"));
  }

  @Test
  void testRegisterTypeNamedEnumNotNullWithMetadata() {
    LogicalTypesSchemaVisitor v = parseScript(
        "ENUM Color ('R', 'G', 'B') COMMENT 'rgb colors';"
        + "TYPE Color NOT NULL"
    );
    Schema root = v.getRootSchema();
    assertEquals(Schema.Type.NAMED_TYPE_REF, root.getType());
    assertFalse(root.isNullable());
    assertEquals("rgb colors", v.getNamedTypes().get("Color").getDoc());
  }

  @Test
  void testRegisterTypeDefaultNullable() {
    // Explicit TYPE without nullability marker → root nullable by default.
    LogicalTypesSchemaVisitor v = parseScript(
        "ROW Person (name STRING);"
        + "TYPE Person"
    );
    assertTrue(v.getRootSchema().isNullable());
  }

  @Test
  void testRegisterTypeAnonymousRow() {
    LogicalTypesSchemaVisitor v = parseScript(
        "TYPE ROW(id BIGINT NOT NULL, name STRING)"
    );
    Schema root = v.getRootSchema();
    assertNotNull(root);
    assertEquals(Schema.Type.STRUCT, root.getType());
    assertEquals(2, root.getFields().size());
    assertFalse(root.getField("id").getSchema().isNullable());
    assertTrue(root.getField("name").getSchema().isNullable());
  }

  @Test
  void testNamedTypeRefInStruct() {
    LogicalTypesSchemaVisitor v = parseScript(
        "ROW Address (street STRING, city STRING);"
        + "TYPE ROW(name STRING, addr Address)"
    );
    Schema root = v.getRootSchema();
    Schema.Field addrField = root.getField("addr");
    assertNotNull(addrField);
    assertEquals(Schema.Type.NAMED_TYPE_REF, addrField.getSchema().getType());
    assertEquals("Address", addrField.getSchema().getQualifiedName());
  }

  @Test
  void testFullScript() {
    String script =
        "NAMESPACE com.example;"
        + "ENUM Status ("
        + "  'ACTIVE' COMMENT 'currently active',"
        + "  'INACTIVE',"
        + "  'PENDING'"
        + ");"
        + "ROW UserProfile ("
        + "  id BIGINT NOT NULL,"
        + "  name VARCHAR(100) COMMENT 'full name',"
        + "  email STRING TAGS('PII'),"
        + "  status Status,"
        + "  scores ARRAY<DOUBLE>,"
        + "  metadata MAP<STRING, STRING>,"
        + "  created_at TIMESTAMP(3)"
        + ");"
        + "TYPE UserProfile";

    LogicalTypesSchemaVisitor v = parseScript(script);

    assertEquals("com.example", v.getNamespace());
    assertEquals(2, v.getNamedTypes().size());

    // Named-type declarations get the namespace prefix
    Schema status = v.getNamedTypes().get("com.example.Status");
    assertNotNull(status);
    assertEquals(Schema.Type.ENUM, status.getType());
    assertEquals(3, status.getEnumValues().size());
    assertEquals("currently active", status.getEnumValues().get(0).getDoc());

    Schema profile = v.getNamedTypes().get("com.example.UserProfile");
    assertNotNull(profile);
    assertEquals(Schema.Type.STRUCT, profile.getType());
    assertEquals(7, profile.getFields().size());

    Schema.Field idField = profile.getField("id");
    assertEquals(Schema.Type.BIGINT, idField.getSchema().getType());
    assertFalse(idField.getSchema().isNullable());

    Schema.Field nameField = profile.getField("name");
    assertEquals(Schema.Type.VARCHAR, nameField.getSchema().getType());
    assertEquals(100, nameField.getSchema().getLength());
    assertEquals("full name", nameField.getDoc());

    Schema.Field emailField = profile.getField("email");
    assertEquals(1, emailField.getTags().size());
    assertEquals("PII", emailField.getTags().get(0));

    Schema.Field statusField = profile.getField("status");
    assertEquals(Schema.Type.NAMED_TYPE_REF, statusField.getSchema().getType());
    assertEquals("com.example.Status", statusField.getSchema().getQualifiedName());

    Schema.Field scoresField = profile.getField("scores");
    assertEquals(Schema.Type.ARRAY, scoresField.getSchema().getType());
    assertEquals(Schema.Type.DOUBLE, scoresField.getSchema().getElementType().getType());

    Schema.Field metaField = profile.getField("metadata");
    assertEquals(Schema.Type.MAP, metaField.getSchema().getType());

    Schema.Field createdField = profile.getField("created_at");
    assertEquals(Schema.Type.TIMESTAMP, createdField.getSchema().getType());
    assertEquals(3, createdField.getSchema().getPrecision());

    // Root schema reference also gets the namespace prefix
    Schema root = v.getRootSchema();
    assertNotNull(root);
    assertEquals(Schema.Type.NAMED_TYPE_REF, root.getType());
    assertEquals("com.example.UserProfile", root.getQualifiedName());
  }

  // =========================================================================
  // NAMESPACE qualification rules
  // =========================================================================

  @Test
  void testNamespaceQualifiesCreateType() {
    LogicalTypesSchemaVisitor v = parseScript(
        "NAMESPACE a.b;"
        + "ROW Foo (x INT);"
        + "TYPE Foo");
    assertNotNull(v.getNamedTypes().get("a.b.Foo"));
  }

  @Test
  void testNamespaceDoesNotDoubleQualify() {
    // Idempotent: a name already qualified with the same namespace stays as-is.
    LogicalTypesSchemaVisitor v = parseScript(
        "NAMESPACE a.b;"
        + "ROW a.b.Foo (x INT);"
        + "TYPE a.b.Foo");
    assertNotNull(v.getNamedTypes().get("a.b.Foo"));
    assertEquals(1, v.getNamedTypes().size());
  }

  @Test
  void testNamespaceDoesNotOverrideQualified() {
    // A different qualified name overrides the namespace.
    LogicalTypesSchemaVisitor v = parseScript(
        "NAMESPACE a.b;"
        + "ROW x.y.Foo (n INT);"
        + "TYPE x.y.Foo");
    assertNotNull(v.getNamedTypes().get("x.y.Foo"));
  }

  @Test
  void testNamespaceDoesNotQualifyReferenceType() {
    LogicalTypesSchemaVisitor v = parseScript(
        "NAMESPACE a.b;"
        + "REFERENCE TYPE Money;"
        + "ROW Holder (amount Money);"
        + "TYPE Holder");
    // REFERENCE TYPE name is captured as-written
    assertTrue(v.getReferencedTypes().contains("Money"));
    assertFalse(v.getReferencedTypes().contains("a.b.Money"));
  }

  @Test
  void testNamespaceQualifiesUnqualifiedFieldRef() {
    // A bare field-type reference with no matching REFERENCE TYPE → namespace applies.
    LogicalTypesSchemaVisitor v = parseScript(
        "NAMESPACE a.b;"
        + "ROW Inner (x INT);"
        + "ROW Outer (inner Inner);"
        + "TYPE Outer");
    Schema outer = v.getNamedTypes().get("a.b.Outer");
    Schema.Field innerField = outer.getField("inner");
    assertEquals(Schema.Type.NAMED_TYPE_REF, innerField.getSchema().getType());
    assertEquals("a.b.Inner", innerField.getSchema().getQualifiedName());
  }

  @Test
  void testReferencedNameWinsOverNamespacePrefix() {
    // A bare field-type reference matching a REFERENCE TYPE entry → use as-is.
    LogicalTypesSchemaVisitor v = parseScript(
        "NAMESPACE a.b;"
        + "REFERENCE TYPE Money;"
        + "ROW Holder (amount Money);"
        + "TYPE Holder");
    Schema holder = v.getNamedTypes().get("a.b.Holder");
    Schema.Field amount = holder.getField("amount");
    assertEquals(Schema.Type.NAMED_TYPE_REF, amount.getSchema().getType());
    assertEquals("Money", amount.getSchema().getQualifiedName());
  }

  @Test
  void testQualifiedFieldRefIsAlwaysUnchanged() {
    LogicalTypesSchemaVisitor v = parseScript(
        "NAMESPACE a.b;"
        + "ROW Holder (cur x.y.Currency);"
        + "TYPE Holder");
    Schema holder = v.getNamedTypes().get("a.b.Holder");
    assertEquals("x.y.Currency",
        holder.getField("cur").getSchema().getQualifiedName());
  }

  @Test
  void testNoNamespaceLeavesNamesUnchanged() {
    LogicalTypesSchemaVisitor v = parseScript(
        "REFERENCE TYPE Money;"
        + "ROW Holder (amount Money, name STRING);"
        + "TYPE Holder");
    assertNotNull(v.getNamedTypes().get("Holder"));
    assertEquals("Money",
        v.getNamedTypes().get("Holder").getField("amount").getSchema().getQualifiedName());
  }

  @Test
  void testVisitorToLogicalTypePopulatesNamespace() {
    LogicalTypesSchemaVisitor v = parseScript(
        "NAMESPACE com.example;"
        + "TYPE INT");
    LogicalType lt = v.toLogicalType();
    assertEquals("com.example", lt.getNamespace());
  }

  @Test
  void testNamespaceCollisionImportsWin() {
    // If a ROW/ENUM declaration name and a REFERENCE TYPE name collide on the bare
    // identifier, unqualified field references resolve to the import. The
    // local definition is still accessible via its fully-qualified name.
    LogicalTypesSchemaVisitor v = parseScript(
        "NAMESPACE a.b;"
        + "REFERENCE TYPE Foo;"
        + "ROW Foo (x INT);"
        + "ROW Holder ("
        + "  importedFoo Foo,"
        + "  localFoo a.b.Foo"
        + ");"
        + "TYPE Holder");
    Schema holder = v.getNamedTypes().get("a.b.Holder");
    assertEquals("Foo", holder.getField("importedFoo").getSchema().getQualifiedName(), "imports win for unqualified field refs");
    assertEquals("a.b.Foo",
        holder.getField("localFoo").getSchema().getQualifiedName());
    // Local definition is registered under its qualified name
    assertNotNull(v.getNamedTypes().get("a.b.Foo"));
  }

  // =========================================================================
  // Equality
  // =========================================================================

  @Test
  void testPrimitiveEquality() {
    assertEquals(parseTypeExpr("INT"), parseTypeExpr("INTEGER"));
    assertEquals(parseTypeExpr("DECIMAL(10, 2)"), parseTypeExpr("DEC(10, 2)"));
    assertEquals(parseTypeExpr("DECIMAL(10, 2)"), parseTypeExpr("NUMERIC(10, 2)"));
  }

  @Test
  void testArrayEquality() {
    assertEquals(parseTypeExpr("ARRAY<INT>"), parseTypeExpr("INT ARRAY"));
  }

  // =========================================================================
  // toDdl
  // =========================================================================

  @Test
  void testToDdlPrimitives() {
    assertEquals("INT", parseTypeExpr("INT").toDdl());
    assertEquals("INT", parseTypeExpr("INTEGER").toDdl());
    assertEquals("DECIMAL(10, 2)", parseTypeExpr("DECIMAL(10, 2)").toDdl());
    assertEquals("VARCHAR(255)", parseTypeExpr("VARCHAR(255)").toDdl());
    assertEquals("TIMESTAMP(6)", parseTypeExpr("TIMESTAMP(6)").toDdl());
    assertEquals("TIMESTAMP_LTZ(3)",
        parseTypeExpr("TIMESTAMP(3) WITH LOCAL TIME ZONE").toDdl());
  }

  @Test
  void testToDdlArray() {
    assertEquals("ARRAY<INT>", parseTypeExpr("ARRAY<INT>").toDdl());
    assertEquals("ARRAY<INT>", parseTypeExpr("INT ARRAY").toDdl());
  }

  @Test
  void testToDdlMap() {
    assertEquals("MAP<STRING, INT>", parseTypeExpr("MAP<STRING, INT>").toDdl());
  }

  // =========================================================================
  // Typed default literal coercion
  // =========================================================================

  private Schema.Field firstField(String typeAndDefault) {
    Schema s = parseTypeExpr("ROW(f " + typeAndDefault + ")");
    return s.getFields().get(0);
  }

  @Test
  void testDefaultBoolean() {
    assertEquals(Boolean.TRUE, firstField("BOOLEAN DEFAULT TRUE").getDefaultValue());
    assertEquals(Boolean.FALSE, firstField("BOOLEAN DEFAULT FALSE").getDefaultValue());
  }

  @Test
  void testDefaultTinyint() {
    assertEquals((byte) 1, firstField("TINYINT DEFAULT 1").getDefaultValue());
    assertEquals((byte) -128, firstField("TINYINT DEFAULT -128").getDefaultValue());
  }

  @Test
  void testDefaultTinyintOverflow() {
    assertThrows(ValidationException.class, () -> {
        firstField("TINYINT DEFAULT 200");
    });
  }

  @Test
  void testDefaultSmallint() {
    assertEquals((short) 30000, firstField("SMALLINT DEFAULT 30000").getDefaultValue());
  }

  @Test
  void testDefaultSmallintOverflow() {
    assertThrows(ValidationException.class, () -> {
        firstField("SMALLINT DEFAULT 99999");
    });
  }

  @Test
  void testDefaultInt() {
    assertEquals(42, firstField("INT DEFAULT 42").getDefaultValue());
  }

  @Test
  void testDefaultBigint() {
    assertEquals(9999999999L, firstField("BIGINT DEFAULT 9999999999").getDefaultValue());
  }

  @Test
  void testDefaultFloatFromFloatLiteral() {
    assertEquals(1.5f, firstField("FLOAT DEFAULT 1.5").getDefaultValue());
  }

  @Test
  void testDefaultFloatFromIntLiteral() {
    assertEquals(5.0f, firstField("FLOAT DEFAULT 5").getDefaultValue());
  }

  @Test
  void testDefaultDoubleFromIntLiteral() {
    assertEquals(5.0, firstField("DOUBLE DEFAULT 5").getDefaultValue());
  }

  @Test
  void testDefaultString() {
    assertEquals("Anonymous",
        firstField("STRING DEFAULT 'Anonymous'").getDefaultValue());
  }

  @Test
  void testDefaultDecimalFromString() {
    assertEquals(new java.math.BigDecimal("12.34567890123456789"),
        firstField("DECIMAL DEFAULT '12.34567890123456789'").getDefaultValue());
  }

  @Test
  void testDefaultDecimalFromNumeric() {
    assertEquals(new java.math.BigDecimal("12.34"),
        firstField("DECIMAL DEFAULT 12.34").getDefaultValue());
  }

  @Test
  void testDefaultDate() {
    assertEquals(java.time.LocalDate.of(2026, 4, 17),
        firstField("DATE DEFAULT '2026-04-17'").getDefaultValue());
  }

  @Test
  void testDefaultTime() {
    assertEquals(java.time.LocalTime.of(12, 30, 45),
        firstField("TIME DEFAULT '12:30:45'").getDefaultValue());
  }

  @Test
  void testDefaultTimestamp() {
    assertEquals(java.time.LocalDateTime.of(2026, 4, 17, 18, 0, 0),
        firstField("TIMESTAMP DEFAULT '2026-04-17T18:00:00'").getDefaultValue());
  }

  @Test
  void testDefaultTimestampLtz() {
    assertEquals(java.time.Instant.parse("2026-04-17T18:00:00Z"),
        firstField("TIMESTAMP_LTZ DEFAULT '2026-04-17T18:00:00Z'").getDefaultValue());
  }

  @Test
  void testStringLiteralOnBinaryFieldThrows() {
    assertThrows(ValidationException.class, () -> {
        // String literals are no longer accepted for BINARY/VARBINARY defaults —
        // even one whose contents look like hex.
        firstField("BINARY DEFAULT 'hello'");
    });
  }

  @Test
  void testBytesLiteralUppercase() {
    byte[] expected = {0x48, 0x65, 0x6C, 0x6C, 0x6F};
    assertArrayEquals(expected,
        (byte[]) firstField("BYTES DEFAULT X'48656C6C6F'").getDefaultValue());
  }

  @Test
  void testBytesLiteralLowercase() {
    byte[] expected = {0x48, 0x65, 0x6c, 0x6c, 0x6f};
    assertArrayEquals(expected,
        (byte[]) firstField("BYTES DEFAULT x'48656c6c6f'").getDefaultValue());
  }

  @Test
  void testBytesLiteralEmpty() {
    assertArrayEquals(new byte[0],
        (byte[]) firstField("BYTES DEFAULT X''").getDefaultValue());
  }

  @Test
  void testBytesLiteralOnVarbinary() {
    byte[] expected = {0x01, 0x02, 0x03, 0x04};
    assertArrayEquals(expected,
        (byte[]) firstField("VARBINARY(50) DEFAULT X'01020304'").getDefaultValue());
  }

  @Test
  void testBytesLiteralOnBinary() {
    byte[] expected = {0x01, 0x02, 0x03, 0x04};
    assertArrayEquals(expected,
        (byte[]) firstField("BINARY(4) DEFAULT X'01020304'").getDefaultValue());
  }

  @Test
  void testBytesLiteralOddLengthThrows() {
    assertThrows(ValidationException.class, () -> {
        firstField("BYTES DEFAULT X'123'");
    });
  }

  @Test
  void testBytesLiteralInvalidHexThrows() {
    assertThrows(ValidationException.class, () -> {
        firstField("BYTES DEFAULT X'48G5'");
    });
  }

  @Test
  void testBytesLiteralOnIntFieldThrows() {
    assertThrows(ValidationException.class, () -> {
        firstField("INT DEFAULT X'01'");
    });
  }

  @Test
  void testDefaultIntFromString() {
    assertThrows(ValidationException.class, () -> {
        firstField("INT DEFAULT 'foo'");
    });
  }

  @Test
  void testDefaultStringFromInt() {
    assertThrows(ValidationException.class, () -> {
        firstField("STRING DEFAULT 5");
    });
  }

  @Test
  void testDefaultArrayNonNull() {
    assertThrows(ValidationException.class, () -> {
        firstField("ARRAY<INT> DEFAULT 5");
    });
  }

  @Test
  void testDefaultIntNull() {
    Schema.Field f = firstField("INT DEFAULT NULL");
    assertTrue(f.hasDefaultValue());
    assertEquals(null, f.getDefaultValue());
  }

  @Test
  void testDefaultArrayNull() {
    Schema.Field f = firstField("ARRAY<INT> DEFAULT NULL");
    assertTrue(f.hasDefaultValue());
    assertEquals(null, f.getDefaultValue());
  }

  // =========================================================================
  // Non-reserved keywords usable as identifiers
  // =========================================================================

  /**
   * The grammar's nonReservedKeyword rule permits these 9 keywords to be used
   * as field names without backtick quoting: ENUM, MAP, NAMESPACE, REFERENCE,
   * SCHEMA, TAGS, TYPE, VARIANT, ZONE. This test exercises each in a struct
   * body to ensure the demotion holds and the parser correctly disambiguates
   * keyword-as-identifier from keyword-as-grammar-token.
   */
  @Test
  void testNonReservedKeywordsAsFieldNames() {
    Schema s = parseTypeExpr(
        "ROW("
            + "enum INT, "
            + "map STRING, "
            + "namespace STRING, "
            + "reference STRING, "
            + "schema STRING, "
            + "tags STRING, "
            + "type STRING, "
            + "variant STRING, "
            + "zone STRING"
            + ")");
    assertEquals(Schema.Type.STRUCT, s.getType());
    assertEquals(9, s.getFields().size());
    assertNotNull(s.getField("enum"));
    assertNotNull(s.getField("map"));
    assertNotNull(s.getField("namespace"));
    assertNotNull(s.getField("reference"));
    assertNotNull(s.getField("schema"));
    assertNotNull(s.getField("tags"));
    assertNotNull(s.getField("type"));
    assertNotNull(s.getField("variant"));
    assertNotNull(s.getField("zone"));
    assertEquals(Schema.Type.INT, s.getField("enum").getSchema().getType());
    assertEquals(Schema.Type.VARCHAR, s.getField("schema").getSchema().getType());
  }

  // =========================================================================
  // Located validation errors — position information for tooling (e.g. UI markers)
  // =========================================================================

  /**
   * Validation errors produced by the visitor must carry the source position of
   * the offending input. The playground UI uses these positions to draw squigglies;
   * generic {@code (1, 0)}-positioned errors would render at the top of the document
   * instead of where the user actually made the mistake.
   */
  @Test
  void testValidationErrorCarriesPosition_outOfRangeTinyInt() {
    String sql = "TYPE ROW(\n"
        + "  bad TINYINT NOT NULL DEFAULT 999\n"
        + ")";
    LocatedValidationException e = assertThrows(
        LocatedValidationException.class, () -> parseScript(sql));
    // Line 2 (the field def), column points into the literal.
    assertEquals(2, e.getLine(), "should point at the field's line");
    // The default literal "999" sits after "DEFAULT " on line 2.
    int defaultStartCol = "  bad TINYINT NOT NULL DEFAULT ".length();
    assertEquals(defaultStartCol, e.getColumn(),
        "column should land on the start of the literal");
    assertEquals(defaultStartCol + "999".length(), e.getEndColumn(),
        "endColumn should bound the literal");
  }

  /** A duplicate-named-type error should point at the duplicated name token. */
  @Test
  void testValidationErrorCarriesPosition_duplicateNamedType() {
    String sql = "ROW Foo (a INT);\n"
        + "ROW Foo (b STRING);\n"
        + "TYPE Foo";
    LocatedValidationException e = assertThrows(
        LocatedValidationException.class, () -> parseScript(sql));
    assertEquals(2, e.getLine(), "should point at the duplicating declaration");
    int fooStartCol = "ROW ".length();
    assertEquals(fooStartCol, e.getColumn(),
        "column should land on the duplicated name");
    assertEquals(fooStartCol + "Foo".length(), e.getEndColumn());
  }

  // =========================================================================
  // Sugar: ROW/ENUM declarations auto-register the unique root as ... NOT NULL
  // =========================================================================

  @Test
  void testSugarSingleTypeRegistersAsNotNull() {
    LogicalTypesSchemaVisitor v = parseScript(
        "ROW Foo (x INT)"
    );
    Schema root = v.getRootSchema();
    assertEquals(Schema.Type.NAMED_TYPE_REF, root.getType());
    assertEquals("Foo", root.getQualifiedName());
    assertFalse(root.isNullable(), "sugar root must be NOT NULL");
  }

  @Test
  void testSugarVsExplicitDifferInRootNullability() {
    // The sugar bakes in NOT NULL. Manually expanding to "TYPE Foo"
    // (no marker) gives a nullable root. They are intentionally different.
    LogicalTypesSchemaVisitor sugar = parseScript("ROW Foo (x INT)");
    LogicalTypesSchemaVisitor explicit = parseScript(
        "ROW Foo (x INT); TYPE Foo");
    assertFalse(sugar.getRootSchema().isNullable());
    assertTrue(explicit.getRootSchema().isNullable());
  }

  @Test
  void testSugarMultiTypeUniqueRootInferred() {
    // Address is referenced by User; only User is unreferenced → User is the root.
    LogicalTypesSchemaVisitor v = parseScript(
        "ROW Address (street STRING, city STRING);"
        + "ROW User (name STRING, addr Address)"
    );
    Schema root = v.getRootSchema();
    assertEquals(Schema.Type.NAMED_TYPE_REF, root.getType());
    assertEquals("User", root.getQualifiedName());
    assertFalse(root.isNullable());
  }

  @Test
  void testSugarReferenceTypeIsNotADefinedType() {
    // REFERENCE TYPE Ext is external; Foo is the unique defined root.
    LogicalTypesSchemaVisitor v = parseScript(
        "REFERENCE TYPE Ext;"
        + "ROW Foo (x Ext)"
    );
    Schema root = v.getRootSchema();
    assertEquals("Foo", root.getQualifiedName());
  }

  @Test
  void testSugarMultipleRootsInferredAsUnion() {
    // Two unreferenced top-level types and no explicit trailing TYPE → sugar
    // infers a non-nullable UNION of NAMED_TYPE_REFs at the root. Avro/JSON
    // emit this as a tagged union; the proto writer detects the shape and
    // treats it as a multi-message file.
    LogicalTypesSchemaVisitor v = parseScript(
        "ROW Foo (x INT);"
        + "ROW Bar (y INT)");
    Schema root = v.getRootSchema();
    assertEquals(Schema.Type.UNION, root.getType());
    assertFalse(root.isNullable(), "sugar root must be NOT NULL");
    assertEquals(2, root.getBranches().size());
    assertEquals("Foo", root.getBranches().get(0).getName());
    assertEquals(Schema.Type.NAMED_TYPE_REF,
        root.getBranches().get(0).getSchema().getType());
    assertEquals("Foo",
        root.getBranches().get(0).getSchema().getQualifiedName());
    assertEquals("Bar", root.getBranches().get(1).getName());
    assertEquals("Bar",
        root.getBranches().get(1).getSchema().getQualifiedName());
  }

  @Test
  void testSugarMultipleRootsInNamespace() {
    // Multi-root with default namespace — branch names use the simple name,
    // member NAMED_TYPE_REFs carry the qualified key.
    LogicalTypesSchemaVisitor v = parseScript(
        "NAMESPACE com.example;"
        + "ROW Foo (x INT);"
        + "ROW Bar (y INT)");
    Schema root = v.getRootSchema();
    assertEquals(Schema.Type.UNION, root.getType());
    assertEquals("Foo", root.getBranches().get(0).getName());
    assertEquals("com.example.Foo",
        root.getBranches().get(0).getSchema().getQualifiedName());
    assertEquals("Bar", root.getBranches().get(1).getName());
    assertEquals("com.example.Bar",
        root.getBranches().get(1).getSchema().getQualifiedName());
  }

  @Test
  void testSugarCycleErrors() {
    LocatedValidationException e = assertThrows(
        LocatedValidationException.class,
        () -> parseScript(
            "ROW A (b B);"
            + "ROW B (a A)"));
    assertTrue(e.getMessage().contains("dependency cycle"),
        () -> "expected cycle message, got: " + e.getMessage());
  }

  @Test
  void testSugarNoTypesErrors() {
    LocatedValidationException e = assertThrows(
        LocatedValidationException.class,
        () -> parseScript("NAMESPACE com.example;"));
    assertTrue(e.getMessage().contains("No type to register"),
        () -> "expected 'no type to register' message, got: " + e.getMessage());
  }

  @Test
  void testExplicitRegisterOverridesAmbiguity() {
    // Two unrelated types — would be a multi-root error under sugar.
    // Explicit TYPE disambiguates without complaint.
    LogicalTypesSchemaVisitor v = parseScript(
        "ROW Foo (x INT);"
        + "ROW Bar (y INT);"
        + "TYPE Bar"
    );
    assertEquals("Bar", v.getRootSchema().getQualifiedName());
  }

  @Test
  void testRegisterTypeAnonymousPrimitive() {
    LogicalTypesSchemaVisitor v = parseScript("TYPE INT");
    Schema root = v.getRootSchema();
    assertEquals(Schema.Type.INT, root.getType());
    assertTrue(root.isNullable(), "explicit trailing TYPE without marker is nullable by default");
  }

  @Test
  void testRegisterTypeAnonymousPrimitiveNotNull() {
    LogicalTypesSchemaVisitor v = parseScript("TYPE INT NOT NULL");
    assertFalse(v.getRootSchema().isNullable());
  }

  @Test
  void testRegisterTypeWithExplicitNullKeepsNullable() {
    // TYPE Foo NULL → nullable, even though sugar would have forced NOT NULL.
    LogicalTypesSchemaVisitor v = parseScript(
        "ROW Foo (x INT);"
        + "TYPE Foo NULL"
    );
    assertTrue(v.getRootSchema().isNullable());
  }

  @Test
  void testOrphanNamedTypes() {
    // Foo is reachable from Bar (the registered root). Baz is orphan.
    LogicalTypesSchemaVisitor v = parseScript(
        "ROW Foo (x INT);"
        + "ROW Baz (y INT);"
        + "ROW Bar (foo Foo);"
        + "TYPE Bar"
    );
    java.util.Set<String> orphans = v.toLogicalType().findOrphanNamedTypes();
    assertEquals(java.util.Set.of("Baz"), orphans);
  }

  @Test
  void testNoOrphansWhenAllReachable() {
    LogicalTypesSchemaVisitor v = parseScript(
        "ROW Foo (x INT);"
        + "ROW Bar (foo Foo)"
    );
    assertTrue(v.toLogicalType().findOrphanNamedTypes().isEmpty());
  }

  // =========================================================================
  // Dotted-name nesting convention
  //
  // A dot in a declaration name MAY denote nesting (under a previously-
  // declared local type), or namespace qualification (when no parent prefix
  // matches). Resolution is per-statement, declaration-order-sensitive.
  // =========================================================================

  @Test
  void testNestedTypeUnderDefaultNamespace() {
    // Outer is declared first → stored as com.example.Outer.
    // Outer.Inner has prefix Outer (matches com.example.Outer after default-ns
    // expansion) → nested → stored as com.example.Outer.Inner.
    LogicalTypesSchemaVisitor v = parseScript(
        "NAMESPACE com.example;"
        + "ROW Outer (x INT);"
        + "ROW Outer.Inner (y STRING);"
        + "TYPE Outer"
    );
    assertNotNull(v.getNamedTypes().get("com.example.Outer"));
    assertNotNull(v.getNamedTypes().get("com.example.Outer.Inner"));
    assertNull(v.getNamedTypes().get("Outer.Inner"));
  }

  @Test
  void testDottedNameWithoutMatchingPrefixIsForeignNamespace() {
    // Other.Bar — no Other defined locally → treated as namespace-qualified
    // (top-level type in namespace Other), stored verbatim.
    LogicalTypesSchemaVisitor v = parseScript(
        "NAMESPACE com.example;"
        + "ROW Other.Bar (x INT);"
        + "TYPE Other.Bar"
    );
    assertNotNull(v.getNamedTypes().get("Other.Bar"));
    assertNull(v.getNamedTypes().get("com.example.Other.Bar"));
  }

  @Test
  void testDeeplyNestedTypes() {
    LogicalTypesSchemaVisitor v = parseScript(
        "NAMESPACE com.example;"
        + "ROW Outer (x INT);"
        + "ROW Outer.Mid (y INT);"
        + "ROW Outer.Mid.Inner (z INT);"
        + "TYPE Outer"
    );
    assertNotNull(v.getNamedTypes().get("com.example.Outer"));
    assertNotNull(v.getNamedTypes().get("com.example.Outer.Mid"));
    assertNotNull(v.getNamedTypes().get("com.example.Outer.Mid.Inner"));
  }

  @Test
  void testGapInNestingChainRejected() {
    // Outer exists, Outer.Mid does not, Outer.Mid.Inner would be nested two
    // levels under Outer with a missing intermediate parent.
    LocatedValidationException e = assertThrows(
        LocatedValidationException.class,
        () -> parseScript(
            "NAMESPACE com.example;"
            + "ROW Outer (x INT);"
            + "ROW Outer.Mid.Inner (z INT);"
            + "TYPE Outer"));
    assertTrue(e.getMessage().contains("missing"),
        () -> "expected missing-intermediate message, got: " + e.getMessage());
    assertTrue(e.getMessage().contains("Outer.Mid"));
  }

  @Test
  void testNestedTypeAsExplicitRootRejected() {
    LocatedValidationException e = assertThrows(
        LocatedValidationException.class,
        () -> parseScript(
            "ROW Outer (x INT);"
            + "ROW Outer.Inner (y INT);"
            + "TYPE Outer.Inner"));
    assertTrue(e.getMessage().contains("Cannot register nested type"),
        () -> "expected nested-root rejection, got: " + e.getMessage());
    assertTrue(e.getMessage().contains("Outer"),
        () -> "expected suggested top-level Outer, got: " + e.getMessage());
  }

  @Test
  void testSugarSkipsNestedTypesWhenInferringRoot() {
    // Outer is referenced by neither — without nesting awareness Outer.Inner
    // would also count as a "root candidate" and we'd hit a multi-root error.
    // With nesting awareness, only Outer is a candidate → unique root.
    LogicalTypesSchemaVisitor v = parseScript(
        "ROW Outer (x INT);"
        + "ROW Outer.Inner (y INT)"
    );
    assertEquals("Outer", v.getRootSchema().getQualifiedName());
  }

  @Test
  void testFieldRefToNestedType() {
    // A field type written as `Outer.Inner` should resolve to the qualified
    // nested key `com.example.Outer.Inner`.
    LogicalTypesSchemaVisitor v = parseScript(
        "NAMESPACE com.example;"
        + "ROW Outer (x INT);"
        + "ROW Outer.Inner (y INT);"
        + "ROW Holder (inner Outer.Inner);"
        + "TYPE Holder"
    );
    Schema holder = v.getNamedTypes().get("com.example.Holder");
    Schema.Field innerField = holder.getField("inner");
    assertEquals(Schema.Type.NAMED_TYPE_REF, innerField.getSchema().getType());
    assertEquals("com.example.Outer.Inner",
        innerField.getSchema().getQualifiedName());
  }

  @Test
  void testFieldRefWithForeignNamespace() {
    // No local type called Other; field type Other.Bar resolves to a
    // namespace-qualified ref (left as-written).
    LogicalTypesSchemaVisitor v = parseScript(
        "NAMESPACE com.example;"
        + "ROW Other.Bar (x INT);"
        + "ROW Holder (b Other.Bar);"
        + "TYPE Holder"
    );
    Schema holder = v.getNamedTypes().get("com.example.Holder");
    assertEquals("Other.Bar", holder.getField("b").getSchema().getQualifiedName());
  }

  @Test
  void testReferenceTypeIgnoresNestingRule() {
    // External REFERENCE TYPE names may have dots but are NOT subject to the
    // local nesting rule — they're stored verbatim and used verbatim.
    LogicalTypesSchemaVisitor v = parseScript(
        "NAMESPACE com.example;"
        + "REFERENCE TYPE com.other.Outer.Inner;"
        + "ROW Holder (inner com.other.Outer.Inner);"
        + "TYPE Holder"
    );
    Schema holder = v.getNamedTypes().get("com.example.Holder");
    assertEquals("com.other.Outer.Inner",
        holder.getField("inner").getSchema().getQualifiedName());
  }

  @Test
  void testFullyQualifiedNestedDeclarationName() {
    // User writes the full path explicitly. Should match Outer's full name
    // exactly (no double-namespacing).
    LogicalTypesSchemaVisitor v = parseScript(
        "NAMESPACE com.example;"
        + "ROW com.example.Outer (x INT);"
        + "ROW com.example.Outer.Inner (y INT);"
        + "TYPE com.example.Outer"
    );
    assertNotNull(v.getNamedTypes().get("com.example.Outer"));
    assertNotNull(v.getNamedTypes().get("com.example.Outer.Inner"));
  }

}
