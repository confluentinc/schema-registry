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

package io.confluent.kafka.schemaregistry.type.logical.constraint;

import io.confluent.kafka.schemaregistry.type.logical.Schema;
import org.junit.jupiter.api.Test;

import java.util.Arrays;

import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * Smoke tests for {@link CelValidator#assertValidStrict} — the strict
 * cel-java post-emit type-checker we use as the safety net in
 * {@link ConstraintToCelTranslator#translate}. These tests pin the contract
 * of which CEL constructs strict mode accepts and rejects, so a future
 * change to {@link ConstraintCelChecker#commonDeclarations} or to
 * {@link ConstraintTypeProvider} can't silently widen or narrow coverage.
 *
 * <p>Coverage groups:
 * <ul>
 *   <li>Type mapping: each Schema type -&gt; CEL type via celTypeFor</li>
 *   <li>Comparison/arithmetic overload coverage</li>
 *   <li>Macro receivers (collection vs scalar)</li>
 *   <li>commonDeclarations() functions: format validators + string ext</li>
 *   <li>has() argument shape</li>
 *   <li>Bracket access on synthetic root type</li>
 *   <li>Column-level vs table-level `this`</li>
 *   <li>Nested struct field access</li>
 *   <li>Cross-engine concerns we restored validators for</li>
 * </ul>
 */
class StrictCelCheckSmokeTest {

  // -------------------------------------------------------------------
  // Fixtures
  // -------------------------------------------------------------------

  private Schema fixture() {
    return Schema.createStruct(Arrays.asList(
        new Schema.Field("x", Schema.create(Schema.Type.INT), 0),
        new Schema.Field("name", Schema.createString(), 1),
        new Schema.Field("ratio", Schema.create(Schema.Type.DOUBLE), 2),
        new Schema.Field("blob", Schema.createBytes(), 3),
        new Schema.Field("active", Schema.create(Schema.Type.BOOLEAN), 4),
        new Schema.Field("tags",
            Schema.createArray(Schema.createString()), 5),
        new Schema.Field("ts",
            Schema.createTimestampLtz(6), 6),
        new Schema.Field("addr",
            Schema.createStruct(Arrays.asList(
                new Schema.Field("zip",
                    Schema.create(Schema.Type.INT), 0))), 7),
        new Schema.Field("scores",
            Schema.createMap(Schema.createString(),
                Schema.create(Schema.Type.INT)), 8),
        new Schema.Field("ages",
            Schema.createArray(Schema.create(Schema.Type.INT)), 9)));
  }

  // -------------------------------------------------------------------
  // Type mapping (Schema -> CEL primitive)
  // -------------------------------------------------------------------

  @Test void intFieldTypesAsInt() {
    CelValidator.assertValidStrict("this.x > 0", fixture());
  }

  @Test void stringFieldTypesAsString() {
    CelValidator.assertValidStrict("this.name == 'foo'", fixture());
  }

  @Test void doubleFieldTypesAsDouble() {
    CelValidator.assertValidStrict("this.ratio > 0.0", fixture());
  }

  @Test void bytesFieldTypesAsBytes() {
    CelValidator.assertValidStrict("this.blob == b''", fixture());
  }

  @Test void boolFieldTypesAsBool() {
    CelValidator.assertValidStrict("this.active == true", fixture());
  }

  @Test void timestampFieldTypesAsTimestamp() {
    CelValidator.assertValidStrict("this.ts.getFullYear() > 2000", fixture());
  }

  @Test void listFieldTypesAsList() {
    CelValidator.assertValidStrict("this.tags.size() > 0", fixture());
  }

  @Test void mapFieldTypesAsMap() {
    CelValidator.assertValidStrict("this.scores.size() > 0", fixture());
  }

  @Test void structFieldTypesAsObject() {
    CelValidator.assertValidStrict("this.addr.zip > 0", fixture());
  }

  // -------------------------------------------------------------------
  // Comparison / arithmetic overload coverage
  // -------------------------------------------------------------------

  @Test void rejectsIntVsString() {
    assertThrows(AssertionError.class, () ->
        CelValidator.assertValidStrict("this.x > 'foo'", fixture()));
  }

  @Test void rejectsIntVsDouble() {
    // No `_>_(int, double)` overload — strict catches mixed numeric.
    assertThrows(AssertionError.class, () ->
        CelValidator.assertValidStrict("this.x > 1.5", fixture()));
  }

  @Test void rejectsStringPlusBytes() {
    assertThrows(AssertionError.class, () ->
        CelValidator.assertValidStrict(
            "this.name + this.blob == ''", fixture()));
  }

  @Test void acceptsStringPlusString() {
    // CEL's `+` on string is concat — valid (we layer SQL `+` intent on top
    // via validateArithmeticOperands; strict alone permits this).
    CelValidator.assertValidStrict("this.name + this.name == ''", fixture());
  }

  @Test void rejectsBoolArithmetic() {
    assertThrows(AssertionError.class, () ->
        CelValidator.assertValidStrict(
            "this.active + this.x > 0", fixture()));
  }

  @Test void rejectsCastBoolFromInt() {
    assertThrows(AssertionError.class, () ->
        CelValidator.assertValidStrict("bool(this.x)", fixture()));
  }

  @Test void rejectsCastBytesFromInt() {
    assertThrows(AssertionError.class, () ->
        CelValidator.assertValidStrict("bytes(this.x) == b''", fixture()));
  }

  @Test void acceptsCastBoolFromString() {
    CelValidator.assertValidStrict("bool(this.name) == true", fixture());
  }

  // -------------------------------------------------------------------
  // Macro receivers
  // -------------------------------------------------------------------

  @Test void rejectsMacroOnInt() {
    assertThrows(AssertionError.class, () ->
        CelValidator.assertValidStrict(
            "this.x.all(t, t > 0)", fixture()));
  }

  @Test void allowsMacroOnArray() {
    CelValidator.assertValidStrict(
        "this.tags.all(t, t == 'foo')", fixture());
  }

  @Test void allowsAllMacroOnIntArray() {
    CelValidator.assertValidStrict(
        "this.ages.all(t, t > 0)", fixture());
  }

  @Test void allowsExistsMacroOnArray() {
    CelValidator.assertValidStrict(
        "this.tags.exists(t, t == 'foo')", fixture());
  }

  @Test void macroBodyTypeMismatchRejected() {
    // tags is list<string>, `t > 0` would compare string to int — strict
    // catches the body's bad overload.
    assertThrows(AssertionError.class, () ->
        CelValidator.assertValidStrict(
            "this.tags.all(t, t > 0)", fixture()));
  }

  // -------------------------------------------------------------------
  // commonDeclarations() — format validators
  // -------------------------------------------------------------------

  @Test void allowsIsEmail() {
    CelValidator.assertValidStrict("this.name.isEmail()", fixture());
  }

  @Test void allowsIsHostname() {
    CelValidator.assertValidStrict("this.name.isHostname()", fixture());
  }

  @Test void allowsIsIpv4() {
    CelValidator.assertValidStrict("this.name.isIpv4()", fixture());
  }

  @Test void allowsIsIpv6() {
    CelValidator.assertValidStrict("this.name.isIpv6()", fixture());
  }

  @Test void allowsIsUri() {
    CelValidator.assertValidStrict("this.name.isUri()", fixture());
  }

  @Test void allowsIsUriRef() {
    CelValidator.assertValidStrict("this.name.isUriRef()", fixture());
  }

  @Test void allowsIsUuid() {
    CelValidator.assertValidStrict("this.name.isUuid()", fixture());
  }

  @Test void rejectsIsEmailOnInt() {
    assertThrows(AssertionError.class, () ->
        CelValidator.assertValidStrict("this.x.isEmail()", fixture()));
  }

  // -------------------------------------------------------------------
  // commonDeclarations() — string extension methods
  // -------------------------------------------------------------------

  @Test void allowsUpperAscii() {
    CelValidator.assertValidStrict(
        "this.name.upperAscii() == 'X'", fixture());
  }

  @Test void allowsLowerAscii() {
    CelValidator.assertValidStrict(
        "this.name.lowerAscii() == 'x'", fixture());
  }

  @Test void allowsTrim() {
    CelValidator.assertValidStrict(
        "this.name.trim() == 'x'", fixture());
  }

  @Test void allowsSubstring1Arg() {
    CelValidator.assertValidStrict(
        "this.name.substring(1) == 'x'", fixture());
  }

  @Test void allowsSubstring2Arg() {
    CelValidator.assertValidStrict(
        "this.name.substring(1, 3) == 'x'", fixture());
  }

  @Test void allowsIndexOf() {
    CelValidator.assertValidStrict(
        "this.name.indexOf('a') > 0", fixture());
  }

  @Test void allowsReplace() {
    CelValidator.assertValidStrict(
        "this.name.replace('a', 'b') == 'b'", fixture());
  }

  // -------------------------------------------------------------------
  // commonDeclarations() — `now` variable
  // -------------------------------------------------------------------

  @Test void allowsNowVar() {
    CelValidator.assertValidStrict("now > this.ts", fixture());
  }

  // -------------------------------------------------------------------
  // has() argument shape
  // -------------------------------------------------------------------

  @Test void allowsHasOnField() {
    CelValidator.assertValidStrict("has(this.x)", fixture());
  }

  @Test void rejectsHasOnExpression() {
    // CEL spec: has() argument must be a field-selection. Strict catches.
    assertThrows(AssertionError.class, () ->
        CelValidator.assertValidStrict("has(this.x > 1)", fixture()));
  }

  @Test void rejectsHasOnLiteral() {
    assertThrows(AssertionError.class, () ->
        CelValidator.assertValidStrict("has(1)", fixture()));
  }

  // -------------------------------------------------------------------
  // Bracket access — table-level synthetic root supports string indexing
  // -------------------------------------------------------------------

  @Test void allowsBracketAccessOnRoot() {
    // For CEL-reserved field names the emitter uses `this["name"]` form.
    CelValidator.assertValidStrict(
        "this[\"name\"] == 'foo'", fixture());
  }

  // -------------------------------------------------------------------
  // Column-level vs table-level shape
  // -------------------------------------------------------------------

  @Test void columnLevelIntThis() {
    // Column-level: `this` is the column's primitive type (Int here).
    Schema intCol = Schema.create(Schema.Type.INT);
    CelValidator.assertValidStrictColumnLevel("this > 0", "x", intCol);
  }

  @Test void columnLevelStringThis() {
    Schema stringCol = Schema.createString();
    CelValidator.assertValidStrictColumnLevel("this == 'foo'", "name", stringCol);
  }

  @Test void columnLevelStructThis() {
    // Column-level STRUCT: `this` is the struct, fields accessed directly.
    Schema struct = Schema.createStruct(Arrays.asList(
        new Schema.Field("zip", Schema.create(Schema.Type.INT), 0)));
    CelValidator.assertValidStrictColumnLevel("this.zip > 0", "addr", struct);
  }

  @Test void columnLevelIntThisStringComparisonRejectedByStrict() {
    // Google cel-java's strict type-checker rejects `int == string` — its
    // `_==_` overload requires both operands to be the same type. This is
    // stricter than Nessie cel-java, which accepted cross-type equality and
    // evaluated always-false at runtime. The hand-coded validateCompare
    // cross-type check still produces the primary user-facing diagnostic;
    // strict here is now a redundant safety net (catches the same case).
    Schema intCol = Schema.create(Schema.Type.INT);
    assertThrows(AssertionError.class, () ->
        CelValidator.assertValidStrictColumnLevel(
            "this == 'foo'", "x", intCol));
  }

  @Test void columnLevelIntThisStringRelationalRejected() {
    // Strict DOES catch ordering ops on int vs string (no
    // `_<_(int,string)` overload).
    Schema intCol = Schema.create(Schema.Type.INT);
    assertThrows(AssertionError.class, () ->
        CelValidator.assertValidStrictColumnLevel(
            "this < 'foo'", "x", intCol));
  }

  // -------------------------------------------------------------------
  // Nested struct field access
  // -------------------------------------------------------------------

  @Test void allowsNestedStructAccess() {
    CelValidator.assertValidStrict("this.addr.zip > 0", fixture());
  }

  @Test void rejectsNestedStructWrongType() {
    // Strict DOES catch ordering ops on nested fields (no overload),
    // but NOT cross-type equality (CEL spec accepts always-false).
    assertThrows(AssertionError.class, () ->
        CelValidator.assertValidStrict(
            "this.addr.zip < 'foo'", fixture()));
  }

  // -------------------------------------------------------------------
  // CEL stdlib overlap (declared by nessie StdLib, NOT us)
  // -------------------------------------------------------------------

  @Test void allowsContains() {
    CelValidator.assertValidStrict(
        "this.name.contains('a')", fixture());
  }

  @Test void allowsStartsWith() {
    CelValidator.assertValidStrict(
        "this.name.startsWith('a')", fixture());
  }

  @Test void allowsEndsWith() {
    CelValidator.assertValidStrict(
        "this.name.endsWith('a')", fixture());
  }

  @Test void allowsMatches() {
    CelValidator.assertValidStrict(
        "this.name.matches('^a.*$')", fixture());
  }

  @Test void rejectsBytesMatches() {
    assertThrows(AssertionError.class, () ->
        CelValidator.assertValidStrict(
            "this.blob.matches('foo')", fixture()));
  }

  @Test void allowsSizeOnList() {
    CelValidator.assertValidStrict(
        "this.tags.size() > 0", fixture());
  }

  @Test void allowsSizeOnString() {
    CelValidator.assertValidStrict(
        "this.name.size() > 0", fixture());
  }

  @Test void allowsTimestampAccessors() {
    CelValidator.assertValidStrict(
        "this.ts.getFullYear() > 2000", fixture());
  }

  // -------------------------------------------------------------------
  // Ternary branch unification (CASE / COALESCE / NULLIF emit as ternary)
  // -------------------------------------------------------------------

  @Test void rejectsTernaryWithBareNullThen() {
    assertThrows(AssertionError.class, () ->
        CelValidator.assertValidStrict(
            "(this.active ? null : 1) == 1", fixture()));
  }

  @Test void rejectsTernaryWithBareNullElse() {
    assertThrows(AssertionError.class, () ->
        CelValidator.assertValidStrict(
            "(this.active ? 1 : null) == 1", fixture()));
  }

  @Test void allowsTernaryWithDynNull() {
    // Our NULLIF emit uses dyn(null)/dyn(value) — branches unify on dyn.
    CelValidator.assertValidStrict(
        "(this.active ? dyn(null) : dyn(this.x)) != null", fixture());
  }

  @Test void rejectsTernaryCrossCategory() {
    assertThrows(AssertionError.class, () ->
        CelValidator.assertValidStrict(
            "(this.active ? 1 : 'a') == 1", fixture()));
  }

  // -------------------------------------------------------------------
  // Extended function-family declarations (schema-rules' BuiltinDeclarations)
  //
  // These smoke tests lock in that the Decimal / Timestamp.of / Variant /
  // CelExtensions.math function families are visible to the strict checker.
  // The SQL emit doesn't currently produce calls into these families — these
  // tests pass hand-written CEL through the strict checker directly to verify
  // the declarations are wired. Catches regressions if BuiltinDeclarations
  // drifts or if the library wiring in ConstraintCelChecker is altered.
  // -------------------------------------------------------------------

  @Test void acceptsDecimalArithmetic() {
    Schema intCol = Schema.create(Schema.Type.INT);
    CelValidator.assertValidStrictColumnLevel(
        "decimals.gt(decimals.add(decimal(\"1.5\"), decimal(\"2.5\")),"
            + " decimal(\"3\"))",
        "x", intCol);
  }

  @Test void acceptsDecimalRoundAndTrunc() {
    Schema intCol = Schema.create(Schema.Type.INT);
    CelValidator.assertValidStrictColumnLevel(
        "decimals.eq(decimals.round(decimal(\"1.55\"), 1), decimal(\"1.6\"))"
            + " && decimals.eq(decimals.trunc(decimal(\"1.55\"), 1), decimal(\"1.5\"))",
        "x", intCol);
  }

  @Test void acceptsTimestampOf() {
    Schema intCol = Schema.create(Schema.Type.INT);
    CelValidator.assertValidStrictColumnLevel(
        "timestamp.of(this, \"millis\") < now", "x", intCol);
  }

  @Test void acceptsVariantConstructorAndNavigation() {
    Schema stringCol = Schema.createVarchar(255);
    CelValidator.assertValidStrictColumnLevel(
        "variants.as("
            + " variants.field(variants.parseJson(this), \"name\"),"
            + " \"string\") == \"alice\"",
        "json", stringCol);
  }

  @Test void acceptsVariantTryAsAndIsNull() {
    Schema stringCol = Schema.createVarchar(255);
    CelValidator.assertValidStrictColumnLevel(
        "variants.tryAs(variants.parseJson(this), \"int\") == null"
            + " || variants.isNull(variants.parseJson(this))",
        "json", stringCol);
  }

  @Test void acceptsMathExtension() {
    Schema intCol = Schema.create(Schema.Type.INT);
    CelValidator.assertValidStrictColumnLevel(
        "math.abs(this) > 0 && math.sign(this) >= -1", "x", intCol);
  }

  @Test void rejectsDecimalArithmeticOnNonDecimal() {
    // Strict catches passing an int where Decimal is expected — decimals.add
    // declares (Decimal, Decimal) only; int isn't assignable.
    Schema intCol = Schema.create(Schema.Type.INT);
    assertThrows(AssertionError.class, () ->
        CelValidator.assertValidStrictColumnLevel(
            "decimals.add(this, decimal(\"1\")) == decimal(\"2\")",
            "x", intCol));
  }
}
