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

package io.confluent.kafka.schemaregistry.rules.cel;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.DynamicMessage;
import dev.cel.common.CelValidationException;
import dev.cel.common.CelVarDecl;
import dev.cel.common.types.SimpleType;
import dev.cel.common.types.StructTypeReference;
import dev.cel.runtime.CelRuntime;
import io.confluent.kafka.schemaregistry.protobuf.ProtobufSchema;
import io.confluent.kafka.schemaregistry.rules.cel.CelUtils.RegexEngine;
import io.confluent.kafka.schemaregistry.rules.cel.CelUtils.ScriptType;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;

/**
 * Sanity coverage for CEL {@code ==} / {@code !=} across the type surface, plus the PCRE
 * {@code matches} override.
 *
 * <p>Equality here is the <em>stock</em> cel-java stdlib binding. {@link CelUtils} briefly
 * replaced it to work around bare-{@link java.math.BigDecimal} decimals; that override is gone
 * now that a Decimal is a non-{@link Number}
 * {@link io.confluent.kafka.schemaregistry.rules.cel.builtin.CelDecimal}. This class is the
 * insurance that handing equality back to the stdlib regressed nothing.
 *
 * <p>{@code matches} is still overridden in PCRE mode — now the sole
 * {@code setStandardFunctions(...)} exclusion. Every case runs under BOTH regex engines, so that
 * override's presence can't change an equality answer.
 *
 * @see CelDecimalEqualityTest for the decimal side
 */
public class CelStandardEqualityTest {

  /** Evaluate a variable-free boolean expression under the given regex engine. */
  private static boolean evalBool(String expr, RegexEngine engine) throws Exception {
    CelRuntime.Program program = CelUtils.buildProgram(
        ScriptType.JSON, expr, null, Collections.emptyList(), engine);
    return (Boolean) program.eval();
  }

  /** Assert a variable-free boolean expression yields {@code expected} under both engines. */
  private static void assertEval(boolean expected, String expr) throws Exception {
    for (RegexEngine engine : RegexEngine.values()) {
      assertEquals(expected, evalBool(expr, engine), expr + " (engine=" + engine + ")");
    }
  }

  // ---- scalars ----

  @Test
  void intEquality() throws Exception {
    assertEval(true, "1 == 1");
    assertEval(false, "1 == 2");
    assertEval(false, "1 != 1");
    assertEval(true, "1 != 2");
  }

  @Test
  void stringEquality() throws Exception {
    assertEval(true, "'abc' == 'abc'");
    assertEval(false, "'abc' == 'abd'");
    assertEval(true, "'abc' != 'abd'");
  }

  @Test
  void boolEquality() throws Exception {
    assertEval(true, "true == true");
    assertEval(false, "true == false");
    assertEval(true, "true != false");
  }

  @Test
  void doubleEquality() throws Exception {
    assertEval(true, "1.5 == 1.5");
    assertEval(false, "1.5 == 2.5");
    assertEval(true, "1.5 != 2.5");
  }

  @Test
  void uintEquality() throws Exception {
    assertEval(true, "1u == 1u");
    assertEval(false, "1u == 2u");
  }

  // ---- heterogeneous numerics ----
  //
  // cel-java's `_==_` declaration is homogeneous — `(%A0, %A0)` — so the heterogeneous path is
  // reached only when one side is dyn, which is how it shows up in practice: CelUtils.findCelType
  // returns DYN for logical types, unknown classes and null.

  @Test
  void heterogeneousNumericEquality() throws Exception {
    assertEval(true, "dyn(1) == 1.0");
    assertEval(false, "dyn(1) == 2.0");
    assertEval(true, "dyn(1) == 1u");
    assertEval(false, "dyn(1) == 2u");
    assertEval(true, "dyn(1.0) == 1u");
    assertEval(true, "dyn(1) != 2.0");
    assertEval(false, "dyn(1) != 1.0");
  }

  @Test
  void heterogeneousNumericEqualityIsNotDeclaredForLiterals() {
    // Pinning the compiler behavior so the case above isn't mistaken for a runtime limitation:
    // `==` is declared (%A0, %A0), and CelUtils leaves the compiler side alone.
    assertThrows(CelValidationException.class, () -> evalBool("1 == 1.0", RegexEngine.PCRE));
  }

  // ---- bytes ----

  @Test
  void bytesEquality() throws Exception {
    assertEval(true, "b'abc' == b'abc'");
    assertEval(false, "b'abc' == b'abd'");
    assertEval(false, "b'abc' == b'ab'");
    assertEval(true, "b'abc' != b'abd'");
  }

  // ---- aggregates (recursive equality) ----

  @Test
  void listEquality() throws Exception {
    assertEval(true, "[1, 2, 3] == [1, 2, 3]");
    assertEval(false, "[1, 2, 3] == [1, 2, 4]");
    assertEval(false, "[1, 2] == [1, 2, 3]");
    assertEval(true, "[] == []");
    // Recursion has to reach nested elements, and heterogeneous numerics inside them.
    assertEval(true, "[[1, 2], ['a']] == [[1, 2], ['a']]");
    assertEval(false, "[[1, 2], ['a']] == [[1, 2], ['b']]");
    assertEval(true, "dyn([1, 2]) == [1.0, 2.0]");
    assertEval(false, "dyn([1, 2]) == [1.0, 3.0]");
  }

  @Test
  void mapEquality() throws Exception {
    assertEval(true, "{'a': 1, 'b': 2} == {'b': 2, 'a': 1}");
    assertEval(false, "{'a': 1} == {'a': 2}");
    assertEval(false, "{'a': 1} == {'b': 1}");
    assertEval(false, "{'a': 1} == {'a': 1, 'b': 2}");
    assertEval(true, "{'a': {'b': [1]}} == {'a': {'b': [1]}}");
    assertEval(false, "{'a': {'b': [1]}} == {'a': {'b': [2]}}");
  }

  // ---- well-known temporal types ----

  @Test
  void timestampEquality() throws Exception {
    assertEval(true,
        "timestamp('2020-01-01T00:00:00Z') == timestamp('2020-01-01T00:00:00Z')");
    assertEval(false,
        "timestamp('2020-01-01T00:00:00Z') == timestamp('2021-01-01T00:00:00Z')");
    assertEval(true,
        "timestamp('2020-01-01T00:00:00Z') != timestamp('2021-01-01T00:00:00Z')");
  }

  @Test
  void durationEquality() throws Exception {
    assertEval(true, "duration('1h') == duration('60m')");
    assertEval(false, "duration('1h') == duration('2h')");
    assertEval(true, "duration('1h') != duration('2h')");
  }

  // ---- null ----

  @Test
  void nullEquality() throws Exception {
    assertEval(true, "null == null");
    assertEval(false, "null != null");
  }

  @Test
  void nullComparedToBoundValue() throws Exception {
    // A DYN-declared var — what CelUtils.findCelType produces for a null value, and the only
    // declaration under which `value == null` type-checks at all.
    List<CelVarDecl> decls =
        Collections.singletonList(CelVarDecl.newVarDeclaration("value", SimpleType.DYN));
    for (RegexEngine engine : RegexEngine.values()) {
      CelRuntime.Program program =
          CelUtils.buildProgram(ScriptType.JSON, "value == null", null, decls, engine);
      assertTrue((Boolean) program.eval(
              Collections.singletonMap("value", CelUtils.toCelValue(null))),
          "a bound null must equal null (engine=" + engine + ")");
      assertFalse((Boolean) program.eval(
              Collections.singletonMap("value", CelUtils.toCelValue("x"))),
          "a bound non-null must not equal null (engine=" + engine + ")");
      assertFalse((Boolean) CelUtils
              .buildProgram(ScriptType.JSON, "value != null", null, decls, engine)
              .eval(Collections.singletonMap("value", CelUtils.toCelValue(null))),
          "!= must negate for a bound null (engine=" + engine + ")");
    }
  }

  // ---- proto messages ----

  private static final String PROTO = "syntax = \"proto3\";\n"
      + "package test;\n"
      + "message Point {\n"
      + "  int32 x = 1;\n"
      + "  int32 y = 2;\n"
      + "}\n";

  private static DynamicMessage point(Descriptor desc, int x, int y) {
    return DynamicMessage.newBuilder(desc)
        .setField(desc.findFieldByName("x"), x)
        .setField(desc.findFieldByName("y"), y)
        .build();
  }

  /** Evaluate a boolean expression over two Point messages bound as {@code this} / {@code other}. */
  private static boolean evalProto(String expr, DynamicMessage self, DynamicMessage other,
      Descriptor desc, RegexEngine engine) throws Exception {
    List<CelVarDecl> decls = Arrays.asList(
        CelVarDecl.newVarDeclaration("this", StructTypeReference.create(desc.getFullName())),
        CelVarDecl.newVarDeclaration("other", StructTypeReference.create(desc.getFullName())));
    CelRuntime.Program program =
        CelUtils.buildProgram(ScriptType.PROTOBUF, expr, desc, decls, engine);
    Map<String, Object> args = new HashMap<>();
    args.put("this", self);
    args.put("other", other);
    return (Boolean) program.eval(args);
  }

  @Test
  void protoMessageEquality() throws Exception {
    Descriptor desc = new ProtobufSchema(PROTO).toDescriptor("test.Point");
    DynamicMessage a = point(desc, 1, 2);
    DynamicMessage sameAsA = point(desc, 1, 2);
    DynamicMessage b = point(desc, 3, 4);

    for (RegexEngine engine : RegexEngine.values()) {
      String at = " (engine=" + engine + ")";
      assertTrue(evalProto("this == this", a, b, desc, engine),
          "a message must equal itself" + at);
      assertFalse(evalProto("this != this", a, b, desc, engine),
          "!= must negate self-equality" + at);
      // Distinct instances with identical contents must compare equal — proto message
      // equality must not degrade to reference identity.
      assertTrue(evalProto("this == other", a, sameAsA, desc, engine),
          "equal-valued distinct messages must compare equal" + at);
      assertFalse(evalProto("this == other", a, b, desc, engine),
          "different-valued messages must not compare equal" + at);
      assertTrue(evalProto("this != other", a, b, desc, engine),
          "!= must hold for different-valued messages" + at);
      // Field-level equality through a message still works.
      assertTrue(evalProto("this.x == 1 && this.y == 2", a, b, desc, engine),
          "scalar field equality on a message" + at);
    }
  }

  // ---- the PCRE matches override still composes with everything else ----

  @Test
  void matchesWorksUnderBothEngines() throws Exception {
    assertEval(true, "'abc123'.matches('[a-z]+[0-9]+')");
    assertEval(false, "'abc'.matches('[0-9]+')");
    // And matches in the same expression as an equality.
    assertEval(true, "'abc'.matches('[a-z]+') == true");
  }

  @Test
  void pcreOnlyFeatureNeedsPcreEngine() throws Exception {
    // Lookahead is the reason the PCRE override exists: java.util.regex supports it, RE2
    // does not. Pins that the override is actually installed in PCRE mode.
    assertTrue(evalBool("'abc1'.matches('^(?=.*[0-9]).*$')", RegexEngine.PCRE),
        "PCRE mode must support lookahead");
  }
}
