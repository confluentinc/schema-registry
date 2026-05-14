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
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.DynamicMessage;
import io.confluent.kafka.schemaregistry.protobuf.ProtobufSchema;
import io.confluent.kafka.schemaregistry.rules.ValidationRuleError;
import java.util.List;
import org.junit.jupiter.api.Test;

/**
 * Smoke tests that {@code CelExtensions.math(CelOptions.DEFAULT)} is wired
 * into the compiler + runtime via {@link CelUtils#buildProgram}. Covers a
 * function ({@code math.abs}), a binary op ({@code math.bitAnd}), and the
 * variadic macro ({@code math.greatest}) — the macro path exercises
 * {@code setParserOptions}, the function path exercises decl + binding.
 */
public class CelMathExtensionsTest {

  /**
   * Builds a {@code test.X} schema carrying the given CEL expression as a
   * {@code message_meta} rule, fills fields a/b/c with the given longs, and
   * runs validation. One helper avoids the cross-descriptor field copy that
   * separate {@code msg(...)} + {@code validate(...)} helpers force.
   */
  private static List<ValidationRuleError> validate(String expr, long a, long b, long c) {
    String s = "syntax = \"proto3\";\n"
        + "package test;\n"
        + "import \"confluent/meta.proto\";\n"
        + "message X {\n"
        + "  option (confluent.message_meta) = {\n"
        + "    rules: [{name: \"r\", expr: \""
        + expr.replace("\\", "\\\\").replace("\"", "\\\"")
        + "\"}]\n"
        + "  };\n"
        + "  int64 a = 1;\n"
        + "  int64 b = 2;\n"
        + "  int64 c = 3;\n"
        + "}\n";
    ProtobufSchema schema = new ProtobufSchema(s);
    Descriptor desc = schema.toDescriptor("test.X");
    DynamicMessage m = DynamicMessage.newBuilder(desc)
        .setField(desc.findFieldByName("a"), a)
        .setField(desc.findFieldByName("b"), b)
        .setField(desc.findFieldByName("c"), c)
        .build();
    return schema.validateMessage(new CelValidator(), m);
  }

  @Test
  void mathAbs_negativeMakesPositive() {
    // math.abs(-5) == 5
    assertTrue(validate("math.abs(this.a) == 5", -5L, 0L, 0L).isEmpty());
  }

  @Test
  void mathBitAnd_masksCorrectly() {
    // 0b1100 & 0b1010 == 0b1000
    assertTrue(validate("math.bitAnd(this.a, this.b) == 8", 12L, 10L, 0L).isEmpty());
  }

  @Test
  void mathGreatest_variadicMacro() {
    // math.greatest(a, b, c) — exercises the macro/parser path.
    assertTrue(validate("math.greatest(this.a, this.b, this.c) == 7", 3L, 7L, 5L).isEmpty());
  }

  @Test
  void mathLeast_variadicMacro() {
    assertTrue(validate("math.least(this.a, this.b, this.c) == 3", 3L, 7L, 5L).isEmpty());
  }

  @Test
  void mathSign_returnsExpected() {
    assertTrue(validate("math.sign(this.a) == -1", -42L, 0L, 0L).isEmpty());
  }

  @Test
  void mathAbs_failingRuleReportsError() {
    List<ValidationRuleError> errs = validate("math.abs(this.a) == 999", -5L, 0L, 0L);
    assertEquals(1, errs.size());
  }
}
