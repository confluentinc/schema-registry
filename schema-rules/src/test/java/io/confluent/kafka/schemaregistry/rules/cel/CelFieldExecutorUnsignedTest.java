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

import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.DynamicMessage;
import io.confluent.kafka.schemaregistry.ParsedSchema;
import io.confluent.kafka.schemaregistry.client.rest.entities.Rule;
import io.confluent.kafka.schemaregistry.client.rest.entities.RuleKind;
import io.confluent.kafka.schemaregistry.client.rest.entities.RuleMode;
import io.confluent.kafka.schemaregistry.protobuf.ProtobufSchema;
import io.confluent.kafka.schemaregistry.rules.FieldTransform;
import io.confluent.kafka.schemaregistry.rules.RuleContext;
import io.confluent.kafka.schemaregistry.rules.RuleContext.FieldContext;
import io.confluent.kafka.schemaregistry.rules.RuleContext.Type;
import java.util.Collections;
import java.util.Map;
import org.junit.jupiter.api.Test;

/**
 * A domain rule (CEL_FIELD) on an unsigned protobuf field. Java has no unsigned primitive,
 * so a {@code uint64} reaches the transform as a {@code Long} exactly as an {@code int64}
 * does; the field context carries the distinction so the executor can present it to CEL as
 * unsigned without changing the value the field itself holds.
 *
 * <p>That reading is not the one this executor has always used, so it is asked for by rule
 * parameter — {@code cel.unsigned.field.type: "uint"} — and rules that do not ask keep the
 * signed reading they were written against. Both are exercised here.
 */
public class CelFieldExecutorUnsignedTest {

  private static final String SCHEMA =
      "syntax = \"proto3\"; package test; "
          + "message U { uint64 serial = 1; int64 signed = 2; }";

  private static final Map<String, String> UINT_MODE =
      Collections.singletonMap(CelExecutor.CEL_UNSIGNED_FIELD_TYPE, "uint");

  private static Object transform(String expr, String fieldName, long value)
      throws Exception {
    return transform(expr, fieldName, value, UINT_MODE);
  }

  private static Object transform(String expr, String fieldName, long value,
      Map<String, String> params) throws Exception {
    return transform(new CelFieldExecutor(), expr, fieldName, value, params);
  }

  private static Object transform(CelFieldExecutor executor, String expr, String fieldName,
      long value, Map<String, String> params) throws Exception {
    Rule rule = new Rule("myRule", null, RuleKind.TRANSFORM, RuleMode.WRITE,
        CelFieldExecutor.TYPE, null, params, expr, null, null, false);
    Map<String, Object> configs = Collections.emptyMap();
    // enterField reads metadata tags off the target schema, so it cannot be left unset,
    // and the containing message is bound to the rule as `message`.
    ParsedSchema target = new ProtobufSchema(SCHEMA);
    Descriptor desc = ((ProtobufSchema) target).toDescriptor("test.U");
    DynamicMessage message = DynamicMessage.newBuilder(desc)
        .setField(desc.findFieldByName(fieldName), value).build();
    RuleContext ctx = new RuleContext(configs, null, null, target, "topic-value", "topic",
        null, null, null, false, RuleMode.WRITE, rule, 0, Collections.singletonList(rule));
    FieldTransform fieldTransform = executor.newTransform(ctx);
    try (FieldContext fc = ctx.enterField(message, "test.U." + fieldName, fieldName,
        Type.LONG, Collections.emptySet(), desc.findFieldByName(fieldName))) {
      return fieldTransform.transform(ctx, fc, value);
    }
  }

  @Test
  public void unsignedFieldIsPresentedToCelAsUnsigned() throws Exception {
    // 25 % 10 == 5, so the rule hands the value back. Read as a signed int the expression
    // has no matching overload at all and the rule fails instead.
    assertEquals(25L, transform("value % 10u == 5u ? value : 0u", "serial", 25L));
  }

  @Test
  public void unsignedFieldComparesCorrectlyAboveLongMaxValue() throws Exception {
    // 2^64 - 5 is positive as an unsigned value but negative as a signed long, so a signed
    // reading would take the other branch. The value handed back is protobuf's own
    // representation, ready to be written back to the field.
    assertEquals(-5L, transform("value > 0u ? value : 0u", "serial", -5L));
  }

  @Test
  public void signedFieldIsUnaffected() throws Exception {
    // An int64 field declares itself signed, so nothing about its handling changes.
    assertEquals(7L, transform("value > 0 ? value : 0", "signed", 7L));
  }

  @Test
  public void aRuleThatAsksForNothingKeepsTheSignedReading() throws Exception {
    // The same bit pattern, read the way rules written before the mode existed read it:
    // 2^64 - 5 is a negative long, so a comparison against a signed literal is false, where
    // under the unsigned reading the very same expression is true.
    // One executor answers both, so the two rules must not collide in its compiled-rule
    // cache even though they agree on expression, schema, and field. They don't, because
    // the declared type of `value` is part of the key and is exactly what the mode changes.
    CelFieldExecutor executor = new CelFieldExecutor();
    assertEquals(false, transform(executor, "value > 0", "serial", -5L, null));
    assertEquals(true, transform(executor, "value > 0", "serial", -5L, UINT_MODE));
  }
}
