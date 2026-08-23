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
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertSame;

import io.confluent.kafka.schemaregistry.ParsedSchema;
import io.confluent.kafka.schemaregistry.avro.AvroSchema;
import io.confluent.kafka.schemaregistry.client.rest.entities.Rule;
import io.confluent.kafka.schemaregistry.client.rest.entities.RuleKind;
import io.confluent.kafka.schemaregistry.client.rest.entities.RuleMode;
import io.confluent.kafka.schemaregistry.rules.FieldTransform;
import io.confluent.kafka.schemaregistry.rules.RuleContext;
import io.confluent.kafka.schemaregistry.rules.RuleContext.FieldContext;
import io.confluent.kafka.schemaregistry.rules.RuleContext.Type;
import io.confluent.kafka.schemaregistry.rules.cel.builtin.CelDecimal;
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.apache.avro.LogicalTypes;
import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.junit.jupiter.api.Test;

/**
 * The consumer side of the Decimal wrapper: what a TRANSFORM rule <em>returns</em>.
 *
 * <p>Inside CEL a Decimal is a {@link CelDecimal}; everything downstream of evaluation wants a
 * {@link BigDecimal}. Both executors therefore unwrap before any result writer runs — recursively,
 * since a rule can return a Decimal at top level or buried in a result map/list.
 */
public class CelDecimalTransformTest {

  /** {@code record Money { bytes(decimal 10,2) amount; string label; }} */
  private static Schema moneySchema() {
    Schema decimalSchema = LogicalTypes.decimal(10, 2)
        .addToSchema(Schema.create(Schema.Type.BYTES));
    return SchemaBuilder.record("Money").namespace("test").fields()
        .name("amount").type(decimalSchema).noDefault()
        .requiredString("label")
        .endRecord();
  }

  private static GenericRecord money(Schema schema, String amount, String label) {
    GenericRecord r = new GenericData.Record(schema);
    r.put("amount", new BigDecimal(amount));
    r.put("label", label);
    return r;
  }

  private static RuleContext ctx(String expr, String executorType, ParsedSchema target) {
    Rule rule = new Rule("myRule", null, RuleKind.TRANSFORM, RuleMode.WRITE,
        executorType, null, null, expr, null, null, false);
    return new RuleContext(Collections.emptyMap(), null, null, target, "topic-value", "topic",
        null, null, null, false, RuleMode.WRITE, rule, 0, Collections.singletonList(rule));
  }

  // ---- CelExecutor: the whole-message transform path ----

  private static Object transformMessage(String expr, GenericRecord record) throws Exception {
    AvroSchema target = new AvroSchema(record.getSchema());
    RuleContext ruleCtx = ctx(expr, CelExecutor.TYPE, target);
    return new CelExecutor().transform(ruleCtx, record);
  }

  @Test
  public void decimalInsideResultMapReachesAvroAsBigDecimal() throws Exception {
    Schema schema = moneySchema();
    // Without the unwrap the AvroResultWriter rejects a CelDecimal at a bytes/decimal field.
    Object result = transformMessage(
        "{\"amount\": decimals.round(decimal(message.amount), 1), \"label\": message.label}",
        money(schema, "100.56", "usd"));
    GenericRecord out = assertInstanceOf(GenericRecord.class, result);
    assertEquals(new BigDecimal("100.6"), out.get("amount"));
    assertEquals("usd", out.get("label").toString());
  }

  @Test
  public void decimalArithmeticInResultMap() throws Exception {
    Schema schema = moneySchema();
    Object result = transformMessage(
        "{\"amount\": decimals.add(decimal(message.amount), decimal(\"1.00\")),"
            + " \"label\": message.label}",
        money(schema, "100.56", "usd"));
    GenericRecord out = assertInstanceOf(GenericRecord.class, result);
    assertEquals(new BigDecimal("101.56"), out.get("amount"));
  }

  @Test
  public void unchangedResultIsPassedThroughUntouched() throws Exception {
    // A rule that returns no decimals must not be reshaped by the unwrap pass.
    Schema schema = moneySchema();
    Object result = transformMessage(
        "{\"amount\": message.amount, \"label\": message.label + \"!\"}",
        money(schema, "100.56", "usd"));
    GenericRecord out = assertInstanceOf(GenericRecord.class, result);
    assertEquals(new BigDecimal("100.56"), out.get("amount"));
    assertEquals("usd!", out.get("label").toString());
  }

  // ---- CelFieldExecutor: the field transform path ----

  private static Object transformField(String expr, Schema schema, String fieldName,
      Type fieldType, Object fieldValue) throws Exception {
    AvroSchema target = new AvroSchema(schema);
    RuleContext ruleCtx = ctx(expr, CelFieldExecutor.TYPE, target);
    GenericRecord containing = money(schema, "100.56", "usd");
    FieldTransform fieldTransform = new CelFieldExecutor().newTransform(ruleCtx);
    try (FieldContext fc = ruleCtx.enterField(containing, "test.Money." + fieldName,
        fieldName, fieldType, Collections.emptySet(), null)) {
      return fieldTransform.transform(ruleCtx, fc, fieldValue);
    }
  }

  @Test
  public void decimalFieldResultIsUnwrappedForTheFieldSetter() throws Exception {
    Object result = transformField(
        "decimals.round(decimal(value), 1)", moneySchema(), "amount", Type.BYTES,
        new BigDecimal("100.56"));
    assertEquals(BigDecimal.class, result.getClass(),
        "a Decimal result must reach the field setter as a BigDecimal");
    assertEquals(new BigDecimal("100.6"), result);
  }

  @Test
  public void decimalResultStillGoesThroughNumericNarrowing() throws Exception {
    // The unwrap has to run BEFORE the Number-based narrowing chain, which a CelDecimal
    // would silently skip.
    Schema intSchema = SchemaBuilder.record("Money").namespace("test").fields()
        .requiredInt("amount")
        .requiredString("label")
        .endRecord();
    Object result = transformField(
        "decimals.round(decimal(value), 0)", intSchema, "amount", Type.INT, 7);
    assertEquals(Integer.class, result.getClass(),
        "a Decimal result at an INT field must narrow to Integer");
    assertEquals(7, result);
  }

  // ---- the unwrap helper's own contract ----

  @Test
  public void unwrapIsRecursiveAndIdentityPreserving() {
    CelDecimal d = CelDecimal.of(new BigDecimal("2.50"));

    assertEquals(new BigDecimal("2.50"), CelUtils.unwrapCelDecimals(d));

    // Nested: map -> list -> map -> decimal.
    Map<String, Object> inner = new LinkedHashMap<>();
    inner.put("d", d);
    inner.put("s", "keep");
    Map<String, Object> outer = new LinkedHashMap<>();
    outer.put("first", "untouched");
    outer.put("nested", Collections.singletonList(inner));
    @SuppressWarnings("unchecked")
    Map<String, Object> unwrapped = (Map<String, Object>) CelUtils.unwrapCelDecimals(outer);
    assertEquals("untouched", unwrapped.get("first"));
    @SuppressWarnings("unchecked")
    List<Object> nested = (List<Object>) unwrapped.get("nested");
    @SuppressWarnings("unchecked")
    Map<String, Object> unwrappedInner = (Map<String, Object>) nested.get(0);
    assertEquals(new BigDecimal("2.50"), unwrappedInner.get("d"));
    assertEquals("keep", unwrappedInner.get("s"));
    // Key order is preserved across the copy.
    assertEquals(Arrays.asList("first", "nested"), new ArrayList<>(unwrapped.keySet()));
    assertEquals(Arrays.asList("d", "s"), new ArrayList<>(unwrappedInner.keySet()));

    // A decimal before the last entry still triggers the lazy copy correctly.
    Map<String, Object> decimalFirst = new LinkedHashMap<>();
    decimalFirst.put("d", d);
    decimalFirst.put("s", "keep");
    @SuppressWarnings("unchecked")
    Map<String, Object> df = (Map<String, Object>) CelUtils.unwrapCelDecimals(decimalFirst);
    assertEquals(new BigDecimal("2.50"), df.get("d"));
    assertEquals("keep", df.get("s"));
    assertEquals(2, df.size());

    List<Object> list = Arrays.asList(d, "x", 1L);
    @SuppressWarnings("unchecked")
    List<Object> unwrappedList = (List<Object>) CelUtils.unwrapCelDecimals(list);
    assertEquals(Arrays.asList(new BigDecimal("2.50"), "x", 1L), unwrappedList);

    // Nothing to unwrap: the very same instance comes back, no copy.
    Map<String, Object> clean = new LinkedHashMap<>();
    clean.put("a", 1L);
    clean.put("b", Collections.singletonList("c"));
    assertSame(clean, CelUtils.unwrapCelDecimals(clean));
    assertSame(list.get(1), CelUtils.unwrapCelDecimals(list.get(1)));
    List<Object> cleanList = Arrays.asList(1L, "x");
    assertSame(cleanList, CelUtils.unwrapCelDecimals(cleanList));
    assertSame(null, CelUtils.unwrapCelDecimals(null));
  }
}
