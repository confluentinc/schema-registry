/*
 * Copyright 2025 Confluent Inc.
 *
 * Licensed under the Confluent Community License (the "License"); you may not use
 * this file except in compliance with the License.  You may obtain a copy of the
 * License at
 *
 * http://www.confluent.io/confluent-community-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OF ANY KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations under the License.
 */

package io.confluent.kafka.schemaregistry.rules.cel;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.google.protobuf.ByteString;
import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.Message;
import com.google.protobuf.Timestamp;
import io.confluent.kafka.schemaregistry.client.rest.entities.Rule;
import io.confluent.kafka.schemaregistry.client.rest.entities.RuleKind;
import io.confluent.kafka.schemaregistry.client.rest.entities.RuleMode;
import io.confluent.kafka.schemaregistry.protobuf.ProtobufSchema;
import io.confluent.kafka.schemaregistry.rules.CelValueTypesProto.ValueTypes;
import io.confluent.kafka.schemaregistry.rules.FieldTransform;
import io.confluent.kafka.schemaregistry.rules.RuleContext;
import io.confluent.kafka.schemaregistry.rules.RuleConditionException;
import io.confluent.kafka.schemaregistry.type.Variant;
import io.confluent.kafka.schemaregistry.type.VariantUtils;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import org.junit.jupiter.api.Test;

/**
 * CEL rules over the protobuf message types that stand in for Avro's logical types:
 * {@code confluent.type.Decimal}, {@code google.protobuf.Timestamp} and
 * {@code confluent.type.Variant}. The protobuf counterpart of {@link CelDecimalTransformTest}.
 *
 * <p>Two behaviours are pinned here. A CEL_FIELD rule reaches such a field as a single value —
 * {@code ProtobufSchema.getType} reports the primitive type its Avro counterpart reports, and the
 * walk stops there instead of descending into {@code value}/{@code scale}/{@code seconds}. And a
 * message-level rule that *computes* one of them writes back correctly, which needs
 * {@code ProtobufResultWriter}: protobuf JSON has no rendering for a bare BigDecimal or a Variant.
 */
public class CelProtobufValueTypeTest {

  private static io.confluent.protobuf.type.Decimal decimalOf(String v) {
    BigDecimal d = new BigDecimal(v);
    return io.confluent.protobuf.type.Decimal.newBuilder()
        .setValue(ByteString.copyFrom(d.unscaledValue().toByteArray()))
        .setPrecision(8)
        .setScale(d.scale())
        .build();
  }

  private static ValueTypes message() {
    BigDecimal amount = new BigDecimal("12.34");
    Variant variant = VariantUtils.fromJson("{\"name\":\"alice\"}");
    return ValueTypes.newBuilder()
        .setAmount(io.confluent.protobuf.type.Decimal.newBuilder()
            .setValue(ByteString.copyFrom(amount.unscaledValue().toByteArray()))
            .setPrecision(8)
            .setScale(amount.scale())
            .build())
        .setTs(Timestamp.newBuilder().setSeconds(1700000000L).setNanos(123000000).build())
        .setData(io.confluent.protobuf.type.Variant.newBuilder()
            .setValue(ByteString.copyFrom(variant.getValueBuffer().duplicate()))
            .setMetadata(ByteString.copyFrom(variant.getMetadataBuffer().duplicate()))
            .build())
        .setLabel("hi")
        .setRatio(1.5d)
        .addAmounts(decimalOf("1.11"))
        .addAmounts(decimalOf("2.22"))
        .putAmountMap("a", decimalOf("3.33"))
        .setTotalAmount(decimalOf("9.99"))
        .build();
  }

  private static ProtobufSchema schema() {
    return new ProtobufSchema(ValueTypes.getDescriptor());
  }

  private static RuleContext ctx(
      String expr, String executorType, RuleKind kind, Set<String> tags) {
    Rule rule = new Rule("myRule", null, kind, RuleMode.WRITE,
        executorType, tags, null, expr, null, null, false);
    return new RuleContext(Collections.emptyMap(), null, null, schema(), "topic-value", "topic",
        null, null, null, false, RuleMode.WRITE, rule, 0, Collections.singletonList(rule));
  }

  /** Runs a CEL_FIELD rule over the whole message through the real field walk. */
  private static Object transformField(String expr, RuleKind kind, String tag) throws Exception {
    RuleContext ruleCtx = ctx(expr, CelFieldExecutor.TYPE, kind, Collections.singleton(tag));
    FieldTransform transform = new CelFieldExecutor().newTransform(ruleCtx);
    return schema().transformMessage(ruleCtx, transform, message());
  }

  /** Runs a message-level CEL rule; the expression returns a map of field name to value. */
  private static Object transformMessage(String expr) throws Exception {
    return new CelExecutor().transform(
        ctx(expr, CelExecutor.TYPE, RuleKind.TRANSFORM, null), message());
  }

  private static boolean hasRuleException(Throwable t) {
    for (Throwable c = t; c != null && c.getCause() != c; c = c.getCause()) {
      if (c instanceof io.confluent.kafka.schemaregistry.rules.RuleException) {
        return true;
      }
    }
    return false;
  }

  private static String rootMessage(Throwable t) {
    Throwable root = t;
    while (root.getCause() != null && root.getCause() != root) {
      root = root.getCause();
    }
    return String.valueOf(root.getMessage());
  }

  private static boolean isConditionFailure(Throwable t) {
    for (Throwable c = t; c != null && c.getCause() != c; c = c.getCause()) {
      if (c instanceof RuleConditionException) {
        return true;
      }
    }
    return false;
  }

  // ---- readers over the result, which comes back as a DynamicMessage ----

  private static Message field(Message msg, String name) {
    return (Message) msg.getField(msg.getDescriptorForType().findFieldByName(name));
  }

  private static BigDecimal amountOf(Message msg) {
    Message amount = field(msg, "amount");
    Descriptor d = amount.getDescriptorForType();
    ByteString unscaled = (ByteString) amount.getField(d.findFieldByName("value"));
    int scale = ((Number) amount.getField(d.findFieldByName("scale"))).intValue();
    return new BigDecimal(new BigInteger(unscaled.toByteArray()), scale);
  }

  private static int precisionOf(Message msg) {
    Message amount = field(msg, "amount");
    return ((Number) amount.getField(
        amount.getDescriptorForType().findFieldByName("precision"))).intValue();
  }

  private static String tsOf(Message msg) {
    Message ts = field(msg, "ts");
    Descriptor d = ts.getDescriptorForType();
    return ts.getField(d.findFieldByName("seconds")) + "."
        + ts.getField(d.findFieldByName("nanos"));
  }

  private static String variantOf(Message msg) {
    Message data = field(msg, "data");
    Descriptor d = data.getDescriptorForType();
    return VariantUtils.toJsonString(new Variant(
        ((ByteString) data.getField(d.findFieldByName("value"))).toByteArray(),
        ((ByteString) data.getField(d.findFieldByName("metadata"))).toByteArray()));
  }

  private static BigDecimal decimalIn(Message holder) {
    Descriptor d = holder.getDescriptorForType();
    ByteString unscaled = (ByteString) holder.getField(d.findFieldByName("value"));
    int scale = ((Number) holder.getField(d.findFieldByName("scale"))).intValue();
    return new BigDecimal(new BigInteger(unscaled.toByteArray()), scale);
  }

  @SuppressWarnings("unchecked")
  private static List<BigDecimal> amountsOf(Message msg) {
    List<Message> raw = (List<Message>) msg.getField(
        msg.getDescriptorForType().findFieldByName("amounts"));
    List<BigDecimal> out = new ArrayList<>(raw.size());
    for (Message m : raw) {
      out.add(decimalIn(m));
    }
    return out;
  }

  /** A proto map arrives as a list of synthetic entry messages. */
  @SuppressWarnings("unchecked")
  private static BigDecimal amountMapValue(Message msg, String key) {
    List<Message> entries = (List<Message>) msg.getField(
        msg.getDescriptorForType().findFieldByName("amount_map"));
    for (Message entry : entries) {
      Descriptor ed = entry.getDescriptorForType();
      if (key.equals(entry.getField(ed.findFieldByName("key")))) {
        return decimalIn((Message) entry.getField(ed.findFieldByName("value")));
      }
    }
    return null;
  }

  // ---- CEL_FIELD: the field is a leaf, not a record to descend into ----

  @Test
  public void fieldWalkTreatsValueTypesAsLeaves() throws Exception {
    RuleContext ruleCtx = ctx("value", CelFieldExecutor.TYPE, RuleKind.TRANSFORM, null);
    List<String> visited = new ArrayList<>();
    FieldTransform spy = (c, fieldCtx, value) -> {
      visited.add(fieldCtx.getFullName() + "=" + fieldCtx.getType());
      return value;
    };
    schema().transformMessage(ruleCtx, spy, message());

    assertTrue(visited.contains("io.confluent.kafka.schemaregistry.rules.ValueTypes.amount=BYTES"),
        "a Decimal field is a BYTES leaf, as its Avro counterpart is: " + visited);
    assertTrue(visited.contains("io.confluent.kafka.schemaregistry.rules.ValueTypes.ts=LONG"),
        "a Timestamp field is a LONG leaf: " + visited);
    // The walk must not descend into a value type: transforming value/scale/seconds/nanos
    // individually would corrupt it, and an untagged rule applies to every primitive field.
    assertFalse(visited.stream().anyMatch(v -> v.startsWith("confluent.type.Decimal.")),
        "must not descend into Decimal: " + visited);
    assertFalse(visited.stream().anyMatch(v -> v.startsWith("google.protobuf.Timestamp.")),
        "must not descend into Timestamp: " + visited);
  }

  @Test
  public void fieldConditionOnDecimal() throws Exception {
    Object result = transformField(
        "decimals.gt(decimal(value), decimal(\"10.00\"))", RuleKind.CONDITION, "AMOUNT");
    assertEquals(new BigDecimal("12.34"), amountOf(assertInstanceOf(Message.class, result)));

    Exception e = assertThrows(Exception.class, () -> transformField(
        "decimals.gt(decimal(value), decimal(\"1000.00\"))", RuleKind.CONDITION, "AMOUNT"));
    assertTrue(isConditionFailure(e), "12.34 > 1000.00 must fail the condition, got: " + e);
  }

  @Test
  public void fieldConditionOnTimestamp() throws Exception {
    Object result = transformField(
        "value > timestamp(\"2000-01-01T00:00:00Z\")", RuleKind.CONDITION, "TS");
    assertEquals("1700000000.123000000", tsOf(assertInstanceOf(Message.class, result)));

    Exception e = assertThrows(Exception.class, () -> transformField(
        "value > timestamp(\"2050-01-01T00:00:00Z\")", RuleKind.CONDITION, "TS"));
    assertTrue(isConditionFailure(e), "2023 > 2050 must fail the condition, got: " + e);
  }

  @Test
  public void fieldTransformOnDecimal() throws Exception {
    Message out = assertInstanceOf(Message.class, transformField(
        "decimals.add(decimal(value), decimal(\"1.00\"))", RuleKind.TRANSFORM, "AMOUNT"));
    assertEquals(new BigDecimal("13.34"), amountOf(out),
        "a Decimal result must be rebuilt as a confluent.type.Decimal message");
    assertEquals(4, precisionOf(out), "precision describes the value, as DecimalUtils does");
  }

  @Test
  public void fieldTransformOnTimestamp() throws Exception {
    Message out = assertInstanceOf(Message.class, transformField(
        "value + duration(\"60s\")", RuleKind.TRANSFORM, "TS"));
    assertEquals("1700000060.123000000", tsOf(out),
        "a Timestamp result must be rebuilt as a google.protobuf.Timestamp, nanos preserved");
  }

  /** A variant is a record in both formats, so a CEL_FIELD rule never reaches it. */
  @Test
  public void fieldTransformSkipsVariant() throws Exception {
    Message out = assertInstanceOf(Message.class, transformField(
        "variants.parseJson(\"{\\\"name\\\":\\\"bob\\\"}\")", RuleKind.TRANSFORM, "DATA"));
    assertEquals("{\"name\":\"alice\"}", variantOf(out));
  }

  // ---- message-level CEL: a computed value must round-trip back into the message ----

  private static final String ALL =
      "\"amount\": message.amount, \"ts\": message.ts, \"data\": message.data,"
          + " \"label\": message.label, \"ratio\": message.ratio";

  @Test
  public void messageTransformPassesValueTypesThrough() throws Exception {
    Message out = assertInstanceOf(Message.class, transformMessage("{" + ALL + "}"));
    assertEquals(new BigDecimal("12.34"), amountOf(out));
    assertEquals("1700000000.123000000", tsOf(out));
    assertEquals("{\"name\":\"alice\"}", variantOf(out));
  }

  @Test
  public void messageTransformComputesDecimal() throws Exception {
    Message out = assertInstanceOf(Message.class, transformMessage(
        "{\"amount\": decimals.add(decimal(message.amount), decimal(\"1.00\")), \"ts\": message.ts,"
            + " \"data\": message.data, \"label\": message.label, \"ratio\": message.ratio}"));
    assertEquals(new BigDecimal("13.34"), amountOf(out));
  }

  @Test
  public void messageTransformComputesTimestamp() throws Exception {
    Message out = assertInstanceOf(Message.class, transformMessage(
        "{\"amount\": message.amount, \"ts\": message.ts + duration(\"60s\"),"
            + " \"data\": message.data, \"label\": message.label, \"ratio\": message.ratio}"));
    assertEquals("1700000060.123000000", tsOf(out));
  }

  @Test
  public void messageTransformComputesVariant() throws Exception {
    Message out = assertInstanceOf(Message.class, transformMessage(
        "{\"amount\": message.amount, \"ts\": message.ts,"
            + " \"data\": variants.parseJson(\"{\\\"name\\\":\\\"bob\\\"}\"),"
            + " \"label\": message.label, \"ratio\": message.ratio}"));
    assertEquals("{\"name\":\"bob\"}", variantOf(out));
  }

  // ---- repeated, map and JSON-name cases ----

  /** Each element of a repeated Decimal field reaches the rule on its own and must be rebuilt. */
  @Test
  public void fieldTransformOnRepeatedDecimal() throws Exception {
    Message out = assertInstanceOf(Message.class, transformField(
        "decimals.add(decimal(value), decimal(\"1.00\"))", RuleKind.TRANSFORM, "AMOUNTS"));
    assertEquals(List.of(new BigDecimal("2.11"), new BigDecimal("3.22")), amountsOf(out));
  }

  @Test
  public void messageTransformComputesRepeatedDecimal() throws Exception {
    Message out = assertInstanceOf(Message.class, transformMessage(
        "{" + ALL + ", \"amounts\": [decimals.add(decimal(\"1.00\"), decimal(\"0.11\"))]}"));
    assertEquals(List.of(new BigDecimal("1.11")), amountsOf(out));
  }

  /**
   * A proto map is a repeated synthetic entry message, but its CEL and protobuf JSON form is an
   * object, so it needs converting per entry rather than per list element.
   */
  @Test
  public void messageTransformComputesDecimalInMap() throws Exception {
    Message out = assertInstanceOf(Message.class, transformMessage(
        "{" + ALL + ", \"amount_map\": {\"a\": decimals.add(decimal(\"3.00\"),"
            + " decimal(\"0.33\"))}}"));
    assertEquals(new BigDecimal("3.33"), amountMapValue(out, "a"));
  }

  /** JsonFormat accepts a field's JSON name as well as its declared name; so must the writer. */
  @Test
  public void messageTransformComputesDecimalUnderJsonName() throws Exception {
    Message out = assertInstanceOf(Message.class, transformMessage(
        "{" + ALL + ", \"totalAmount\": decimals.add(decimal(\"9.00\"), decimal(\"0.99\"))}"));
    assertEquals(new BigDecimal("9.99"), decimalIn(field(out, "total_amount")));
  }

  // ---- a result that cannot be stored is a rule error, not a raw CCE/NPE ----

  @Test
  public void fieldTransformRejectsUnstorableResult() {
    Exception e = assertThrows(Exception.class, () -> transformField(
        "'REDACTED'", RuleKind.TRANSFORM, "AMOUNT"));
    assertTrue(hasRuleException(e), "expected a RuleException, got: " + e);
    assertTrue(rootMessage(e).contains("expected a decimal"), rootMessage(e));
  }

  @Test
  public void fieldTransformClearsFieldOnNull() throws Exception {
    Message out = assertInstanceOf(Message.class, transformField(
        "null", RuleKind.TRANSFORM, "AMOUNT"));
    assertFalse(out.hasField(out.getDescriptorForType().findFieldByName("amount")),
        "a null result clears the field rather than raising a NullPointerException");
  }

  /**
   * The re-shaping is driven by the field's declared type, not by the result's Java type: a
   * decimal computed for a plain {@code double} field must still render as a number. Converting
   * every decimal would break this.
   */
  @Test
  public void computedDecimalIntoNumericFieldStaysANumber() throws Exception {
    Message out = assertInstanceOf(Message.class, transformMessage(
        "{\"amount\": message.amount, \"ts\": message.ts, \"data\": message.data,"
            + " \"label\": message.label,"
            + " \"ratio\": decimals.add(decimal(\"1.50\"), decimal(\"1.00\"))}"));
    assertEquals(2.5d,
        ((Number) out.getField(out.getDescriptorForType().findFieldByName("ratio"))).doubleValue(),
        0.0d);
  }
}
