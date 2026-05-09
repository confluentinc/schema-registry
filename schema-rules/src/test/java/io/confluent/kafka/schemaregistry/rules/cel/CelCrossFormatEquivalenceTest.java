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

import com.google.protobuf.ByteString;
import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.Descriptors.FieldDescriptor;
import com.google.protobuf.DynamicMessage;
import com.google.protobuf.Timestamp;
import io.confluent.avro.type.VariantConversion;
import io.confluent.kafka.schemaregistry.avro.AvroSchema;
import io.confluent.kafka.schemaregistry.protobuf.ProtobufSchema;
import io.confluent.kafka.schemaregistry.rules.ValidationRuleError;
import io.confluent.kafka.schemaregistry.type.Variant;
import io.confluent.kafka.schemaregistry.type.VariantBuilder;
import io.confluent.kafka.schemaregistry.type.VariantObjectBuilder;
import io.confluent.protobuf.type.Decimal;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.time.Instant;
import java.util.Collections;
import java.util.List;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.IndexedRecord;
import org.junit.jupiter.api.Test;

/**
 * Verifies that the same CEL expression text yields equivalent validation
 * outcomes across Proto and Avro schemas for each extended type. This is the
 * portability check for the spec — if a rule author writes
 * {@code decimal_ge(to_decimal(this), to_decimal("0"))}, both formats must
 * report the same violations on the same logical input.
 */
public class CelCrossFormatEquivalenceTest {

  // ============================================================== Decimal ==

  private static final String DECIMAL_RULE =
      "decimal_ge(to_decimal(this), to_decimal(\\\"0\\\"))";

  private static final String DECIMAL_RULE_UNESCAPED =
      "decimal_ge(to_decimal(this), to_decimal(\"0\"))";

  private static final String PROTO_DECIMAL_SCHEMA = ""
      + "syntax = \"proto3\";\n"
      + "package test;\n"
      + "import \"confluent/meta.proto\";\n"
      + "import \"confluent/type/decimal.proto\";\n"
      + "message Order {\n"
      + "  confluent.type.Decimal amount = 1 [(confluent.field_meta) = {\n"
      + "    rules: [{name: \"r\", expr: \"" + DECIMAL_RULE + "\"}]\n"
      + "  }];\n"
      + "}\n";

  private static final String AVRO_DECIMAL_SCHEMA = "{"
      + "\"type\":\"record\","
      + "\"name\":\"Order\","
      + "\"namespace\":\"test\","
      + "\"fields\":[{"
      + "  \"name\":\"amount\","
      + "  \"type\":{\"type\":\"bytes\",\"logicalType\":\"decimal\","
      + "          \"precision\":10,\"scale\":2},"
      + "  \"confluent:rules\":[{\"name\":\"r\","
      + "                       \"expr\":\"" + DECIMAL_RULE + "\"}]"
      + "}]}";

  private static int protoDecimalViolations(BigDecimal amount) {
    try {
      ProtobufSchema ps = new ProtobufSchema(PROTO_DECIMAL_SCHEMA);
      Descriptor desc = ps.toDescriptor("test.Order");
      FieldDescriptor amountField = desc.findFieldByName("amount");
      Decimal d = Decimal.newBuilder()
          .setValue(ByteString.copyFrom(amount.unscaledValue().toByteArray()))
          .setScale(amount.scale())
          .build();
      Descriptor decDesc = amountField.getMessageType();
      DynamicMessage payload = DynamicMessage.parseFrom(decDesc, d.toByteArray());
      DynamicMessage msg = DynamicMessage.newBuilder(desc)
          .setField(amountField, payload).build();
      return ps.validateMessage(new CelValidator(), msg).size();
    } catch (com.google.protobuf.InvalidProtocolBufferException e) {
      throw new RuntimeException(e);
    }
  }

  private static int avroDecimalViolations(BigDecimal amount) {
    AvroSchema as = new AvroSchema(AVRO_DECIMAL_SCHEMA);
    GenericRecord r = new GenericData.Record(as.rawSchema());
    r.put("amount", amount);
    return as.validateMessage(new CelValidator(), r).size();
  }

  @Test
  void decimal_positive_satisfiesBothFormats() {
    BigDecimal v = new BigDecimal("100.50");
    int proto = protoDecimalViolations(v);
    int avro = avroDecimalViolations(v);
    assertEquals(proto, avro,
        "Proto and Avro should agree on positive decimal: proto=" + proto
            + " avro=" + avro);
    assertEquals(0, proto, "expected no violations for positive amount");
  }

  @Test
  void decimal_negative_violatesBothFormats() {
    BigDecimal v = new BigDecimal("-1.50");
    int proto = protoDecimalViolations(v);
    int avro = avroDecimalViolations(v);
    assertEquals(proto, avro,
        "Proto and Avro should agree on negative decimal: proto=" + proto
            + " avro=" + avro);
    assertEquals(1, proto, "expected exactly one violation for negative amount");
  }

  @Test
  void decimal_ruleExpressionIsPortable() {
    // Sanity: the expression text used in both schemas is byte-identical.
    assertTrue(PROTO_DECIMAL_SCHEMA.contains(DECIMAL_RULE));
    assertTrue(AVRO_DECIMAL_SCHEMA.contains(DECIMAL_RULE));
  }

  // ============================================================ Timestamp ==

  private static final String TIMESTAMP_RULE = "to_timestamp(this) <= now";

  private static final String PROTO_TIMESTAMP_SCHEMA = ""
      + "syntax = \"proto3\";\n"
      + "package test;\n"
      + "import \"confluent/meta.proto\";\n"
      + "import \"google/protobuf/timestamp.proto\";\n"
      + "message Event {\n"
      + "  google.protobuf.Timestamp created_at = 1 [(confluent.field_meta) = {\n"
      + "    rules: [{name: \"r\", expr: \"" + TIMESTAMP_RULE + "\"}]\n"
      + "  }];\n"
      + "}\n";

  private static final String AVRO_TIMESTAMP_SCHEMA = "{"
      + "\"type\":\"record\","
      + "\"name\":\"Event\","
      + "\"namespace\":\"test\","
      + "\"fields\":[{"
      + "  \"name\":\"created_at\","
      + "  \"type\":{\"type\":\"long\",\"logicalType\":\"timestamp-millis\"},"
      + "  \"confluent:rules\":[{\"name\":\"r\","
      + "                       \"expr\":\"" + TIMESTAMP_RULE + "\"}]"
      + "}]}";

  private static int protoTimestampViolations(Instant when) {
    ProtobufSchema ps = new ProtobufSchema(PROTO_TIMESTAMP_SCHEMA);
    Descriptor desc = ps.toDescriptor("test.Event");
    FieldDescriptor ts = desc.findFieldByName("created_at");
    Timestamp t = Timestamp.newBuilder()
        .setSeconds(when.getEpochSecond()).setNanos(when.getNano()).build();
    DynamicMessage msg = DynamicMessage.newBuilder(desc).setField(ts, t).build();
    return ps.validateMessage(new CelValidator(), msg).size();
  }

  private static int avroTimestampViolations(Instant when) {
    AvroSchema as = new AvroSchema(AVRO_TIMESTAMP_SCHEMA);
    GenericRecord r = new GenericData.Record(as.rawSchema());
    r.put("created_at", when);
    return as.validateMessage(new CelValidator(), r).size();
  }

  @Test
  void timestamp_past_satisfiesBothFormats() {
    Instant past = Instant.now().minusSeconds(60);
    assertEquals(protoTimestampViolations(past), avroTimestampViolations(past),
        "Proto and Avro should agree on past timestamp");
    assertEquals(0, protoTimestampViolations(past));
  }

  @Test
  void timestamp_future_violatesBothFormats() {
    Instant future = Instant.now().plusSeconds(3600);
    assertEquals(protoTimestampViolations(future), avroTimestampViolations(future),
        "Proto and Avro should agree on future timestamp");
    assertEquals(1, protoTimestampViolations(future));
  }

  // ============================================================== Variant ==

  private static final VariantConversion VARIANT_CONVERSION = new VariantConversion();

  private static final String VARIANT_RULE =
      "variant_get_string(variant_get_field(to_variant(this), \\\"name\\\")) "
      + "== \\\"alice\\\"";

  private static final String PROTO_VARIANT_SCHEMA = ""
      + "syntax = \"proto3\";\n"
      + "package test;\n"
      + "import \"confluent/meta.proto\";\n"
      + "import \"confluent/type/variant.proto\";\n"
      + "message Event {\n"
      + "  confluent.type.Variant payload = 1 [(confluent.field_meta) = {\n"
      + "    rules: [{name: \"r\", expr: \"" + VARIANT_RULE + "\"}]\n"
      + "  }];\n"
      + "}\n";

  private static Variant nameObject(String name) {
    VariantBuilder vb = new VariantBuilder();
    VariantObjectBuilder ob = vb.startObject();
    ob.appendKey("name");
    ob.appendString(name);
    vb.endObject();
    return vb.build();
  }

  private static int protoVariantViolations(Variant v) {
    try {
      ProtobufSchema ps = new ProtobufSchema(PROTO_VARIANT_SCHEMA);
      Descriptor desc = ps.toDescriptor("test.Event");
      FieldDescriptor payloadField = desc.findFieldByName("payload");
      io.confluent.protobuf.type.Variant proto =
          io.confluent.protobuf.type.Variant.newBuilder()
              .setValue(ByteString.copyFrom(v.getValueBuffer()))
              .setMetadata(ByteString.copyFrom(v.getMetadataBuffer()))
              .build();
      Descriptor variantDesc = payloadField.getMessageType();
      DynamicMessage payload = DynamicMessage.parseFrom(variantDesc, proto.toByteArray());
      DynamicMessage msg = DynamicMessage.newBuilder(desc)
          .setField(payloadField, payload).build();
      return ps.validateMessage(new CelValidator(), msg).size();
    } catch (com.google.protobuf.InvalidProtocolBufferException e) {
      throw new RuntimeException(e);
    }
  }

  private static int avroVariantViolations(Variant v) {
    Schema variantSchema = VARIANT_CONVERSION.getRecommendedSchema();
    Schema.Field payload = new Schema.Field(
        "payload", variantSchema, null, (Object) null);
    payload.addProp("confluent:rules", List.of(java.util.Map.of(
        "name", "r",
        "expr",
        "variant_get_string(variant_get_field(to_variant(this), \"name\")) "
            + "== \"alice\"")));
    Schema outer = Schema.createRecord("Event", null, "test", false,
        Collections.singletonList(payload));

    AvroSchema as = new AvroSchema(outer);
    GenericRecord r = new GenericData.Record(outer);
    IndexedRecord rec = VARIANT_CONVERSION.toRecord(
        v, variantSchema, variantSchema.getLogicalType());
    r.put("payload", rec);
    return as.validateMessage(new CelValidator(), r).size();
  }

  @Test
  void variant_aliceName_satisfiesBothFormats() {
    Variant v = nameObject("alice");
    int proto = protoVariantViolations(v);
    int avro = avroVariantViolations(v);
    assertEquals(proto, avro, "Proto and Avro should agree on alice variant");
    assertEquals(0, proto);
  }

  @Test
  void variant_otherName_violatesBothFormats() {
    Variant v = nameObject("bob");
    int proto = protoVariantViolations(v);
    int avro = avroVariantViolations(v);
    assertEquals(proto, avro, "Proto and Avro should agree on bob variant");
    assertEquals(1, proto);
  }
}
