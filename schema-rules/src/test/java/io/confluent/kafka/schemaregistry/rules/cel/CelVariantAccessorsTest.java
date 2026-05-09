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

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.google.protobuf.ByteString;
import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.Descriptors.FieldDescriptor;
import com.google.protobuf.DynamicMessage;
import io.confluent.kafka.schemaregistry.protobuf.ProtobufSchema;
import io.confluent.kafka.schemaregistry.rules.ValidationRuleError;
import io.confluent.kafka.schemaregistry.type.Variant;
import io.confluent.kafka.schemaregistry.type.VariantBuilder;
import io.confluent.kafka.schemaregistry.type.VariantObjectBuilder;
import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.function.Consumer;
import org.junit.jupiter.api.Test;

/**
 * Coverage for the variant accessor surface: {@code variant_type},
 * {@code is_variant_null}, typed extractors ({@code variant_get_string/int/double/
 * bool/decimal/timestamp}), path navigation ({@code variant_get},
 * {@code variant_get_field}, {@code variant_get_element}), and the
 * {@code try_variant_get_*} family.
 */
public class CelVariantAccessorsTest {

  private static final String SCHEMA = "syntax = \"proto3\";\n"
      + "package test;\n"
      + "import \"confluent/meta.proto\";\n"
      + "import \"confluent/type/variant.proto\";\n"
      + "message Event {\n"
      + "  confluent.type.Variant payload = 1 [(confluent.field_meta) = {\n"
      + "    rules: [{name: \"check\", expr: \"<EXPR>\"}]\n"
      + "  }];\n"
      + "}\n";

  /** Build a Variant with a single object containing the given builder steps. */
  private static Variant object(Consumer<VariantObjectBuilder> populate) {
    VariantBuilder vb = new VariantBuilder();
    VariantObjectBuilder ob = vb.startObject();
    populate.accept(ob);
    vb.endObject();
    return vb.build();
  }

  /** Build a top-level scalar Variant via the supplied appender. */
  private static Variant scalar(Consumer<VariantBuilder> populate) {
    VariantBuilder vb = new VariantBuilder();
    populate.accept(vb);
    return vb.build();
  }

  /** Evaluate {@code expr} against an Event whose payload is {@code v}. */
  private static boolean eval(Variant v, String expr) {
    String escaped = expr.replace("\\", "\\\\").replace("\"", "\\\"");
    String schema = SCHEMA.replace("<EXPR>", escaped);
    ProtobufSchema ps = new ProtobufSchema(schema);
    DynamicMessage msg = eventOf(ps, v);
    List<ValidationRuleError> errors = ps.validateMessage(new CelValidator(), msg);
    if (!errors.isEmpty() && errors.get(0).getCause() != null) {
      throw new AssertionError(
          "Rule errored for expr `" + expr + "`: " + errors.get(0).getCause(),
          errors.get(0).getCause());
    }
    return errors.isEmpty();
  }

  private static DynamicMessage eventOf(ProtobufSchema ps, Variant v) {
    try {
      Descriptor desc = ps.toDescriptor("test.Event");
      FieldDescriptor payloadField = desc.findFieldByName("payload");
      ByteBuffer valueBuf = v.getValueBuffer();
      ByteBuffer metadataBuf = v.getMetadataBuffer();
      io.confluent.protobuf.type.Variant proto =
          io.confluent.protobuf.type.Variant.newBuilder()
              .setValue(ByteString.copyFrom(valueBuf))
              .setMetadata(ByteString.copyFrom(metadataBuf))
              .build();
      Descriptor variantDesc = payloadField.getMessageType();
      DynamicMessage payload = DynamicMessage.parseFrom(variantDesc, proto.toByteArray());
      return DynamicMessage.newBuilder(desc).setField(payloadField, payload).build();
    } catch (com.google.protobuf.InvalidProtocolBufferException e) {
      throw new RuntimeException(e);
    }
  }

  // -- variant_type: string returned per Variant.Type --

  @Test
  void variantType_object() {
    assertTrue(eval(object(o -> {}),
        "variant_type(to_variant(this)) == \"object\""));
  }

  @Test
  void variantType_string() {
    assertTrue(eval(scalar(b -> b.appendString("hi")),
        "variant_type(to_variant(this)) == \"string\""));
  }

  @Test
  void variantType_int() {
    assertTrue(eval(scalar(b -> b.appendLong(42L)),
        "variant_type(to_variant(this)) == \"int\""));
  }

  @Test
  void variantType_double() {
    assertTrue(eval(scalar(b -> b.appendDouble(3.14)),
        "variant_type(to_variant(this)) == \"double\""));
  }

  @Test
  void variantType_bool() {
    assertTrue(eval(scalar(b -> b.appendBoolean(true)),
        "variant_type(to_variant(this)) == \"boolean\""));
  }

  @Test
  void variantType_decimal() {
    assertTrue(eval(scalar(b -> b.appendDecimal(new BigDecimal("12.34"))),
        "variant_type(to_variant(this)) == \"decimal\""));
  }

  @Test
  void variantType_null() {
    assertTrue(eval(scalar(VariantBuilder::appendNull),
        "variant_type(to_variant(this)) == \"null\""));
  }

  @Test
  void isVariantNull_yes() {
    assertTrue(eval(scalar(VariantBuilder::appendNull),
        "is_variant_null(to_variant(this))"));
  }

  @Test
  void isVariantNull_no() {
    assertFalse(eval(scalar(b -> b.appendString("nope")),
        "is_variant_null(to_variant(this))"));
  }

  // -- typed extractors --

  @Test
  void variantGetString() {
    assertTrue(eval(scalar(b -> b.appendString("alice")),
        "variant_get_string(to_variant(this)) == \"alice\""));
  }

  @Test
  void variantGetInt_longValue() {
    assertTrue(eval(scalar(b -> b.appendLong(123_456_789L)),
        "variant_get_int(to_variant(this)) == 123456789"));
  }

  @Test
  void variantGetDouble() {
    assertTrue(eval(scalar(b -> b.appendDouble(3.14)),
        "variant_get_double(to_variant(this)) > 3.13 "
            + "&& variant_get_double(to_variant(this)) < 3.15"));
  }

  @Test
  void variantGetBool() {
    assertTrue(eval(scalar(b -> b.appendBoolean(true)),
        "variant_get_bool(to_variant(this))"));
  }

  @Test
  void variantGetDecimal() {
    assertTrue(eval(scalar(b -> b.appendDecimal(new BigDecimal("12.34"))),
        "decimal_eq(variant_get_decimal(to_variant(this)), to_decimal(\"12.34\"))"));
  }

  @Test
  void variantGetTimestamp() {
    // Spark variant timestamp_tz: micros since epoch UTC. 1700000000_000_000 = 2023-11-14
    assertTrue(eval(scalar(b -> b.appendTimestampTz(1_700_000_000_000_000L)),
        "variant_get_timestamp(to_variant(this)) > timestamp(\"2020-01-01T00:00:00Z\")"));
  }

  @Test
  void variantGetBinary() {
    byte[] payload = new byte[] {1, 2, 3, 4};
    assertTrue(eval(scalar(b -> b.appendBinary(java.nio.ByteBuffer.wrap(payload))),
        "size(variant_get_binary(to_variant(this))) == 4"));
  }

  @Test
  void variantGetUuid() {
    java.util.UUID uuid = java.util.UUID.fromString("550e8400-e29b-41d4-a716-446655440000");
    assertTrue(eval(scalar(b -> b.appendUUID(uuid)),
        "variant_get_uuid(to_variant(this)) == \"550e8400-e29b-41d4-a716-446655440000\""));
  }

  @Test
  void variantType_binary() {
    byte[] payload = new byte[] {1, 2, 3};
    assertTrue(eval(scalar(b -> b.appendBinary(java.nio.ByteBuffer.wrap(payload))),
        "variant_type(to_variant(this)) == \"binary\""));
  }

  @Test
  void variantType_uuid() {
    java.util.UUID uuid = java.util.UUID.randomUUID();
    assertTrue(eval(scalar(b -> b.appendUUID(uuid)),
        "variant_type(to_variant(this)) == \"uuid\""));
  }

  // -- variant_get path navigation --

  @Test
  void variantGet_simpleField() {
    Variant v = object(o -> {
      o.appendKey("name");
      o.appendString("alice");
    });
    assertTrue(eval(v,
        "variant_get_string(variant_get(to_variant(this), \"$.name\")) == \"alice\""));
  }

  @Test
  void variantGet_nestedPath() {
    Variant v = object(o -> {
      o.appendKey("user");
      VariantObjectBuilder inner = o.startObject();
      inner.appendKey("name");
      inner.appendString("alice");
      o.endObject();
    });
    assertTrue(eval(v,
        "variant_get_string(variant_get(to_variant(this), \"$.user.name\")) == \"alice\""));
  }

  @Test
  void variantGet_quotedKey() {
    Variant v = object(o -> {
      o.appendKey("first name");
      o.appendString("alice");
    });
    assertTrue(eval(v, "variant_get_string("
        + "variant_get(to_variant(this), \"$[\\\"first name\\\"]\")) == \"alice\""));
  }

  @Test
  void variantGet_missingPath_returnsNull() {
    Variant v = object(o -> {
      o.appendKey("name");
      o.appendString("alice");
    });
    assertTrue(eval(v,
        "is_variant_null(variant_get(to_variant(this), \"$.missing\"))"));
  }

  // -- try_variant_get_* type filters --

  @Test
  void tryVariantGetString_match() {
    assertFalse(eval(scalar(b -> b.appendString("hi")),
        "is_variant_null(try_variant_get_string(to_variant(this)))"));
  }

  @Test
  void tryVariantGetString_mismatch() {
    assertTrue(eval(scalar(b -> b.appendLong(42L)),
        "is_variant_null(try_variant_get_string(to_variant(this)))"));
  }

  @Test
  void tryVariantGetInt_match() {
    assertFalse(eval(scalar(b -> b.appendLong(42L)),
        "is_variant_null(try_variant_get_int(to_variant(this)))"));
  }

  @Test
  void tryVariantGetInt_mismatch() {
    assertTrue(eval(scalar(b -> b.appendString("nope")),
        "is_variant_null(try_variant_get_int(to_variant(this)))"));
  }

  @Test
  void tryVariantGetDecimal_match() {
    assertFalse(eval(scalar(b -> b.appendDecimal(new BigDecimal("1.5"))),
        "is_variant_null(try_variant_get_decimal(to_variant(this)))"));
  }

  @Test
  void tryVariantGetBinary_match() {
    byte[] payload = new byte[] {1, 2};
    assertFalse(eval(scalar(b -> b.appendBinary(java.nio.ByteBuffer.wrap(payload))),
        "is_variant_null(try_variant_get_binary(to_variant(this)))"));
  }

  @Test
  void tryVariantGetBinary_mismatch() {
    assertTrue(eval(scalar(b -> b.appendString("nope")),
        "is_variant_null(try_variant_get_binary(to_variant(this)))"));
  }

  @Test
  void tryVariantGetUuid_match() {
    java.util.UUID uuid = java.util.UUID.randomUUID();
    assertFalse(eval(scalar(b -> b.appendUUID(uuid)),
        "is_variant_null(try_variant_get_uuid(to_variant(this)))"));
  }

  @Test
  void tryVariantGetUuid_mismatch() {
    assertTrue(eval(scalar(b -> b.appendString("not-a-uuid")),
        "is_variant_null(try_variant_get_uuid(to_variant(this)))"));
  }

  // -- JSON string ↔ Variant --

  @Test
  void variantToJson_object() {
    Variant v = object(o -> {
      o.appendKey("name");
      o.appendString("alice");
    });
    assertTrue(eval(v,
        "variant_to_json(to_variant(this)) == \"{\\\"name\\\":\\\"alice\\\"}\""));
  }

  @Test
  void variantToJson_scalarString() {
    // A top-level string variant serializes to a JSON-quoted string.
    assertTrue(eval(scalar(b -> b.appendString("hi")),
        "variant_to_json(to_variant(this)) == \"\\\"hi\\\"\""));
  }

  @Test
  void variantToJson_scalarInt() {
    assertTrue(eval(scalar(b -> b.appendLong(42L)),
        "variant_to_json(to_variant(this)) == \"42\""));
  }

  @Test
  void variantToJson_null() {
    assertTrue(eval(scalar(VariantBuilder::appendNull),
        "variant_to_json(to_variant(this)) == \"null\""));
  }

  @Test
  void toVariant_jsonString_object() {
    // Build any payload (the rule ignores `this`); verify to_variant parses
    // the JSON literal in the rule body.
    assertTrue(eval(object(o -> {
      o.appendKey("placeholder");
      o.appendString("");
    }),
        "variant_get_string("
            + "variant_get_field("
            + "to_variant(\"{\\\"name\\\":\\\"alice\\\"}\"), \"name\")) == \"alice\""));
  }

  @Test
  void toVariant_jsonString_array() {
    assertTrue(eval(scalar(VariantBuilder::appendNull),
        "variant_get_int("
            + "variant_get_element(to_variant(\"[1,2,3]\"), 1)) == 2"));
  }

  @Test
  void toVariant_malformedJson_errors() {
    // Malformed JSON should produce a clean ValidationRuleError (the safeUnary
    // wrapper converts the IllegalArgumentException to a CEL Err).
    Variant v = scalar(VariantBuilder::appendNull);
    String escaped = "is_variant_null(to_variant(\"{not valid json}\"))"
        .replace("\\", "\\\\").replace("\"", "\\\"");
    String schema = SCHEMA.replace("<EXPR>", escaped);
    ProtobufSchema ps = new ProtobufSchema(schema);
    DynamicMessage msg = eventOf(ps, v);
    List<ValidationRuleError> errors = ps.validateMessage(new CelValidator(), msg);
    // Expect exactly one violation, with the cause chain pointing at JSON parse failure.
    org.junit.jupiter.api.Assertions.assertEquals(1, errors.size(),
        "Malformed JSON should produce one violation, got: " + errors);
    Throwable cause = errors.get(0).getCause();
    org.junit.jupiter.api.Assertions.assertNotNull(cause,
        "Expected a cause chain on malformed-JSON violation");
  }

  // Round-trip: build a variant, serialize to JSON, parse it back, and assert
  // the parsed result equals the original at the leaf level.
  @Test
  void jsonRoundTrip() {
    Variant v = object(o -> {
      o.appendKey("name");
      o.appendString("alice");
    });
    assertTrue(eval(v,
        "variant_get_string("
            + "variant_get_field("
            + "to_variant(variant_to_json(to_variant(this))), \"name\")) == \"alice\""));
  }
}
