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

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.protobuf.ByteString;
import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.DynamicMessage;
import io.confluent.kafka.schemaregistry.protobuf.ProtobufSchema;
import io.confluent.kafka.schemaregistry.rules.ValidationRuleError;
import io.confluent.kafka.schemaregistry.type.Variant;
import io.confluent.kafka.schemaregistry.type.VariantUtils;
import java.nio.ByteBuffer;
import java.util.List;
import org.junit.jupiter.api.Test;

/**
 * Integration tests for {@code variant(...)} and {@code variants.*} accessors
 * against a Proto schema with a {@code confluent.type.Variant} field.
 */
public class CelValidatorVariantTest {

  private static final ObjectMapper MAPPER = new ObjectMapper();

  /** Build a Doc proto with a Variant field carrying the given JSON payload. */
  private static DynamicMessage docWithVariantJson(ProtobufSchema schema, String json)
      throws Exception {
    Variant v = VariantUtils.fromJsonNode(MAPPER.readTree(json));
    Descriptor docDesc = schema.toDescriptor("test.Doc");
    Descriptor variantDesc = docDesc.findFieldByName("payload").getMessageType();

    DynamicMessage variantMsg = DynamicMessage.newBuilder(variantDesc)
        .setField(variantDesc.findFieldByName("value"), toByteString(getValueBuffer(v)))
        .setField(variantDesc.findFieldByName("metadata"), toByteString(getMetadataBuffer(v)))
        .build();
    return DynamicMessage.newBuilder(docDesc)
        .setField(docDesc.findFieldByName("payload"), variantMsg)
        .build();
  }

  private static ByteString toByteString(ByteBuffer buf) {
    ByteBuffer dup = buf.duplicate();
    byte[] out = new byte[dup.remaining()];
    dup.get(out);
    return ByteString.copyFrom(out);
  }

  /** Variant.value is package-private; round-trip through toJsonString → Variant to extract. */
  private static ByteBuffer getValueBuffer(Variant v) {
    // We rebuild via the public byte[] constructor from JSON serialization. A bit
    // roundabout but avoids touching package-private internals.
    String j = VariantUtils.toJsonString(v);
    try {
      Variant rebuilt = VariantUtils.fromJsonNode(MAPPER.readTree(j));
      // VariantValue/Metadata exposed via reflection on the package-private fields
      // would be cleaner, but the json roundtrip suffices for testing.
      java.lang.reflect.Field valueField = Variant.class.getDeclaredField("value");
      valueField.setAccessible(true);
      return (ByteBuffer) valueField.get(rebuilt);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  private static ByteBuffer getMetadataBuffer(Variant v) {
    try {
      java.lang.reflect.Field metaField = Variant.class.getDeclaredField("metadata");
      metaField.setAccessible(true);
      return (ByteBuffer) metaField.get(v);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  // ---- variants.type, variants.getString via variants.getField ----

  @Test
  void variantGetField_getString_passes() throws Exception {
    String s = "syntax = \"proto3\";\n"
        + "package test;\n"
        + "import \"confluent/meta.proto\";\n"
        + "import \"confluent/type/variant.proto\";\n"
        + "message Doc {\n"
        + "  confluent.type.Variant payload = 1 [(confluent.field_meta) = {\n"
        + "    rules: [{name: \"nameIsAlice\","
        + "             expr: \"variants.getString("
        + "                       variants.getField(variant(this), \\\"name\\\"))"
        + "                    == \\\"alice\\\"\"}]\n"
        + "  }];\n"
        + "}\n";
    ProtobufSchema schema = new ProtobufSchema(s);
    DynamicMessage doc = docWithVariantJson(schema, "{\"name\":\"alice\"}");
    List<ValidationRuleError> errs = schema.validateMessage(new CelValidator(), doc);
    assertTrue(errs.isEmpty(), "got: " + errs + " causes: " + dumpCauses(errs));
  }

  private static String dumpCauses(List<ValidationRuleError> errs) {
    StringBuilder sb = new StringBuilder();
    for (ValidationRuleError e : errs) {
      Throwable t = e.getCause();
      while (t != null) {
        sb.append("\n  ").append(t.getClass().getSimpleName()).append(": ").append(t.getMessage());
        t = t.getCause();
      }
    }
    return sb.toString();
  }

  @Test
  void variantGetField_getString_failsOnMismatch() throws Exception {
    String s = "syntax = \"proto3\";\n"
        + "package test;\n"
        + "import \"confluent/meta.proto\";\n"
        + "import \"confluent/type/variant.proto\";\n"
        + "message Doc {\n"
        + "  confluent.type.Variant payload = 1 [(confluent.field_meta) = {\n"
        + "    rules: [{name: \"r\","
        + "             expr: \"variants.getString("
        + "                       variants.getField(variant(this), \\\"name\\\"))"
        + "                    == \\\"alice\\\"\"}]\n"
        + "  }];\n"
        + "}\n";
    ProtobufSchema schema = new ProtobufSchema(s);
    DynamicMessage doc = docWithVariantJson(schema, "{\"name\":\"bob\"}");
    List<ValidationRuleError> errs = schema.validateMessage(new CelValidator(), doc);
    assertEquals(1, errs.size());
  }

  // ---- variants.get JSONPath navigation ----

  @Test
  void variantsGet_jsonPathNavigation() throws Exception {
    String s = "syntax = \"proto3\";\n"
        + "package test;\n"
        + "import \"confluent/meta.proto\";\n"
        + "import \"confluent/type/variant.proto\";\n"
        + "message Doc {\n"
        + "  confluent.type.Variant payload = 1 [(confluent.field_meta) = {\n"
        + "    rules: [{name: \"r\","
        + "             expr: \"variants.getString("
        + "                       variants.get(variant(this), \\\"$.user.name\\\"))"
        + "                    == \\\"alice\\\"\"}]\n"
        + "  }];\n"
        + "}\n";
    ProtobufSchema schema = new ProtobufSchema(s);
    DynamicMessage doc = docWithVariantJson(schema,
        "{\"user\":{\"name\":\"alice\",\"role\":\"admin\"}}");
    assertTrue(schema.validateMessage(new CelValidator(), doc).isEmpty());
  }

  // ---- variants.isNull on a missing path ----

  @Test
  void variantsIsNull_missingFieldReturnsVariantNull() throws Exception {
    String s = "syntax = \"proto3\";\n"
        + "package test;\n"
        + "import \"confluent/meta.proto\";\n"
        + "import \"confluent/type/variant.proto\";\n"
        + "message Doc {\n"
        + "  confluent.type.Variant payload = 1 [(confluent.field_meta) = {\n"
        + "    rules: [{name: \"r\","
        + "             expr: \"variants.isNull("
        + "                       variants.getField(variant(this), \\\"missing\\\"))\"}]\n"
        + "  }];\n"
        + "}\n";
    ProtobufSchema schema = new ProtobufSchema(s);
    DynamicMessage doc = docWithVariantJson(schema, "{\"name\":\"alice\"}");
    assertTrue(schema.validateMessage(new CelValidator(), doc).isEmpty());
  }

  // ---- variants.type returns correct strings ----

  @Test
  void variantsType_reportsObject() throws Exception {
    String s = "syntax = \"proto3\";\n"
        + "package test;\n"
        + "import \"confluent/meta.proto\";\n"
        + "import \"confluent/type/variant.proto\";\n"
        + "message Doc {\n"
        + "  confluent.type.Variant payload = 1 [(confluent.field_meta) = {\n"
        + "    rules: [{name: \"r\","
        + "             expr: \"variants.type(variant(this)) == \\\"object\\\"\"}]\n"
        + "  }];\n"
        + "}\n";
    ProtobufSchema schema = new ProtobufSchema(s);
    DynamicMessage doc = docWithVariantJson(schema, "{\"x\":1}");
    assertTrue(schema.validateMessage(new CelValidator(), doc).isEmpty());
  }

  // ---- variants.getInt: cross-type — variant integer flows into CEL int arithmetic ----

  @Test
  void variantGetInt_thenArithmetic() throws Exception {
    // Jackson parses JSON `42` as Int → Variant.Type.INT. variants.getInt extracts
    // it as a CEL int (long). The rule then compares against an int literal.
    String s = "syntax = \"proto3\";\n"
        + "package test;\n"
        + "import \"confluent/meta.proto\";\n"
        + "import \"confluent/type/variant.proto\";\n"
        + "message Doc {\n"
        + "  confluent.type.Variant payload = 1 [(confluent.field_meta) = {\n"
        + "    rules: [{name: \"r\","
        + "             expr: \"variants.getInt("
        + "                       variants.getField(variant(this), \\\"count\\\")) >= 0\"}]\n"
        + "  }];\n"
        + "}\n";
    ProtobufSchema schema = new ProtobufSchema(s);
    DynamicMessage doc = docWithVariantJson(schema, "{\"count\":42}");
    List<ValidationRuleError> errs = schema.validateMessage(new CelValidator(), doc);
    assertTrue(errs.isEmpty(), "got: " + errs + " causes: " + dumpCauses(errs));
  }

  // ---- variants.tryGet* — type-mismatch returns NULL Variant ----

  @Test
  void variantTryGetString_onIntVariantReturnsNull() throws Exception {
    String s = "syntax = \"proto3\";\n"
        + "package test;\n"
        + "import \"confluent/meta.proto\";\n"
        + "import \"confluent/type/variant.proto\";\n"
        + "message Doc {\n"
        + "  confluent.type.Variant payload = 1 [(confluent.field_meta) = {\n"
        + "    rules: [{name: \"r\","
        + "             expr: \"variants.isNull("
        + "                       variants.tryGetString("
        + "                         variants.getField(variant(this), \\\"count\\\")))\"}]\n"
        + "  }];\n"
        + "}\n";
    ProtobufSchema schema = new ProtobufSchema(s);
    DynamicMessage doc = docWithVariantJson(schema, "{\"count\":42}");
    assertTrue(schema.validateMessage(new CelValidator(), doc).isEmpty());
  }

  // ---- JSON-literal constructor: variant("...") ----

  @Test
  void variantFromJsonLiteral_parses() throws Exception {
    String s = "syntax = \"proto3\";\n"
        + "package test;\n"
        + "import \"confluent/meta.proto\";\n"
        + "message X {\n"
        + "  int32 anchor = 1 [(confluent.field_meta) = {\n"
        + "    rules: [{name: \"r\","
        + "             expr: \"variants.getString("
        + "                       variants.getField("
        + "                         variant(\\\"{\\\\\\\"name\\\\\\\":\\\\\\\"alice\\\\\\\"}\\\"),"
        + "                         \\\"name\\\"))"
        + "                    == \\\"alice\\\"\"}]\n"
        + "  }];\n"
        + "}\n";
    ProtobufSchema schema = new ProtobufSchema(s);
    Descriptor desc = schema.toDescriptor("test.X");
    DynamicMessage msg = DynamicMessage.newBuilder(desc)
        .setField(desc.findFieldByName("anchor"), 1)
        .build();
    assertTrue(schema.validateMessage(new CelValidator(), msg).isEmpty());
  }
}
