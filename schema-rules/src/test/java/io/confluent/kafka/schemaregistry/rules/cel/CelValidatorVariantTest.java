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
import io.confluent.kafka.schemaregistry.avro.AvroSchema;
import io.confluent.kafka.schemaregistry.protobuf.ProtobufSchema;
import io.confluent.kafka.schemaregistry.rules.ValidationRuleError;
import io.confluent.kafka.schemaregistry.type.Variant;
import io.confluent.kafka.schemaregistry.type.VariantUtils;
import java.nio.ByteBuffer;
import java.util.List;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
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
        .setField(variantDesc.findFieldByName("value"), toByteString(v.getValueBuffer()))
        .setField(variantDesc.findFieldByName("metadata"), toByteString(v.getMetadataBuffer()))
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

  // ---- variants.type, variants.as("string") via variants.field ----

  @Test
  void variantField_asString_passes() throws Exception {
    String s = "syntax = \"proto3\";\n"
        + "package test;\n"
        + "import \"confluent/meta.proto\";\n"
        + "import \"confluent/type/variant.proto\";\n"
        + "message Doc {\n"
        + "  confluent.type.Variant payload = 1 [(confluent.field_meta) = {\n"
        + "    rules: [{name: \"nameIsAlice\","
        + "             expr: \"variants.as("
        + "                       variants.field(variant(this), \\\"name\\\"), \\\"string\\\")"
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
  void variantField_asString_failsOnMismatch() throws Exception {
    String s = "syntax = \"proto3\";\n"
        + "package test;\n"
        + "import \"confluent/meta.proto\";\n"
        + "import \"confluent/type/variant.proto\";\n"
        + "message Doc {\n"
        + "  confluent.type.Variant payload = 1 [(confluent.field_meta) = {\n"
        + "    rules: [{name: \"r\","
        + "             expr: \"variants.as("
        + "                       variants.field(variant(this), \\\"name\\\"), \\\"string\\\")"
        + "                    == \\\"alice\\\"\"}]\n"
        + "  }];\n"
        + "}\n";
    ProtobufSchema schema = new ProtobufSchema(s);
    DynamicMessage doc = docWithVariantJson(schema, "{\"name\":\"bob\"}");
    List<ValidationRuleError> errs = schema.validateMessage(new CelValidator(), doc);
    assertEquals(1, errs.size());
  }

  // ---- variants.path JSONPath navigation ----

  @Test
  void variantsPath_jsonPathNavigation() throws Exception {
    String s = "syntax = \"proto3\";\n"
        + "package test;\n"
        + "import \"confluent/meta.proto\";\n"
        + "import \"confluent/type/variant.proto\";\n"
        + "message Doc {\n"
        + "  confluent.type.Variant payload = 1 [(confluent.field_meta) = {\n"
        + "    rules: [{name: \"r\","
        + "             expr: \"variants.as("
        + "                       variants.path(variant(this), \\\"$.user.name\\\"), \\\"string\\\")"
        + "                    == \\\"alice\\\"\"}]\n"
        + "  }];\n"
        + "}\n";
    ProtobufSchema schema = new ProtobufSchema(s);
    DynamicMessage doc = docWithVariantJson(schema,
        "{\"user\":{\"name\":\"alice\",\"role\":\"admin\"}}");
    assertTrue(schema.validateMessage(new CelValidator(), doc).isEmpty());
  }

  // ---- Null-model orthogonality: CEL null (miss) vs variant-null (explicit JSON null) ----
  //
  // Three distinct predicates under 1b:
  //   result == null              -> path missed (Spark IS NULL equivalent)
  //   variants.isNull(result)     -> path resolved to explicit JSON null
  //                                  (Spark is_variant_null equivalent)
  //   variants.type(result)       -> CEL null for miss, "null" for variant-null,
  //                                  otherwise the type label

  @Test
  void variantsField_missingField_returnsCelNull() throws Exception {
    // Missing field -> CEL null. Detect via `... == null`.
    String s = "syntax = \"proto3\";\n"
        + "package test;\n"
        + "import \"confluent/meta.proto\";\n"
        + "import \"confluent/type/variant.proto\";\n"
        + "message Doc {\n"
        + "  confluent.type.Variant payload = 1 [(confluent.field_meta) = {\n"
        + "    rules: [{name: \"r\","
        + "             expr: \"variants.field(variant(this), \\\"missing\\\")"
        + "                    == null\"}]\n"
        + "  }];\n"
        + "}\n";
    ProtobufSchema schema = new ProtobufSchema(s);
    DynamicMessage doc = docWithVariantJson(schema, "{\"name\":\"alice\"}");
    assertTrue(schema.validateMessage(new CelValidator(), doc).isEmpty());
  }

  @Test
  void variantsIsNull_onMissingField_returnsFalse() throws Exception {
    // 1b strict-Spark: missing field produces CEL null, NOT variant-null.
    // variants.isNull(cel_null) is false (input isn't a Variant).
    String s = "syntax = \"proto3\";\n"
        + "package test;\n"
        + "import \"confluent/meta.proto\";\n"
        + "import \"confluent/type/variant.proto\";\n"
        + "message Doc {\n"
        + "  confluent.type.Variant payload = 1 [(confluent.field_meta) = {\n"
        + "    rules: [{name: \"r\","
        + "             expr: \"!variants.isNull("
        + "                       variants.field(variant(this), \\\"missing\\\"))\"}]\n"
        + "  }];\n"
        + "}\n";
    ProtobufSchema schema = new ProtobufSchema(s);
    DynamicMessage doc = docWithVariantJson(schema, "{\"name\":\"alice\"}");
    assertTrue(schema.validateMessage(new CelValidator(), doc).isEmpty());
  }

  @Test
  void variantsIsNull_onNonNullVariant_returnsFalse() throws Exception {
    String s = "syntax = \"proto3\";\n"
        + "package test;\n"
        + "import \"confluent/meta.proto\";\n"
        + "import \"confluent/type/variant.proto\";\n"
        + "message Doc {\n"
        + "  confluent.type.Variant payload = 1 [(confluent.field_meta) = {\n"
        + "    rules: [{name: \"r\","
        + "             expr: \"!variants.isNull(variant(this))\"}]\n"
        + "  }];\n"
        + "}\n";
    ProtobufSchema schema = new ProtobufSchema(s);
    DynamicMessage doc = docWithVariantJson(schema, "{\"name\":\"alice\"}");
    assertTrue(schema.validateMessage(new CelValidator(), doc).isEmpty());
  }

  @Test
  void variantsIsNull_onExplicitJsonNull_returnsTrue() throws Exception {
    // The variant payload itself encodes JSON null — top-level variant has
    // type=NULL. variants.isNull reports true (matches Spark is_variant_null).
    String s = "syntax = \"proto3\";\n"
        + "package test;\n"
        + "import \"confluent/meta.proto\";\n"
        + "import \"confluent/type/variant.proto\";\n"
        + "message Doc {\n"
        + "  confluent.type.Variant payload = 1 [(confluent.field_meta) = {\n"
        + "    rules: [{name: \"r\","
        + "             expr: \"variants.isNull(variant(this))\"}]\n"
        + "  }];\n"
        + "}\n";
    ProtobufSchema schema = new ProtobufSchema(s);
    DynamicMessage doc = docWithVariantJson(schema, "null");
    assertTrue(schema.validateMessage(new CelValidator(), doc).isEmpty());
  }

  @Test
  void variantsNullModel_orthogonality_threePredicatesDistinguishCases() throws Exception {
    // Locks in 1b's three orthogonal predicates:
    //   - `result == null`         => path missed (CEL null)
    //   - `variants.isNull(result)` => path resolved to explicit JSON null
    //   - both false              => path resolved to a real (non-null) value
    // Each scenario in the payload has a unique signature across the three
    // predicates. Constructs one combined rule to assert all 9 outcomes
    // at once.
    String s = "syntax = \"proto3\";\n"
        + "package test;\n"
        + "import \"confluent/meta.proto\";\n"
        + "import \"confluent/type/variant.proto\";\n"
        + "message Doc {\n"
        + "  confluent.type.Variant payload = 1 [(confluent.field_meta) = {\n"
        + "    rules: [{name: \"r\","
        + "             expr: \""
        // Missing field: == null TRUE, isNull FALSE
        + "      variants.field(variant(this), \\\"missing\\\") == null"
        + "      && !variants.isNull(variants.field(variant(this), \\\"missing\\\"))"
        // Explicit JSON null: == null FALSE, isNull TRUE
        + "      && !(variants.field(variant(this), \\\"explicit\\\") == null)"
        + "      && variants.isNull(variants.field(variant(this), \\\"explicit\\\"))"
        // Real value: == null FALSE, isNull FALSE
        + "      && !(variants.field(variant(this), \\\"real\\\") == null)"
        + "      && !variants.isNull(variants.field(variant(this), \\\"real\\\"))"
        + "\"}]\n"
        + "  }];\n"
        + "}\n";
    ProtobufSchema schema = new ProtobufSchema(s);
    DynamicMessage doc = docWithVariantJson(schema,
        "{\"explicit\": null, \"real\": \"hello\"}");
    List<ValidationRuleError> errs = schema.validateMessage(new CelValidator(), doc);
    assertTrue(errs.isEmpty(), "got: " + errs + " causes: " + dumpCauses(errs));
  }

  @Test
  void variantsChain_celNullPropagatesThroughEveryVariantsFunction() throws Exception {
    // Lock in the load-bearing chain-composition guarantee: a CEL-null
    // intermediate (produced by a navigation miss) propagates correctly
    // through the Variant-returning variants.* functions. Each conjunct
    // exercises one of the four functions whose CEL-null-input handling
    // wasn't otherwise covered by existing tests:
    //
    //   variants.field   — chained navigation (3-deep)
    //   variants.path    — JSONPath on null
    //   variants.elem    — array indexing on null
    //   variants.tryAs   — soft extraction, propagates to CEL null
    //
    // variants.type and variants.toJson are NOT included — they return
    // STRING and there's no `string == null` overload in cel-java. The
    // contract for those is "real Variant in, string out; check the source
    // with `v == null` upstream, don't call them on a possibly-null value."
    //
    // If any function throws or returns a non-null value when its first arg
    // is CEL null, the rule fails and the assertion catches the regression.
    String s = "syntax = \"proto3\";\n"
        + "package test;\n"
        + "import \"confluent/meta.proto\";\n"
        + "import \"confluent/type/variant.proto\";\n"
        + "message Doc {\n"
        + "  confluent.type.Variant payload = 1 [(confluent.field_meta) = {\n"
        + "    rules: [{name: \"r\","
        + "             expr: \""
        + "      variants.field(variants.field(variant(this), \\\"missing\\\"),"
        + "                     \\\"x\\\") == null"
        + "      && variants.path(variants.field(variant(this), \\\"missing\\\"),"
        + "                       \\\"$.x\\\") == null"
        + "      && variants.elem(variants.field(variant(this), \\\"missing\\\"),"
        + "                       0) == null"
        + "      && variants.tryAs(variants.field(variant(this), \\\"missing\\\"),"
        + "                        \\\"string\\\") == null"
        + "\"}]\n"
        + "  }];\n"
        + "}\n";
    ProtobufSchema schema = new ProtobufSchema(s);
    DynamicMessage doc = docWithVariantJson(schema, "{\"name\":\"alice\"}");
    List<ValidationRuleError> errs = schema.validateMessage(new CelValidator(), doc);
    assertTrue(errs.isEmpty(), "got: " + errs + " causes: " + dumpCauses(errs));
  }

  // ---- B1: navigation on wrong-type receiver → CEL null (miss) ----
  //
  // Variant.getFieldByKey throws IllegalArgumentException for non-OBJECT receivers
  // (and getElementAtIndex for non-ARRAY) — the bindings type-check defensively
  // so wrong-type-receiver counts as a "miss" and produces CEL null.

  @Test
  void variantField_onNonObjectReceiver_returnsCelNull() throws Exception {
    String s = "syntax = \"proto3\";\n"
        + "package test;\n"
        + "import \"confluent/meta.proto\";\n"
        + "import \"confluent/type/variant.proto\";\n"
        + "message Doc {\n"
        + "  confluent.type.Variant payload = 1 [(confluent.field_meta) = {\n"
        + "    rules: [{name: \"r\","
        + "             expr: \"variants.field(variant(this), \\\"x\\\") == null\"}]\n"
        + "  }];\n"
        + "}\n";
    ProtobufSchema schema = new ProtobufSchema(s);
    // Top-level variant is an INT (Variant.Type.INT), not OBJECT.
    DynamicMessage doc = docWithVariantJson(schema, "42");
    List<ValidationRuleError> errs = schema.validateMessage(new CelValidator(), doc);
    assertTrue(errs.isEmpty(), "got: " + errs + " causes: " + dumpCauses(errs));
  }

  @Test
  void variantElem_onNonArrayReceiver_returnsCelNull() throws Exception {
    String s = "syntax = \"proto3\";\n"
        + "package test;\n"
        + "import \"confluent/meta.proto\";\n"
        + "import \"confluent/type/variant.proto\";\n"
        + "message Doc {\n"
        + "  confluent.type.Variant payload = 1 [(confluent.field_meta) = {\n"
        + "    rules: [{name: \"r\","
        + "             expr: \"variants.elem(variant(this), 0) == null\"}]\n"
        + "  }];\n"
        + "}\n";
    ProtobufSchema schema = new ProtobufSchema(s);
    // Top-level variant is a STRING, not ARRAY.
    DynamicMessage doc = docWithVariantJson(schema, "\"hello\"");
    List<ValidationRuleError> errs = schema.validateMessage(new CelValidator(), doc);
    assertTrue(errs.isEmpty(), "got: " + errs + " causes: " + dumpCauses(errs));
  }

  @Test
  void variantElem_indexOverflowsInt_returnsCelNull() throws Exception {
    // CEL ints are i64. Without a range check, a Long like 4_294_967_296 would
    // intValue() to 0 (lower 32 bits) and silently match the first element.
    // The binding must treat any out-of-int-range index as out-of-bounds
    // navigation → CEL null.
    String s = "syntax = \"proto3\";\n"
        + "package test;\n"
        + "import \"confluent/meta.proto\";\n"
        + "import \"confluent/type/variant.proto\";\n"
        + "message Doc {\n"
        + "  confluent.type.Variant payload = 1 [(confluent.field_meta) = {\n"
        + "    rules: [{name: \"r\","
        + "             expr: \"variants.elem(variant(this), 4294967296) == null\"}]\n"
        + "  }];\n"
        + "}\n";
    ProtobufSchema schema = new ProtobufSchema(s);
    // 3-element array; index 4_294_967_296 (= 2^32) overflows int and would
    // silently wrap to 0 without the range check.
    DynamicMessage doc = docWithVariantJson(schema, "[1, 2, 3]");
    List<ValidationRuleError> errs = schema.validateMessage(new CelValidator(), doc);
    assertTrue(errs.isEmpty(), "got: " + errs + " causes: " + dumpCauses(errs));
  }

  @Test
  void variantElem_outOfBoundsIndex_returnsCelNull() throws Exception {
    // Variant.getElementAtIndex returns null (not throws) for indices outside
    // [0, arraySize) — our binding maps that null to CEL null.
    String s = "syntax = \"proto3\";\n"
        + "package test;\n"
        + "import \"confluent/meta.proto\";\n"
        + "import \"confluent/type/variant.proto\";\n"
        + "message Doc {\n"
        + "  confluent.type.Variant payload = 1 [(confluent.field_meta) = {\n"
        + "    rules: [{name: \"r\","
        + "             expr: \"variants.elem(variant(this), 99) == null\"}]\n"
        + "  }];\n"
        + "}\n";
    ProtobufSchema schema = new ProtobufSchema(s);
    // 3-element array; index 99 is well past the end.
    DynamicMessage doc = docWithVariantJson(schema, "[1, 2, 3]");
    List<ValidationRuleError> errs = schema.validateMessage(new CelValidator(), doc);
    assertTrue(errs.isEmpty(), "got: " + errs + " causes: " + dumpCauses(errs));
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

  // ---- variants.as("int"): cross-type — variant integer flows into CEL int arithmetic ----

  @Test
  void variantAsInt_thenArithmetic() throws Exception {
    // Jackson parses JSON `42` as Int → Variant.Type.INT. variants.as(_, "int")
    // extracts it as a CEL int (long). The rule then compares against an int literal.
    String s = "syntax = \"proto3\";\n"
        + "package test;\n"
        + "import \"confluent/meta.proto\";\n"
        + "import \"confluent/type/variant.proto\";\n"
        + "message Doc {\n"
        + "  confluent.type.Variant payload = 1 [(confluent.field_meta) = {\n"
        + "    rules: [{name: \"r\","
        + "             expr: \"variants.as("
        + "                       variants.field(variant(this), \\\"count\\\"), \\\"int\\\") >= 0\"}]\n"
        + "  }];\n"
        + "}\n";
    ProtobufSchema schema = new ProtobufSchema(s);
    DynamicMessage doc = docWithVariantJson(schema, "{\"count\":42}");
    List<ValidationRuleError> errs = schema.validateMessage(new CelValidator(), doc);
    assertTrue(errs.isEmpty(), "got: " + errs + " causes: " + dumpCauses(errs));
  }

  // ---- variants.path("$"): identity selector returns input unchanged ----

  @Test
  void variantsPath_dollarOnScalarRoot_returnsSameVariant() throws Exception {
    // $ is the JSONPath identity selector — should return the root variant
    // unchanged, regardless of whether the root is OBJECT, ARRAY, or a scalar
    // type. Matches Spark variant_get(v, '$') semantics. Confirm by composing
    // with variants.as on a top-level INT variant: extraction succeeds.
    String s = "syntax = \"proto3\";\n"
        + "package test;\n"
        + "import \"confluent/meta.proto\";\n"
        + "import \"confluent/type/variant.proto\";\n"
        + "message Doc {\n"
        + "  confluent.type.Variant payload = 1 [(confluent.field_meta) = {\n"
        + "    rules: [{name: \"r\","
        + "             expr: \"variants.as("
        + "                       variants.path(variant(this), \\\"$\\\"),"
        + "                       \\\"int\\\")"
        + "                    == 42\"}]\n"
        + "  }];\n"
        + "}\n";
    ProtobufSchema schema = new ProtobufSchema(s);
    // Top-level variant is an INT (Variant.Type.INT), not OBJECT or ARRAY.
    DynamicMessage doc = docWithVariantJson(schema, "42");
    List<ValidationRuleError> errs = schema.validateMessage(new CelValidator(), doc);
    assertTrue(errs.isEmpty(), "got: " + errs + " causes: " + dumpCauses(errs));
  }

  // ---- variants.* on non-Variant input: clear IAE, not ClassCastException ----

  @Test
  void variantsField_onNonVariantInput_rejectedAtCompileTime() throws Exception {
    // The Variant-typed receiver lets the type checker reject obviously-wrong
    // calls at rule compilation time. A rule author who passes a literal
    // string by mistake gets a "no matching overload" / type-mismatch error
    // before the rule ever runs, rather than only finding out at evaluation
    // time. The error surfaces as a RuleException at validateMessage time
    // because compilation is lazy (deferred until first evaluation).
    String s = "syntax = \"proto3\";\n"
        + "package test;\n"
        + "import \"confluent/meta.proto\";\n"
        + "message X {\n"
        + "  int32 anchor = 1 [(confluent.field_meta) = {\n"
        + "    rules: [{name: \"r\","
        + "             expr: \"variants.field(\\\"hello\\\", \\\"x\\\") == null\"}]\n"
        + "  }];\n"
        + "}\n";
    ProtobufSchema schema = new ProtobufSchema(s);
    Descriptor desc = schema.toDescriptor("test.X");
    DynamicMessage msg = DynamicMessage.newBuilder(desc)
        .setField(desc.findFieldByName("anchor"), 1)
        .build();
    List<ValidationRuleError> errs = schema.validateMessage(new CelValidator(), msg);
    assertEquals(1, errs.size(),
        "expected non-Variant input to be rejected; got: " + errs);
    // cel-java's type checker reports either "no matching overload" or
    // "found no matching overload" depending on version. Match either form.
    String causes = dumpCauses(errs);
    assertTrue(
        causes.contains("no matching overload")
            || causes.contains("found no matching overload"),
        "expected compile-time overload-mismatch error; got causes: " + causes);
  }

  // ---- variants.tryAs — type-mismatch returns CEL null ----

  @Test
  void variantTryAsString_onIntVariantReturnsNull() throws Exception {
    String s = "syntax = \"proto3\";\n"
        + "package test;\n"
        + "import \"confluent/meta.proto\";\n"
        + "import \"confluent/type/variant.proto\";\n"
        + "message Doc {\n"
        + "  confluent.type.Variant payload = 1 [(confluent.field_meta) = {\n"
        + "    rules: [{name: \"r\","
        + "             expr: \"variants.tryAs("
        + "                       variants.field(variant(this), \\\"count\\\"), \\\"string\\\") == null\"}]\n"
        + "  }];\n"
        + "}\n";
    ProtobufSchema schema = new ProtobufSchema(s);
    DynamicMessage doc = docWithVariantJson(schema, "{\"count\":42}");
    assertTrue(schema.validateMessage(new CelValidator(), doc).isEmpty());
  }

  // ---- variants.tryParseJson(s): soft JSON parse (Spark try_parse_json) ----

  @Test
  void variantsTryParseJson_onInvalidReturnsCelNull() throws Exception {
    // variants.tryParseJson(invalidJsonString) returns CEL null instead of
    // throwing. Matches Spark try_parse_json(s) semantics.
    String s = "syntax = \"proto3\";\n"
        + "package test;\n"
        + "import \"confluent/meta.proto\";\n"
        + "message X {\n"
        + "  int32 anchor = 1 [(confluent.field_meta) = {\n"
        + "    rules: [{name: \"r\","
        + "             expr: \"variants.tryParseJson(\\\"this is not json\\\") == null\"}]\n"
        + "  }];\n"
        + "}\n";
    ProtobufSchema schema = new ProtobufSchema(s);
    Descriptor desc = schema.toDescriptor("test.X");
    DynamicMessage msg = DynamicMessage.newBuilder(desc)
        .setField(desc.findFieldByName("anchor"), 1)
        .build();
    assertTrue(schema.validateMessage(new CelValidator(), msg).isEmpty());
  }

  @Test
  void variantsTryParseJson_onValidStillParses() throws Exception {
    // variants.tryParseJson(validJson) parses normally; soft-fail only
    // affects the error path.
    String s = "syntax = \"proto3\";\n"
        + "package test;\n"
        + "import \"confluent/meta.proto\";\n"
        + "message X {\n"
        + "  int32 anchor = 1 [(confluent.field_meta) = {\n"
        + "    rules: [{name: \"r\","
        + "             expr: \"variants.as(variants.tryParseJson(\\\"42\\\"), \\\"int\\\") == 42\"}]\n"
        + "  }];\n"
        + "}\n";
    ProtobufSchema schema = new ProtobufSchema(s);
    Descriptor desc = schema.toDescriptor("test.X");
    DynamicMessage msg = DynamicMessage.newBuilder(desc)
        .setField(desc.findFieldByName("anchor"), 1)
        .build();
    assertTrue(schema.validateMessage(new CelValidator(), msg).isEmpty());
  }

  @Test
  void variantsParseJson_strictMode_throwsOnInvalid() throws Exception {
    // variants.parseJson(invalidJsonString) without flag should throw.
    // We assert by registering a rule that would only fire if parse threw
    // (i.e., the rule body never evaluates to true because the inner call
    // raises before producing a value).
    String s = "syntax = \"proto3\";\n"
        + "package test;\n"
        + "import \"confluent/meta.proto\";\n"
        + "message X {\n"
        + "  int32 anchor = 1 [(confluent.field_meta) = {\n"
        + "    rules: [{name: \"r\","
        + "             expr: \"variants.parseJson(\\\"not json\\\") == null\"}]\n"
        + "  }];\n"
        + "}\n";
    ProtobufSchema schema = new ProtobufSchema(s);
    Descriptor desc = schema.toDescriptor("test.X");
    DynamicMessage msg = DynamicMessage.newBuilder(desc)
        .setField(desc.findFieldByName("anchor"), 1)
        .build();
    // Strict path throws, surfaces as a non-empty errors list.
    List<ValidationRuleError> errs = schema.validateMessage(new CelValidator(), msg);
    assertEquals(1, errs.size(), "expected strict parse to throw; got: "
        + errs + " causes: " + dumpCauses(errs));
  }

  // ---- Composed navigation + typed extraction: as(field(v, k), t) ----

  @Test
  void composedFieldAs_strict_returnsTypedValue() throws Exception {
    // Composition pattern; equivalent to Spark variant_get(v, '$.k', 'string').
    String s = "syntax = \"proto3\";\n"
        + "package test;\n"
        + "import \"confluent/meta.proto\";\n"
        + "import \"confluent/type/variant.proto\";\n"
        + "message Doc {\n"
        + "  confluent.type.Variant payload = 1 [(confluent.field_meta) = {\n"
        + "    rules: [{name: \"r\","
        + "             expr: \"variants.as("
        + "                       variants.field(variant(this), \\\"name\\\"),"
        + "                       \\\"string\\\")"
        + "                    == \\\"alice\\\"\"}]\n"
        + "  }];\n"
        + "}\n";
    ProtobufSchema schema = new ProtobufSchema(s);
    DynamicMessage doc = docWithVariantJson(schema, "{\"name\":\"alice\"}");
    assertTrue(schema.validateMessage(new CelValidator(), doc).isEmpty());
  }

  @Test
  void composedFieldAs_strict_onMissingFieldReturnsCelNull() throws Exception {
    // Composition: variants.as propagates the CEL-null result of a missing
    // navigation, so the whole expression yields CEL null.
    String s = "syntax = \"proto3\";\n"
        + "package test;\n"
        + "import \"confluent/meta.proto\";\n"
        + "import \"confluent/type/variant.proto\";\n"
        + "message Doc {\n"
        + "  confluent.type.Variant payload = 1 [(confluent.field_meta) = {\n"
        + "    rules: [{name: \"r\","
        + "             expr: \"variants.as("
        + "                       variants.field(variant(this), \\\"missing\\\"),"
        + "                       \\\"string\\\")"
        + "                    == null\"}]\n"
        + "  }];\n"
        + "}\n";
    ProtobufSchema schema = new ProtobufSchema(s);
    DynamicMessage doc = docWithVariantJson(schema, "{\"name\":\"alice\"}");
    assertTrue(schema.validateMessage(new CelValidator(), doc).isEmpty());
  }

  @Test
  void composedFieldTryAs_onTypeMismatchReturnsCelNull() throws Exception {
    // Composition with tryAs; equivalent to Spark try_variant_get(v, '$.k', 'string').
    String s = "syntax = \"proto3\";\n"
        + "package test;\n"
        + "import \"confluent/meta.proto\";\n"
        + "import \"confluent/type/variant.proto\";\n"
        + "message Doc {\n"
        + "  confluent.type.Variant payload = 1 [(confluent.field_meta) = {\n"
        + "    rules: [{name: \"r\","
        + "             expr: \"variants.tryAs("
        + "                       variants.field(variant(this), \\\"count\\\"),"
        + "                       \\\"string\\\")"
        + "                    == null\"}]\n"
        + "  }];\n"
        + "}\n";
    ProtobufSchema schema = new ProtobufSchema(s);
    DynamicMessage doc = docWithVariantJson(schema, "{\"count\":42}");
    assertTrue(schema.validateMessage(new CelValidator(), doc).isEmpty());
  }

  // ---- variants.toJson(v): JSON serialization ----

  @Test
  void variantsToJson_serializesPayload() throws Exception {
    String s = "syntax = \"proto3\";\n"
        + "package test;\n"
        + "import \"confluent/meta.proto\";\n"
        + "import \"confluent/type/variant.proto\";\n"
        + "message Doc {\n"
        + "  confluent.type.Variant payload = 1 [(confluent.field_meta) = {\n"
        + "    rules: [{name: \"r\","
        + "             expr: \"variants.toJson(variant(this))"
        + "                    == \\\"{\\\\\\\"name\\\\\\\":\\\\\\\"alice\\\\\\\"}\\\"\"}]\n"
        + "  }];\n"
        + "}\n";
    ProtobufSchema schema = new ProtobufSchema(s);
    DynamicMessage doc = docWithVariantJson(schema, "{\"name\":\"alice\"}");
    assertTrue(schema.validateMessage(new CelValidator(), doc).isEmpty());
  }

  @Test
  void variantsToJson_callerGuardsForCelNullUpstream() throws Exception {
    // variants.toJson(Variant) -> string contract: serialization always
    // produces a string for a real Variant. CEL null has no JSON
    // representation; the natural pattern is to check the source variable
    // upstream with `v == null` rather than relying on a workaround on the
    // toJson return type. This test exercises that idiomatic pattern: when
    // the navigated value is missing (CEL null), the ternary's first branch
    // skips toJson and returns the default; when present, toJson serializes.
    String s = "syntax = \"proto3\";\n"
        + "package test;\n"
        + "import \"confluent/meta.proto\";\n"
        + "import \"confluent/type/variant.proto\";\n"
        + "message Doc {\n"
        + "  confluent.type.Variant payload = 1 [(confluent.field_meta) = {\n"
        + "    rules: [{name: \"r\","
        + "             expr: \""
        // Missing path: ternary picks "absent" without calling toJson.
        + "      (variants.field(variant(this), \\\"missing\\\") == null"
        + "         ? \\\"absent\\\""
        + "         : variants.toJson(variants.field(variant(this), \\\"missing\\\")))"
        + "      == \\\"absent\\\""
        // Present path: ternary picks the toJson branch.
        + "      && (variants.field(variant(this), \\\"name\\\") == null"
        + "            ? \\\"absent\\\""
        + "            : variants.toJson(variants.field(variant(this), \\\"name\\\")))"
        + "         == \\\"\\\\\\\"alice\\\\\\\"\\\""
        + "\"}]\n"
        + "  }];\n"
        + "}\n";
    ProtobufSchema schema = new ProtobufSchema(s);
    DynamicMessage doc = docWithVariantJson(schema, "{\"name\":\"alice\"}");
    List<ValidationRuleError> errs = schema.validateMessage(new CelValidator(), doc);
    assertTrue(errs.isEmpty(), "got: " + errs + " causes: " + dumpCauses(errs));
  }

  @Test
  void variant_onCelNullInput_returnsCelNull() throws Exception {
    // variant(cel_null) propagates to CEL null. The variantConstructor
    // helper explicitly short-circuits on isCelNull(o), but no test
    // exercised that branch — a future refactor could silently break the
    // propagation. We feed CEL null in via variants.tryParseJson on an
    // invalid string (returns CEL null at runtime, statically typed Variant).
    String s = "syntax = \"proto3\";\n"
        + "package test;\n"
        + "import \"confluent/meta.proto\";\n"
        + "message X {\n"
        + "  int32 anchor = 1 [(confluent.field_meta) = {\n"
        + "    rules: [{name: \"r\","
        + "             expr: \"variant(variants.tryParseJson(\\\"not json\\\"))"
        + "                    == null\"}]\n"
        + "  }];\n"
        + "}\n";
    ProtobufSchema schema = new ProtobufSchema(s);
    Descriptor desc = schema.toDescriptor("test.X");
    DynamicMessage msg = DynamicMessage.newBuilder(desc)
        .setField(desc.findFieldByName("anchor"), 1)
        .build();
    List<ValidationRuleError> errs = schema.validateMessage(new CelValidator(), msg);
    assertTrue(errs.isEmpty(), "got: " + errs + " causes: " + dumpCauses(errs));
  }

  // ---- JSON-literal: variants.parseJson("...") ----

  @Test
  void variantsParseJsonLiteral_parses() throws Exception {
    String s = "syntax = \"proto3\";\n"
        + "package test;\n"
        + "import \"confluent/meta.proto\";\n"
        + "message X {\n"
        + "  int32 anchor = 1 [(confluent.field_meta) = {\n"
        + "    rules: [{name: \"r\","
        + "             expr: \"variants.as("
        + "                       variants.field("
        + "                         variants.parseJson(\\\"{\\\\\\\"name\\\\\\\":\\\\\\\"alice\\\\\\\"}\\\"),"
        + "                         \\\"name\\\"), \\\"string\\\")"
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

  // ---- Avro variant logical type ----

  /**
   * Schema with a variant logical-type field. The variant logical type is a
   * record with {@code metadata: bytes} and {@code value: bytes} fields,
   * tagged with {@code "logicalType": "variant"}. CelValidator's walker hands
   * the inner record value to {@code variant(this)}, where
   * {@code VariantUtils.toVariant(IndexedRecord)} routes through
   * {@code VariantConversion.fromRecord}.
   */
  private static final String AVRO_VARIANT_SCHEMA = ""
      + "{"
      + "  \"type\":\"record\","
      + "  \"name\":\"Doc\","
      + "  \"namespace\":\"test\","
      + "  \"fields\":["
      + "    {"
      + "      \"name\":\"payload\","
      + "      \"type\":{"
      + "        \"type\":\"record\","
      + "        \"name\":\"VariantRecord\","
      + "        \"logicalType\":\"variant\","
      + "        \"fields\":["
      + "          {\"name\":\"metadata\",\"type\":\"bytes\"},"
      + "          {\"name\":\"value\",\"type\":\"bytes\"}"
      + "        ]"
      + "      },"
      + "      \"confluent:rules\":["
      + "        {\"name\":\"nameIsAlice\","
      + "         \"expr\":\"variants.as(variants.field(variant(this), \\\"name\\\"), \\\"string\\\") == \\\"alice\\\"\"}]"
      + "    }"
      + "  ]"
      + "}";

  /** Build a Doc Avro record whose payload is the variant-encoded form of {@code json}. */
  private static GenericRecord avroDocWithVariantJson(String json) throws Exception {
    AvroSchema avro = new AvroSchema(AVRO_VARIANT_SCHEMA);
    org.apache.avro.Schema docSchema = avro.rawSchema();
    org.apache.avro.Schema variantSchema = docSchema.getField("payload").schema();

    Variant v = VariantUtils.fromJsonNode(MAPPER.readTree(json));
    GenericRecord variantRec = new GenericData.Record(variantSchema);
    variantRec.put("metadata", v.getMetadataBuffer());
    variantRec.put("value", v.getValueBuffer());

    GenericRecord doc = new GenericData.Record(docSchema);
    doc.put("payload", variantRec);
    return doc;
  }

  @Test
  void avroVariant_indexedRecordRoutesThroughConversion_passes() throws Exception {
    AvroSchema schema = new AvroSchema(AVRO_VARIANT_SCHEMA);
    GenericRecord doc = avroDocWithVariantJson("{\"name\":\"alice\"}");
    List<ValidationRuleError> errs = schema.validateMessage(new CelValidator(), doc);
    assertTrue(errs.isEmpty(), "got: " + errs + " causes: " + dumpCauses(errs));
  }

  @Test
  void avroVariant_mismatchFails() throws Exception {
    AvroSchema schema = new AvroSchema(AVRO_VARIANT_SCHEMA);
    GenericRecord doc = avroDocWithVariantJson("{\"name\":\"bob\"}");
    List<ValidationRuleError> errs = schema.validateMessage(new CelValidator(), doc);
    assertEquals(1, errs.size());
    assertEquals("nameIsAlice", errs.get(0).getRule().getName());
  }

  @Test
  void avroVariant_missingField_returnsCelNull() throws Exception {
    String s = ""
        + "{"
        + "  \"type\":\"record\","
        + "  \"name\":\"Doc\","
        + "  \"namespace\":\"test\","
        + "  \"fields\":["
        + "    {"
        + "      \"name\":\"payload\","
        + "      \"type\":{"
        + "        \"type\":\"record\","
        + "        \"name\":\"VariantRecord\","
        + "        \"logicalType\":\"variant\","
        + "        \"fields\":["
        + "          {\"name\":\"metadata\",\"type\":\"bytes\"},"
        + "          {\"name\":\"value\",\"type\":\"bytes\"}"
        + "        ]"
        + "      },"
        + "      \"confluent:rules\":["
        + "        {\"name\":\"r\","
        + "         \"expr\":\"variants.field(variant(this), \\\"missing\\\") == null\"}]"
        + "    }"
        + "  ]"
        + "}";
    AvroSchema schema = new AvroSchema(s);
    org.apache.avro.Schema docSchema = schema.rawSchema();
    org.apache.avro.Schema variantSchema = docSchema.getField("payload").schema();

    Variant v = VariantUtils.fromJsonNode(MAPPER.readTree("{\"name\":\"alice\"}"));
    GenericRecord variantRec = new GenericData.Record(variantSchema);
    variantRec.put("metadata", v.getMetadataBuffer());
    variantRec.put("value", v.getValueBuffer());
    GenericRecord doc = new GenericData.Record(docSchema);
    doc.put("payload", variantRec);

    List<ValidationRuleError> errs = schema.validateMessage(new CelValidator(), doc);
    assertTrue(errs.isEmpty(), "got: " + errs + " causes: " + dumpCauses(errs));
  }
}
