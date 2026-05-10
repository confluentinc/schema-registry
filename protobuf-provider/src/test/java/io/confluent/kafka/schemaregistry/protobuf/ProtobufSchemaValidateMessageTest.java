/*
 * Copyright 2026 Confluent Inc.
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

package io.confluent.kafka.schemaregistry.protobuf;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.DynamicMessage;
import io.confluent.kafka.schemaregistry.rules.ValidationRuleError;
import io.confluent.kafka.schemaregistry.rules.ValidationRuleExecutor;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import org.junit.Test;

/**
 * Walker-level tests for {@link ProtobufSchema#validateMessage}. The proto-source
 * parser preserves message-typed extension content (e.g. {@code rules: [{...}]})
 * through the Descriptor round-trip, so the fixtures here are constructed from
 * inline proto-source strings.
 *
 * <p>The stub executor always returns {@code false}, turning every fired rule into a
 * {@link ValidationRuleError} that the test inspects for (rule name, path).
 */
public class ProtobufSchemaValidateMessageTest {

  private static final ValidationRuleExecutor ALWAYS_FAIL =
      (rule, schema, value) -> Boolean.FALSE;

  private static List<String> firedRules(List<ValidationRuleError> errors) {
    List<String> out = new ArrayList<>();
    for (ValidationRuleError e : errors) {
      out.add(e.getRule().getName() + "@" + e.getFieldPath());
    }
    return out;
  }

  @Test
  public void nestedMessage_recursesAndProducesDottedPath() {
    String s = "syntax = \"proto3\";\n"
        + "package test;\n"
        + "import \"confluent/meta.proto\";\n"
        + "message Outer { test.Inner inner = 1; }\n"
        + "message Inner { int32 x = 1 [(confluent.field_meta) = {\n"
        + "  rules: [{name: \"r\", expr: \"true\"}]\n"
        + "}]; }\n";
    ProtobufSchema schema = new ProtobufSchema(s);
    Descriptor outerDesc = schema.toDescriptor("test.Outer");
    Descriptor innerDesc = schema.toDescriptor("test.Inner");
    DynamicMessage inner = DynamicMessage.newBuilder(innerDesc)
        .setField(innerDesc.findFieldByName("x"), 5).build();
    DynamicMessage outer = DynamicMessage.newBuilder(outerDesc)
        .setField(outerDesc.findFieldByName("inner"), inner).build();

    assertEquals(Collections.singletonList("r@inner.x"),
        firedRules(schema.validateMessage(ALWAYS_FAIL, outer)));
  }

  @Test
  public void repeatedMessage_firesPerElementMessageRule() {
    String s = "syntax = \"proto3\";\n"
        + "package test;\n"
        + "import \"confluent/meta.proto\";\n"
        + "message Outer { repeated test.Item items = 1; }\n"
        + "message Item {\n"
        + "  option (confluent.message_meta) = {\n"
        + "    rules: [{name: \"r\", expr: \"true\"}]\n"
        + "  };\n"
        + "  int32 v = 1;\n"
        + "}\n";
    ProtobufSchema schema = new ProtobufSchema(s);
    Descriptor outerDesc = schema.toDescriptor("test.Outer");
    Descriptor itemDesc = schema.toDescriptor("test.Item");
    DynamicMessage i0 = DynamicMessage.newBuilder(itemDesc)
        .setField(itemDesc.findFieldByName("v"), 1).build();
    DynamicMessage i1 = DynamicMessage.newBuilder(itemDesc)
        .setField(itemDesc.findFieldByName("v"), 2).build();
    DynamicMessage outer = DynamicMessage.newBuilder(outerDesc)
        .addRepeatedField(outerDesc.findFieldByName("items"), i0)
        .addRepeatedField(outerDesc.findFieldByName("items"), i1).build();

    assertEquals(Arrays.asList("r@items[0]", "r@items[1]"),
        firedRules(schema.validateMessage(ALWAYS_FAIL, outer)));
  }

  @Test
  public void failFast_stopsAfterFirstViolation() {
    // Same repeated-message shape that without fail-fast produces two violations
    // (one per element). With failFast=true, the walker should stop after the
    // first.
    String s = "syntax = \"proto3\";\n"
        + "package test;\n"
        + "import \"confluent/meta.proto\";\n"
        + "message Outer { repeated test.Item items = 1; }\n"
        + "message Item {\n"
        + "  option (confluent.message_meta) = {\n"
        + "    rules: [{name: \"r\", expr: \"true\"}]\n"
        + "  };\n"
        + "  int32 v = 1;\n"
        + "}\n";
    ProtobufSchema schema = new ProtobufSchema(s);
    Descriptor outerDesc = schema.toDescriptor("test.Outer");
    Descriptor itemDesc = schema.toDescriptor("test.Item");
    DynamicMessage i0 = DynamicMessage.newBuilder(itemDesc)
        .setField(itemDesc.findFieldByName("v"), 1).build();
    DynamicMessage i1 = DynamicMessage.newBuilder(itemDesc)
        .setField(itemDesc.findFieldByName("v"), 2).build();
    DynamicMessage outer = DynamicMessage.newBuilder(outerDesc)
        .addRepeatedField(outerDesc.findFieldByName("items"), i0)
        .addRepeatedField(outerDesc.findFieldByName("items"), i1).build();

    assertEquals(java.util.Collections.singletonList("r@items[0]"),
        firedRules(schema.validateMessage(ALWAYS_FAIL, outer, true)));
  }

  @Test
  public void optionalField_skipsRuleWhenUnset() {
    String s = "syntax = \"proto3\";\n"
        + "package test;\n"
        + "import \"confluent/meta.proto\";\n"
        + "message Outer {\n"
        + "  optional int32 maybe = 1 [(confluent.field_meta) = {\n"
        + "    rules: [{name: \"r\", expr: \"true\"}]\n"
        + "  }];\n"
        + "}\n";
    ProtobufSchema schema = new ProtobufSchema(s);
    Descriptor outerDesc = schema.toDescriptor("test.Outer");

    DynamicMessage unset = DynamicMessage.newBuilder(outerDesc).build();
    assertTrue("Rule must skip when optional field is unset",
        schema.validateMessage(ALWAYS_FAIL, unset).isEmpty());

    DynamicMessage set = DynamicMessage.newBuilder(outerDesc)
        .setField(outerDesc.findFieldByName("maybe"), 42).build();
    assertEquals(Collections.singletonList("r@maybe"),
        firedRules(schema.validateMessage(ALWAYS_FAIL, set)));
  }

  @Test
  public void messageLevelRuleOnNested_firesAtNestedPath() {
    String s = "syntax = \"proto3\";\n"
        + "package test;\n"
        + "import \"confluent/meta.proto\";\n"
        + "message Outer { test.Inner inner = 1; }\n"
        + "message Inner {\n"
        + "  option (confluent.message_meta) = {\n"
        + "    rules: [{name: \"r\", expr: \"true\"}]\n"
        + "  };\n"
        + "  int32 v = 1;\n"
        + "}\n";
    ProtobufSchema schema = new ProtobufSchema(s);
    Descriptor outerDesc = schema.toDescriptor("test.Outer");
    Descriptor innerDesc = schema.toDescriptor("test.Inner");
    DynamicMessage inner = DynamicMessage.newBuilder(innerDesc)
        .setField(innerDesc.findFieldByName("v"), 1).build();
    DynamicMessage outer = DynamicMessage.newBuilder(outerDesc)
        .setField(outerDesc.findFieldByName("inner"), inner).build();

    assertEquals(Collections.singletonList("r@inner"),
        firedRules(schema.validateMessage(ALWAYS_FAIL, outer)));
  }

  @Test
  public void multipleRulesOnSameField_allFire() {
    String s = "syntax = \"proto3\";\n"
        + "package test;\n"
        + "import \"confluent/meta.proto\";\n"
        + "message Outer {\n"
        + "  int32 x = 1 [(confluent.field_meta) = {\n"
        + "    rules: [\n"
        + "      {name: \"r1\", expr: \"true\"},\n"
        + "      {name: \"r2\", expr: \"true\"}\n"
        + "    ]\n"
        + "  }];\n"
        + "}\n";
    ProtobufSchema schema = new ProtobufSchema(s);
    Descriptor outerDesc = schema.toDescriptor("test.Outer");
    DynamicMessage msg = DynamicMessage.newBuilder(outerDesc)
        .setField(outerDesc.findFieldByName("x"), 7).build();

    assertEquals(Arrays.asList("r1@x", "r2@x"),
        firedRules(schema.validateMessage(ALWAYS_FAIL, msg)));
  }
}
