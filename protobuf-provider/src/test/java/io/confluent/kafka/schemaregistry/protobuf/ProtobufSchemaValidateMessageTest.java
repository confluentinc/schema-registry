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
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

import com.google.protobuf.ByteString;
import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.Descriptors.FieldDescriptor;
import com.google.protobuf.DynamicMessage;
import com.google.protobuf.Message;
import io.confluent.kafka.schemaregistry.rules.ValidationRuleError;
import io.confluent.kafka.schemaregistry.rules.ValidationRuleExecutor;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import org.apache.kafka.common.errors.SerializationException;
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

  /**
   * Stands in for a CEL environment built from the registered schema: it can only read the
   * value if the message it is handed names its fields the way the schema does. Mirrors
   * {@code this.renamed == 'x'}.
   */
  private static final ValidationRuleExecutor SCHEMA_NAMED = (rule, schema, value) -> {
    Message evaluated = (Message) value;
    FieldDescriptor fd = evaluated.getDescriptorForType().findFieldByName("renamed");
    return fd != null && "x".equals(evaluated.getField(fd));
  };

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

  @Test
  public void unsetPresenceField_isSkippedEvenWhenTheSchemaDoesNotTrackPresence() {
    // Moving a field out of a oneof is a compatible change, so the registered schema can
    // declare a plain scalar where the producer's class still tracks presence. The walk
    // reads the message through the registered descriptor, but whether a value is absent
    // is the producer's question: the field was never set, so its rule must not fire on a
    // synthesized default.
    String registered = "syntax = \"proto3\";\n"
        + "package test;\n"
        + "import \"confluent/meta.proto\";\n"
        + "message M {\n"
        + "  string name = 1 [(confluent.field_meta) = {\n"
        + "    rules: [{name: \"r\", expr: \"true\"}]\n"
        + "  }];\n"
        + "}\n";
    String producer = "syntax = \"proto3\";\n"
        + "package test;\n"
        + "message M { oneof g { string name = 1; } }\n";
    ProtobufSchema schema = new ProtobufSchema(registered);
    Descriptor producerDesc = new ProtobufSchema(producer).toDescriptor("test.M");

    DynamicMessage unset = DynamicMessage.newBuilder(producerDesc).build();
    assertTrue("Rule must skip a field the producer left unset",
        schema.validateMessage(ALWAYS_FAIL, unset).isEmpty());

    DynamicMessage set = DynamicMessage.newBuilder(producerDesc)
        .setField(producerDesc.findFieldByName("name"), "alice").build();
    assertEquals(Collections.singletonList("r@name"),
        firedRules(schema.validateMessage(ALWAYS_FAIL, set)));
  }

  @Test
  public void defaultValuedField_stillFiresWhenOnlyTheSchemaTracksPresence() {
    // The mirror case: the registered schema tracks presence where the producer's class
    // does not. A plain proto3 scalar holding "" is indistinguishable from unset on the
    // wire, but the producer's class cannot mark it absent, so the rule must still run on
    // the value it has — taking the schema's view would silently stop enforcing it.
    String registered = "syntax = \"proto3\";\n"
        + "package test;\n"
        + "import \"confluent/meta.proto\";\n"
        + "message M {\n"
        + "  oneof g {\n"
        + "    string name = 1 [(confluent.field_meta) = {\n"
        + "      rules: [{name: \"r\", expr: \"true\"}]\n"
        + "    }];\n"
        + "  }\n"
        + "}\n";
    String producer = "syntax = \"proto3\";\n"
        + "package test;\n"
        + "message M { string name = 1; }\n";
    ProtobufSchema schema = new ProtobufSchema(registered);
    Descriptor producerDesc = new ProtobufSchema(producer).toDescriptor("test.M");

    DynamicMessage msg = DynamicMessage.newBuilder(producerDesc)
        .setField(producerDesc.findFieldByName("name"), "").build();
    assertEquals(Collections.singletonList("r@name"),
        firedRules(schema.validateMessage(ALWAYS_FAIL, msg)));
  }

  @Test
  public void messageUnreadableThroughTheSchema_failsInTheSerializationChannel() {
    // bytes -> string is a compatible change, so a producer writing non-UTF-8 bytes can
    // meet a registered schema that declares a string. Those bytes cannot be read through
    // the registered schema — a consumer using it could not read them either — so this
    // fails loudly, in the channel callers already handle, naming the type.
    String registered = "syntax = \"proto3\";\n"
        + "package test;\n"
        + "import \"confluent/meta.proto\";\n"
        + "message M {\n"
        + "  string payload = 1 [(confluent.field_meta) = {\n"
        + "    rules: [{name: \"r\", expr: \"true\"}]\n"
        + "  }];\n"
        + "}\n";
    String producer = "syntax = \"proto3\";\n"
        + "package test;\n"
        + "message M { bytes payload = 1; }\n";
    ProtobufSchema schema = new ProtobufSchema(registered);
    Descriptor producerDesc = new ProtobufSchema(producer).toDescriptor("test.M");
    DynamicMessage msg = DynamicMessage.newBuilder(producerDesc)
        .setField(producerDesc.findFieldByName("payload"),
            ByteString.copyFrom(new byte[] {(byte) 0xff, (byte) 0xfe})).build();

    SerializationException e = assertThrows(SerializationException.class,
        () -> schema.validateMessage(ALWAYS_FAIL, msg));
    assertTrue(e.getMessage(), e.getMessage().contains("test.M"));
  }

  @Test
  public void descriptorsThatAgree_areNotReReadThroughTheSchema() {
    // A generated class's descriptor is never the same object as the one built from the
    // registered schema text, so an identity check alone would re-read every record. When
    // the two describe the same fields there is nothing to gain from it — and something to
    // lose: re-reading imposes the schema's own parse checks on a message that already
    // satisfies its class. A proto2 message built with buildPartial() shows the difference,
    // since re-reading bytes that omit a required field fails.
    String s = "syntax = \"proto2\";\n"
        + "package test;\n"
        + "import \"confluent/meta.proto\";\n"
        + "message M {\n"
        + "  required string a = 1;\n"
        + "  optional string b = 2 [(confluent.field_meta) = {\n"
        + "    rules: [{name: \"r\", expr: \"true\"}]\n"
        + "  }];\n"
        + "}\n";
    ProtobufSchema schema = new ProtobufSchema(s);
    // A separate instance, so the descriptors are distinct objects describing the same type.
    Descriptor producerDesc = new ProtobufSchema(s).toDescriptor("test.M");
    DynamicMessage partial = DynamicMessage.newBuilder(producerDesc)
        .setField(producerDesc.findFieldByName("b"), "set").buildPartial();

    assertEquals(Collections.singletonList("r@b"),
        firedRules(schema.validateMessage(ALWAYS_FAIL, partial)));
  }

  @Test
  public void nestedMessageRule_seesSchemaNamesUnderARename() {
    // A rule that binds `this` to a nested message needs that message in the schema's terms,
    // not just the top-level one: the rule's CEL environment comes from the schema, so
    // `this.renamed` cannot read a field the producer's class calls something else.
    String registered = "syntax = \"proto3\";\n"
        + "package test;\n"
        + "import \"confluent/meta.proto\";\n"
        + "message Outer { test.Inner inner = 1; }\n"
        + "message Inner {\n"
        + "  option (confluent.message_meta) = {\n"
        + "    rules: [{name: \"m\", expr: \"this.renamed == 'x'\"}]\n"
        + "  };\n"
        + "  string renamed = 1;\n"
        + "}\n";
    String producer = "syntax = \"proto3\";\n"
        + "package test;\n"
        + "message Outer { test.Inner inner = 1; }\n"
        + "message Inner { string original = 1; }\n";
    ProtobufSchema schema = new ProtobufSchema(registered);
    ProtobufSchema producerSchema = new ProtobufSchema(producer);
    Descriptor outerDesc = producerSchema.toDescriptor("test.Outer");
    Descriptor innerDesc = producerSchema.toDescriptor("test.Inner");
    DynamicMessage inner = DynamicMessage.newBuilder(innerDesc)
        .setField(innerDesc.findFieldByName("original"), "x").build();
    DynamicMessage outer = DynamicMessage.newBuilder(outerDesc)
        .setField(outerDesc.findFieldByName("inner"), inner).build();

    assertEquals(Collections.emptyList(),
        firedRules(schema.validateMessage(SCHEMA_NAMED, outer)));
  }

  @Test
  public void repeatedNestedMessageRule_seesSchemaNamesUnderARename() {
    // Same requirement per element of a repeated field: the schema's view of the list has to
    // be paired to the caller's list positionally.
    String registered = "syntax = \"proto3\";\n"
        + "package test;\n"
        + "import \"confluent/meta.proto\";\n"
        + "message Outer { repeated test.Inner items = 1; }\n"
        + "message Inner {\n"
        + "  option (confluent.message_meta) = {\n"
        + "    rules: [{name: \"m\", expr: \"this.renamed == 'x'\"}]\n"
        + "  };\n"
        + "  string renamed = 1;\n"
        + "}\n";
    String producer = "syntax = \"proto3\";\n"
        + "package test;\n"
        + "message Outer { repeated test.Inner items = 1; }\n"
        + "message Inner { string original = 1; }\n";
    ProtobufSchema schema = new ProtobufSchema(registered);
    ProtobufSchema producerSchema = new ProtobufSchema(producer);
    Descriptor outerDesc = producerSchema.toDescriptor("test.Outer");
    Descriptor innerDesc = producerSchema.toDescriptor("test.Inner");
    FieldDescriptor original = innerDesc.findFieldByName("original");
    DynamicMessage outer = DynamicMessage.newBuilder(outerDesc)
        .addRepeatedField(outerDesc.findFieldByName("items"),
            DynamicMessage.newBuilder(innerDesc).setField(original, "x").build())
        .addRepeatedField(outerDesc.findFieldByName("items"),
            DynamicMessage.newBuilder(innerDesc).setField(original, "wrong").build())
        .build();

    // Only the second element violates, which also shows each element was paired to its own
    // position rather than all of them seeing the first.
    assertEquals(Collections.singletonList("m@items[1]"),
        firedRules(schema.validateMessage(SCHEMA_NAMED, outer)));
  }
}
