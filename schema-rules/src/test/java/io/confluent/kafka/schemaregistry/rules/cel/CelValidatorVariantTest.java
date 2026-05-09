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
import io.confluent.kafka.schemaregistry.protobuf.ProtobufSchema;
import io.confluent.kafka.schemaregistry.rules.ValidationRuleError;
import io.confluent.kafka.schemaregistry.type.Variant;
import io.confluent.kafka.schemaregistry.type.VariantBuilder;
import io.confluent.kafka.schemaregistry.type.VariantObjectBuilder;
import io.confluent.protobuf.type.Variant.Builder;
import java.nio.ByteBuffer;
import java.util.List;
import org.junit.jupiter.api.Test;

/**
 * End-to-end test for the variant accessor surface: builds a Spark-binary-encoded
 * Variant via {@link VariantBuilder}, packages it in a {@code confluent.type.Variant}
 * proto field, and runs a rule using {@code to_variant}, {@code variant_get_field},
 * and {@code variant_get_string}.
 */
public class CelValidatorVariantTest {

  /**
   * Schema with a {@code confluent.type.Variant} field. Rule extracts the {@code name}
   * field from the variant payload and asserts it equals "alice".
   */
  private static final String SCHEMA = "syntax = \"proto3\";\n"
      + "package test;\n"
      + "import \"confluent/meta.proto\";\n"
      + "import \"confluent/type/variant.proto\";\n"
      + "message Event {\n"
      + "  confluent.type.Variant payload = 1 [(confluent.field_meta) = {\n"
      + "    rules: [{name: \"nameIsAlice\","
      + "             expr: \"variant_get_string("
      + "                       variant_get_field(to_variant(this), \\\"name\\\")) "
      + "                    == \\\"alice\\\"\"}]\n"
      + "  }];\n"
      + "}\n";

  /** Build a Variant payload {"name": value}. */
  private static Variant variantOf(String name) {
    VariantBuilder vb = new VariantBuilder();
    VariantObjectBuilder ob = vb.startObject();
    ob.appendKey("name");
    ob.appendString(name);
    vb.endObject();
    return vb.build();
  }

  /** Wrap a Variant into a confluent.type.Variant proto + outer Event. */
  private static DynamicMessage event(Variant v) {
    try {
      ProtobufSchema schema = new ProtobufSchema(SCHEMA);
      Descriptor desc = schema.toDescriptor("test.Event");
      FieldDescriptor payloadField = desc.findFieldByName("payload");

      ByteBuffer valueBuf = v.getValueBuffer();
      ByteBuffer metadataBuf = v.getMetadataBuffer();

      Builder protoBuilder = io.confluent.protobuf.type.Variant.newBuilder()
          .setValue(ByteString.copyFrom(valueBuf))
          .setMetadata(ByteString.copyFrom(metadataBuf));

      // Convert through DynamicMessage so the rule's schema descriptor (parsed from
      // text) matches when set on the field.
      Descriptor variantDesc = payloadField.getMessageType();
      DynamicMessage payload = DynamicMessage.parseFrom(
          variantDesc, protoBuilder.build().toByteArray());

      return DynamicMessage.newBuilder(desc)
          .setField(payloadField, payload)
          .build();
    } catch (com.google.protobuf.InvalidProtocolBufferException e) {
      throw new RuntimeException(e);
    }
  }

  @Test
  void aliceName_satisfiesRule() {
    ProtobufSchema schema = new ProtobufSchema(SCHEMA);
    DynamicMessage msg = event(variantOf("alice"));
    List<ValidationRuleError> errors =
        schema.validateMessage(new CelValidator(), msg);
    assertTrue(errors.isEmpty(),
        "name=alice should satisfy the rule, got: " + errors);
  }

  @Test
  void otherName_violatesRule() {
    ProtobufSchema schema = new ProtobufSchema(SCHEMA);
    DynamicMessage msg = event(variantOf("bob"));
    List<ValidationRuleError> errors =
        schema.validateMessage(new CelValidator(), msg);
    assertEquals(1, errors.size(),
        "name=bob should fail the rule, got: " + errors);
    assertEquals("nameIsAlice", errors.get(0).getRule().getName());
    assertEquals("payload", errors.get(0).getFieldPath());
  }
}
