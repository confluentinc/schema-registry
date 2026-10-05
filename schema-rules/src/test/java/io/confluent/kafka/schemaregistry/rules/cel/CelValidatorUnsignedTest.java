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
import com.google.protobuf.Descriptors.FieldDescriptor;
import com.google.protobuf.DynamicMessage;
import io.confluent.kafka.schemaregistry.protobuf.ProtobufSchema;
import io.confluent.kafka.schemaregistry.rules.ValidationRuleError;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import org.junit.jupiter.api.Test;

/**
 * Java's boxes do not carry protobuf's type distinctions: a {@code uint64} arrives from
 * reflection as a {@code Long} exactly as an {@code int64} does, and an enum arrives as an
 * {@code EnumValueDescriptor} rather than a number. The validator has to take the type and
 * the value from the field's declaration instead, or a rule written against the field's own
 * type cannot compile, and one written with a signed literal answers wrongly for values
 * above {@link Long#MAX_VALUE}.
 */
public class CelValidatorUnsignedTest {

  private static final String SCHEMA = "syntax = \"proto3\";\n"
      + "package test;\n"
      + "import \"confluent/meta.proto\";\n"
      + "message U {\n"
      + "  uint64 serial = 1 [(confluent.field_meta) = {\n"
      + "    rules: [{name: \"mod\", expr: \"this % 10u == 5u\"},\n"
      + "            {name: \"unsignedCmp\", expr: \"this > 0u\"},\n"
      + "            {name: \"signedCmp\", expr: \"this > 0\"}]\n"
      + "  }];\n"
      + "  uint32 small = 2 [(confluent.field_meta) = {\n"
      + "    rules: [{name: \"small\", expr: \"this > 0u\"}]\n"
      + "  }];\n"
      + "  repeated uint64 serials = 3 [(confluent.field_meta) = {\n"
      + "    rules: [{name: \"each\", expr: \"this.all(v, v > 0u)\"}]\n"
      + "  }];\n"
      + "}\n";

  private static List<String> firedRules(List<ValidationRuleError> errors) {
    List<String> out = new ArrayList<>();
    for (ValidationRuleError e : errors) {
      out.add(e.getRule().getName() + "@" + e.getFieldPath());
    }
    return out;
  }

  private static List<String> validate(long serial) {
    ProtobufSchema schema = new ProtobufSchema(SCHEMA);
    Descriptor desc = schema.toDescriptor("test.U");
    DynamicMessage msg = DynamicMessage.newBuilder(desc)
        .setField(desc.findFieldByName("serial"), serial)
        .setField(desc.findFieldByName("small"), 7)
        .addRepeatedField(desc.findFieldByName("serials"), 5L)
        .addRepeatedField(desc.findFieldByName("serials"), 15L)
        .build();
    return firedRules(schema.validateMessage(new CelValidator(), msg));
  }

  @Test
  public void enumFieldIsComparedByItsNumber() {
    // Protobuf reflection hands back an EnumValueDescriptor, which no Java class mapping
    // recognizes — the rule is left comparing a descriptor object against an integer.
    String schemaStr = "syntax = \"proto3\";\n"
        + "package test;\n"
        + "import \"confluent/meta.proto\";\n"
        + "enum Color { RED = 0; GREEN = 1; }\n"
        + "message M {\n"
        + "  test.Color color = 1 [(confluent.field_meta) = {\n"
        + "    rules: [{name: \"isGreen\", expr: \"this == 1\"}]\n"
        + "  }];\n"
        + "}\n";
    ProtobufSchema schema = new ProtobufSchema(schemaStr);
    Descriptor desc = schema.toDescriptor("test.M");
    FieldDescriptor color = desc.findFieldByName("color");
    DynamicMessage msg = DynamicMessage.newBuilder(desc)
        .setField(color, color.getEnumType().findValueByNumber(1)).build();

    assertEquals(Collections.emptyList(),
        firedRules(schema.validateMessage(new CelValidator(), msg)));
  }

  @Test
  public void unsignedRulesHoldForAValueThatFitsInALong() {
    // 25 % 10 == 5, and every rule here is true of it. Without the field's declared type,
    // the two rules written with unsigned literals do not compile at all.
    assertEquals(Collections.emptyList(), validate(25L));
  }

  @Test
  public void unsignedRulesHoldAboveLongMaxValue() {
    // 2^64 - 5 is positive as an unsigned value but negative as a signed long, and its
    // remainder mod 10 is 1 rather than the 5 a signed reading would give.
    List<String> fired = validate(-5L);
    assertEquals(Collections.singletonList("mod@serial"), fired);
  }
}
