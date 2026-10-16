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

import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.Descriptors.FieldDescriptor;
import com.google.protobuf.DynamicMessage;
import com.google.protobuf.Timestamp;
import io.confluent.kafka.schemaregistry.protobuf.ProtobufSchema;
import io.confluent.kafka.schemaregistry.rules.ValidationRuleError;
import java.time.Instant;
import java.util.List;
import org.junit.jupiter.api.Test;

/**
 * Tests for the implicit {@code now} variable that {@link CelValidator} binds to every
 * rule evaluation as a {@code google.protobuf.Timestamp} of the current wall-clock
 * instant. Pattern matches protovalidate-java's per-evaluation semantics — each rule sees
 * a freshly captured {@code now}, sub-millisecond skew between rules in the same walk is
 * accepted.
 */
public class CelValidatorNowTest {

  /**
   * Schema with a {@code Timestamp} field carrying a rule that compares it to {@code now}.
   * Rule passes when {@code this <= now}, fails when the timestamp is in the future.
   */
  private static final String SCHEMA = "syntax = \"proto3\";\n"
      + "package test;\n"
      + "import \"confluent/meta.proto\";\n"
      + "import \"google/protobuf/timestamp.proto\";\n"
      + "message Event {\n"
      + "  google.protobuf.Timestamp created_at = 1 [(confluent.field_meta) = {\n"
      + "    rules: [{name: \"notFuture\", expr: \"this <= now\"}]\n"
      + "  }];\n"
      + "}\n";

  private static DynamicMessage event(Instant when) {
    ProtobufSchema schema = new ProtobufSchema(SCHEMA);
    Descriptor desc = schema.toDescriptor("test.Event");
    FieldDescriptor createdAt = desc.findFieldByName("created_at");
    Timestamp ts = Timestamp.newBuilder()
        .setSeconds(when.getEpochSecond())
        .setNanos(when.getNano())
        .build();
    return DynamicMessage.newBuilder(desc)
        .setField(createdAt, ts)
        .build();
  }

  @Test
  void pastTimestamp_satisfiesNow() {
    ProtobufSchema schema = new ProtobufSchema(SCHEMA);
    DynamicMessage past = event(Instant.now().minusSeconds(60));

    List<ValidationRuleError> errors =
        schema.validateMessage(new CelValidator(), past);
    assertTrue(errors.isEmpty(),
        "Past timestamp should satisfy `this <= now`, got errors: " + errors);
  }

  @Test
  void futureTimestamp_violatesNow() {
    ProtobufSchema schema = new ProtobufSchema(SCHEMA);
    DynamicMessage future = event(Instant.now().plusSeconds(3600));

    List<ValidationRuleError> errors =
        schema.validateMessage(new CelValidator(), future);
    assertEquals(1, errors.size(), "Future timestamp should fail the rule, got: " + errors);
    assertEquals("notFuture", errors.get(0).getRule().getName());
    assertEquals("created_at", errors.get(0).getFieldPath());
  }
}
