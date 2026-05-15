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
import com.google.protobuf.DynamicMessage;
import com.google.protobuf.Timestamp;
import io.confluent.kafka.schemaregistry.avro.AvroSchema;
import io.confluent.kafka.schemaregistry.protobuf.ProtobufSchema;
import io.confluent.kafka.schemaregistry.rules.ValidationRuleError;
import java.time.Instant;
import java.util.List;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.junit.jupiter.api.Test;

/**
 * Integration tests for {@code timestamp.of(dyn)} and
 * {@code timestamp.of(int, string)} against a Proto schema with a
 * {@code google.protobuf.Timestamp} field and a long timestamp-millis field.
 */
public class CelValidatorTimestampTest {

  private static final String SCHEMA = "syntax = \"proto3\";\n"
      + "package test;\n"
      + "import \"confluent/meta.proto\";\n"
      + "import \"google/protobuf/timestamp.proto\";\n"
      + "message Event {\n"
      + "  google.protobuf.Timestamp created_at = 1 [(confluent.field_meta) = {\n"
      + "    rules: [{name: \"notFuture\","
      + "             expr: \"timestamp.of(this) < now\"}]\n"
      + "  }];\n"
      + "}\n";

  private static DynamicMessage event(Instant when) {
    ProtobufSchema schema = new ProtobufSchema(SCHEMA);
    Descriptor desc = schema.toDescriptor("test.Event");
    Timestamp ts = Timestamp.newBuilder()
        .setSeconds(when.getEpochSecond())
        .setNanos(when.getNano())
        .build();
    return DynamicMessage.newBuilder(desc)
        .setField(desc.findFieldByName("created_at"), ts)
        .build();
  }

  @Test
  void pastTimestamp_passesNow() {
    ProtobufSchema schema = new ProtobufSchema(SCHEMA);
    List<ValidationRuleError> errors =
        schema.validateMessage(new CelValidator(), event(Instant.now().minusSeconds(60)));
    assertTrue(errors.isEmpty(), "Past timestamp should pass, got: " + errors);
  }

  @Test
  void futureTimestamp_failsNow() {
    ProtobufSchema schema = new ProtobufSchema(SCHEMA);
    List<ValidationRuleError> errors =
        schema.validateMessage(new CelValidator(), event(Instant.now().plusSeconds(3600)));
    assertEquals(1, errors.size());
    assertEquals("notFuture", errors.get(0).getRule().getName());
  }

  @Test
  void fromEpochMillis_unitArg() {
    // long field interpreted as epoch millis via timestamp.of(long, "millis").
    String s = "syntax = \"proto3\";\n"
        + "package test;\n"
        + "import \"confluent/meta.proto\";\n"
        + "message X {\n"
        + "  int64 ts_ms = 1 [(confluent.field_meta) = {\n"
        + "    rules: [{name: \"r\","
        + "             expr: \"timestamp.of(this, \\\"millis\\\") < now\"}]\n"
        + "  }];\n"
        + "}\n";
    ProtobufSchema schema = new ProtobufSchema(s);
    Descriptor desc = schema.toDescriptor("test.X");

    // Past time → passes
    DynamicMessage past = DynamicMessage.newBuilder(desc)
        .setField(desc.findFieldByName("ts_ms"),
            Instant.now().minusSeconds(60).toEpochMilli())
        .build();
    assertTrue(schema.validateMessage(new CelValidator(), past).isEmpty());

    // Future time → fails
    DynamicMessage future = DynamicMessage.newBuilder(desc)
        .setField(desc.findFieldByName("ts_ms"),
            Instant.now().plusSeconds(3600).toEpochMilli())
        .build();
    assertEquals(1, schema.validateMessage(new CelValidator(), future).size());
  }

  @Test
  void fromEpochUnknownUnit_failsAtRuntime() {
    String s = "syntax = \"proto3\";\n"
        + "package test;\n"
        + "import \"confluent/meta.proto\";\n"
        + "message X {\n"
        + "  int64 ts_ms = 1 [(confluent.field_meta) = {\n"
        + "    rules: [{name: \"r\","
        + "             expr: \"timestamp.of(this, \\\"weeks\\\") < now\"}]\n"
        + "  }];\n"
        + "}\n";
    ProtobufSchema schema = new ProtobufSchema(s);
    Descriptor desc = schema.toDescriptor("test.X");
    DynamicMessage msg = DynamicMessage.newBuilder(desc)
        .setField(desc.findFieldByName("ts_ms"), 1000L)
        .build();
    List<ValidationRuleError> errors = schema.validateMessage(new CelValidator(), msg);
    assertEquals(1, errors.size());
  }

  // ---- Avro timestamp logical type ----

  private static final String AVRO_TIMESTAMP_MILLIS_SCHEMA = ""
      + "{"
      + "  \"type\":\"record\","
      + "  \"name\":\"Event\","
      + "  \"namespace\":\"test\","
      + "  \"fields\":["
      + "    {"
      + "      \"name\":\"created_at\","
      + "      \"type\":{"
      + "        \"type\":\"long\","
      + "        \"logicalType\":\"timestamp-millis\""
      + "      },"
      + "      \"confluent:rules\":["
      + "        {\"name\":\"notFuture\","
      + "         \"expr\":\"timestamp.of(this) < now\"}]"
      + "    }"
      + "  ]"
      + "}";

  @Test
  void avroTimestampMillis_convertersOn_instantArrives_pastPasses() {
    // With useLogicalTypeConverters=true, the timestamp-millis field arrives
    // as an Instant. timestamp.of(dyn) routes it via TimestampUtils.fromInstant.
    AvroSchema schema = new AvroSchema(AVRO_TIMESTAMP_MILLIS_SCHEMA);
    GenericRecord r = new GenericData.Record(schema.rawSchema());
    r.put("created_at", Instant.now().minusSeconds(60));
    assertTrue(schema.validateMessage(new CelValidator(), r).isEmpty());
  }

  @Test
  void avroTimestampMillis_convertersOn_futureFails() {
    AvroSchema schema = new AvroSchema(AVRO_TIMESTAMP_MILLIS_SCHEMA);
    GenericRecord r = new GenericData.Record(schema.rawSchema());
    r.put("created_at", Instant.now().plusSeconds(3600));
    List<ValidationRuleError> errs = schema.validateMessage(new CelValidator(), r);
    assertEquals(1, errs.size());
    assertEquals("notFuture", errs.get(0).getRule().getName());
  }

  @Test
  void avroTimestampMillis_convertersOff_rawLongRequiresUnitArg() {
    // With useLogicalTypeConverters=false, the value is a raw Long; the one-
    // arg timestamp.of(dyn) throws (no unit). Use the two-arg overload.
    String s = ""
        + "{"
        + "  \"type\":\"record\","
        + "  \"name\":\"Event\","
        + "  \"namespace\":\"test\","
        + "  \"fields\":["
        + "    {"
        + "      \"name\":\"ts_ms\","
        + "      \"type\":\"long\","
        + "      \"confluent:rules\":["
        + "        {\"name\":\"r\","
        + "         \"expr\":\"timestamp.of(this, \\\"millis\\\") < now\"}]"
        + "    }"
        + "  ]"
        + "}";
    AvroSchema schema = new AvroSchema(s);
    GenericRecord r = new GenericData.Record(schema.rawSchema());
    r.put("ts_ms", Instant.now().minusSeconds(60).toEpochMilli());
    assertTrue(schema.validateMessage(new CelValidator(), r).isEmpty());
  }

  @Test
  void avroTimestampMicros_convertersOn_instantArrives() {
    // Same as millis but with the micros logical type. With converters on,
    // Instant arrives regardless of underlying precision.
    String s = ""
        + "{"
        + "  \"type\":\"record\","
        + "  \"name\":\"Event\","
        + "  \"namespace\":\"test\","
        + "  \"fields\":["
        + "    {"
        + "      \"name\":\"created_at\","
        + "      \"type\":{"
        + "        \"type\":\"long\","
        + "        \"logicalType\":\"timestamp-micros\""
        + "      },"
        + "      \"confluent:rules\":["
        + "        {\"name\":\"r\","
        + "         \"expr\":\"timestamp.of(this) < now\"}]"
        + "    }"
        + "  ]"
        + "}";
    AvroSchema schema = new AvroSchema(s);
    GenericRecord r = new GenericData.Record(schema.rawSchema());
    r.put("created_at", Instant.now().minusSeconds(60));
    assertTrue(schema.validateMessage(new CelValidator(), r).isEmpty());
  }
}
