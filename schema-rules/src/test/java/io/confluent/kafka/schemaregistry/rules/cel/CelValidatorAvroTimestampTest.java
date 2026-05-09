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

import io.confluent.kafka.schemaregistry.avro.AvroSchema;
import io.confluent.kafka.schemaregistry.rules.ValidationRuleError;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.List;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.junit.jupiter.api.Test;

/**
 * Coverage for {@code to_timestamp} against Avro timestamp logical types:
 * {@code timestamp-millis}, {@code timestamp-micros}, {@code local-timestamp-millis},
 * {@code local-timestamp-micros}. Compares against the implicit {@code now} variable.
 */
public class CelValidatorAvroTimestampTest {

  /**
   * Builds an Avro schema with one timestamp-typed field. Uses a placeholder for
   * the logical type name so we can vary it per test.
   */
  private static AvroSchema schema(String logicalType) {
    String json = "{"
        + "\"type\":\"record\","
        + "\"name\":\"Event\","
        + "\"namespace\":\"test\","
        + "\"fields\":["
        + "  {\"name\":\"created_at\","
        + "   \"type\":{\"type\":\"long\",\"logicalType\":\"" + logicalType + "\"},"
        + "   \"confluent:rules\":["
        + "     {\"name\":\"notFuture\","
        + "      \"expr\":\"to_timestamp(this) <= now\"}"
        + "   ]}"
        + "]"
        + "}";
    return new AvroSchema(json);
  }

  private static GenericRecord recordOf(AvroSchema avroSchema, Object timestamp) {
    Schema raw = avroSchema.rawSchema();
    GenericRecord r = new GenericData.Record(raw);
    r.put("created_at", timestamp);
    return r;
  }

  // -- timestamp-millis (UTC, epoch milliseconds) --

  @Test
  void timestampMillis_pastInstant_satisfies() {
    AvroSchema s = schema("timestamp-millis");
    GenericRecord r = recordOf(s, Instant.now().minusSeconds(60));
    List<ValidationRuleError> errors = s.validateMessage(new CelValidator(), r);
    if (!errors.isEmpty() && errors.get(0).getCause() != null) {
      throw new AssertionError(errors.get(0).getCause().getMessage(),
          errors.get(0).getCause());
    }
    assertTrue(errors.isEmpty(),
        "Past timestamp-millis should satisfy 'this <= now', got: " + errors);
  }

  @Test
  void timestampMillis_futureInstant_violates() {
    AvroSchema s = schema("timestamp-millis");
    GenericRecord r = recordOf(s, Instant.now().plusSeconds(3600));
    List<ValidationRuleError> errors = s.validateMessage(new CelValidator(), r);
    assertEquals(1, errors.size(),
        "Future timestamp-millis should fail 'this <= now', got: " + errors);
    assertEquals("notFuture", errors.get(0).getRule().getName());
  }

  // -- timestamp-micros (UTC, epoch microseconds) --

  @Test
  void timestampMicros_pastInstant_satisfies() {
    AvroSchema s = schema("timestamp-micros");
    GenericRecord r = recordOf(s, Instant.now().minusSeconds(60));
    List<ValidationRuleError> errors = s.validateMessage(new CelValidator(), r);
    if (!errors.isEmpty() && errors.get(0).getCause() != null) {
      throw new AssertionError(errors.get(0).getCause().getMessage(),
          errors.get(0).getCause());
    }
    assertTrue(errors.isEmpty(),
        "Past timestamp-micros should satisfy 'this <= now', got: " + errors);
  }

  @Test
  void timestampMicros_futureInstant_violates() {
    AvroSchema s = schema("timestamp-micros");
    GenericRecord r = recordOf(s, Instant.now().plusSeconds(3600));
    List<ValidationRuleError> errors = s.validateMessage(new CelValidator(), r);
    assertEquals(1, errors.size(),
        "Future timestamp-micros should fail 'this <= now', got: " + errors);
  }

  // -- local-timestamp-* — out of v1 scope --
  //
  // local-timestamp-* logical types carry no timezone. Silently mapping the
  // wall-clock value to a UTC google.protobuf.Timestamp would produce wrong
  // results for non-UTC producers, so to_timestamp throws on LocalDateTime
  // input. These tests verify the refusal is clean (a ValidationRuleError
  // with an informative cause), not a process-killing escape.

  @Test
  void localTimestampMillis_throwsCleanly() {
    AvroSchema s = schema("local-timestamp-millis");
    LocalDateTime when = LocalDateTime.now(ZoneOffset.UTC);
    GenericRecord r = recordOf(s, when);
    List<ValidationRuleError> errors = s.validateMessage(new CelValidator(), r);
    assertEquals(1, errors.size(),
        "local-timestamp-millis should produce a clean violation, got: " + errors);
    assertTrue(causeChainMentions(errors.get(0).getCause(), "LocalDateTime"),
        "expected cause chain to mention LocalDateTime");
  }

  private static boolean causeChainMentions(Throwable t, String marker) {
    while (t != null) {
      if (t.getMessage() != null && t.getMessage().contains(marker)) {
        return true;
      }
      t = t.getCause();
    }
    return false;
  }

  @Test
  void localTimestampMicros_throwsCleanly() {
    AvroSchema s = schema("local-timestamp-micros");
    LocalDateTime when = LocalDateTime.now(ZoneOffset.UTC);
    GenericRecord r = recordOf(s, when);
    List<ValidationRuleError> errors = s.validateMessage(new CelValidator(), r);
    assertEquals(1, errors.size(),
        "local-timestamp-micros should produce a clean violation, got: " + errors);
  }
}
