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

package io.confluent.kafka.schemaregistry.rules.cel.builtin;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.google.protobuf.Timestamp;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import org.junit.jupiter.api.Test;

/**
 * Unit tests for {@link TimestampUtils} — {@link TimestampUtils#toTimestamp(Object)}
 * dispatch and the {@link TimestampUtils#fromEpoch(long, String)} unit-dispatch.
 */
public class TimestampUtilsTest {

  @Test
  void dispatchIdentity_passesTimestampThrough() {
    Timestamp t = Timestamp.newBuilder().setSeconds(100).setNanos(500).build();
    assertEquals(t, TimestampUtils.toTimestamp(t));
  }

  @Test
  void dispatchInstant_converts() {
    Instant i = Instant.ofEpochSecond(1700000000L, 123456789);
    Timestamp t = TimestampUtils.toTimestamp(i);
    assertEquals(1700000000L, t.getSeconds());
    assertEquals(123456789, t.getNanos());
  }

  @Test
  void dispatchOffsetDateTime_converts() {
    OffsetDateTime odt = OffsetDateTime.of(2026, 1, 1, 0, 0, 0, 0, ZoneOffset.UTC);
    Timestamp t = TimestampUtils.toTimestamp(odt);
    assertEquals(odt.toInstant().getEpochSecond(), t.getSeconds());
  }

  @Test
  void dispatchString_parsesRfc3339() {
    Timestamp t = TimestampUtils.toTimestamp("2026-01-01T00:00:00Z");
    assertEquals(1767225600L, t.getSeconds());
  }

  @Test
  void dispatchLocalDateTime_refusedWithHint() {
    LocalDateTime ldt = LocalDateTime.of(2026, 1, 1, 12, 0);
    IllegalArgumentException e = assertThrows(IllegalArgumentException.class,
        () -> TimestampUtils.toTimestamp(ldt));
    assertTrue(e.getMessage().contains("LocalDateTime"));
    assertTrue(e.getMessage().contains("timestamp.of"));
  }

  @Test
  void dispatchRawLong_throwsWithUnitHint() {
    IllegalArgumentException e = assertThrows(IllegalArgumentException.class,
        () -> TimestampUtils.toTimestamp(1700000000000L));
    assertTrue(e.getMessage().contains("timestamp.of"));
  }

  @Test
  void dispatchNull_throws() {
    assertThrows(IllegalArgumentException.class,
        () -> TimestampUtils.toTimestamp(null));
  }

  @Test
  void fromEpochMillis_basicConversion() {
    Timestamp t = TimestampUtils.fromEpoch(1500L, "millis");
    assertEquals(1L, t.getSeconds());
    assertEquals(500_000_000, t.getNanos());
  }

  @Test
  void fromEpochMicros_basicConversion() {
    Timestamp t = TimestampUtils.fromEpoch(1_500_000L, "micros");
    assertEquals(1L, t.getSeconds());
    assertEquals(500_000_000, t.getNanos());
  }

  @Test
  void fromEpochNanos_basicConversion() {
    Timestamp t = TimestampUtils.fromEpoch(1_500_000_000L, "nanos");
    assertEquals(1L, t.getSeconds());
    assertEquals(500_000_000, t.getNanos());
  }

  @Test
  void fromEpochSeconds_basicConversion() {
    Timestamp t = TimestampUtils.fromEpoch(1700000000L, "seconds");
    assertEquals(1700000000L, t.getSeconds());
    assertEquals(0, t.getNanos());
  }

  @Test
  void fromEpochNegativeMillis_floorDivCorrect() {
    Timestamp t = TimestampUtils.fromEpoch(-500L, "millis");
    assertEquals(-1L, t.getSeconds());
    assertEquals(500_000_000, t.getNanos());
  }

  @Test
  void fromEpochUnknownUnit_throws() {
    IllegalArgumentException e = assertThrows(IllegalArgumentException.class,
        () -> TimestampUtils.fromEpoch(1L, "weeks"));
    assertTrue(e.getMessage().contains("weeks"));
    assertTrue(e.getMessage().contains("millis"));
  }

  @Test
  void fromEpochNullUnit_throws() {
    assertThrows(IllegalArgumentException.class,
        () -> TimestampUtils.fromEpoch(1L, null));
  }
}
