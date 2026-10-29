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
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import org.junit.jupiter.api.Test;

/**
 * Unit tests for {@link TimestampUtils} — the {@link TimestampUtils#toInstant} dispatch
 * backing {@code timestamp(timestamp)} and the {@link TimestampUtils#fromEpochPrecision}
 * precision dispatch backing {@code timestamp(int, int)}.
 */
public class TimestampUtilsTest {

  @Test
  void instant_passesThroughUnchanged() {
    Instant i = Instant.ofEpochSecond(1700000000L, 123456789);
    assertSame(i, TimestampUtils.toInstant(i));
  }

  @Test
  void offsetDateTime_converts() {
    OffsetDateTime odt = OffsetDateTime.of(2026, 1, 1, 5, 0, 0, 0, ZoneOffset.ofHours(5));
    assertEquals(odt.toInstant(), TimestampUtils.toInstant(odt));
  }

  @Test
  void zonedDateTime_converts() {
    ZonedDateTime zdt = ZonedDateTime.of(2026, 1, 1, 0, 0, 0, 0, ZoneOffset.UTC);
    assertEquals(zdt.toInstant(), TimestampUtils.toInstant(zdt));
  }

  @Test
  void localDateTime_refusedWithHint() {
    LocalDateTime ldt = LocalDateTime.of(2026, 1, 1, 12, 0);
    IllegalArgumentException e = assertThrows(IllegalArgumentException.class,
        () -> TimestampUtils.toInstant(ldt));
    assertTrue(e.getMessage().contains("LocalDateTime"));
    assertTrue(e.getMessage().contains("local-timestamp"));
  }

  /** A partial temporal is not a timestamp; refused rather than guessed at. */
  @Test
  void localDate_refused() {
    IllegalArgumentException e = assertThrows(IllegalArgumentException.class,
        () -> TimestampUtils.toInstant(LocalDate.of(2026, 1, 1)));
    assertTrue(e.getMessage().contains("LocalDate"));
  }

  @Test
  void precisionSeconds() {
    assertEquals(Instant.ofEpochSecond(1700000000L),
        TimestampUtils.fromEpochPrecision(1700000000L, 0));
  }

  @Test
  void precisionMillis() {
    assertEquals(Instant.ofEpochSecond(1L, 500_000_000),
        TimestampUtils.fromEpochPrecision(1500L, 3));
  }

  @Test
  void precisionMicros() {
    assertEquals(Instant.ofEpochSecond(1L, 500_000_000),
        TimestampUtils.fromEpochPrecision(1_500_000L, 6));
  }

  @Test
  void precisionNanos() {
    assertEquals(Instant.ofEpochSecond(1L, 500_000_000),
        TimestampUtils.fromEpochPrecision(1_500_000_000L, 9));
  }

  /** Pre-epoch: floorDiv/floorMod, so the nano adjustment stays non-negative. */
  @Test
  void negativeMillis_floorsCorrectly() {
    assertEquals(Instant.ofEpochSecond(-1L, 500_000_000),
        TimestampUtils.fromEpochPrecision(-500L, 3));
  }

  /**
   * With the unit a number rather than a name, rejecting anything outside {0, 3, 6, 9} is the
   * only thing between a typo and a silently wrong instant.
   */
  @Test
  void unknownPrecision_throws() {
    IllegalArgumentException e = assertThrows(IllegalArgumentException.class,
        () -> TimestampUtils.fromEpochPrecision(1L, 7));
    assertTrue(e.getMessage().contains("7"));
    assertTrue(e.getMessage().contains("millis"));

    // Neighbours of the valid values, and a negative, are all rejected too.
    for (long precision : new long[] {-3, 1, 2, 4, 5, 8, 10, 12}) {
      assertThrows(IllegalArgumentException.class,
          () -> TimestampUtils.fromEpochPrecision(1L, precision),
          "precision " + precision + " should be rejected");
    }
  }

  @Test
  void outOfRange_throws() {
    IllegalArgumentException e = assertThrows(IllegalArgumentException.class,
        () -> TimestampUtils.fromEpochPrecision(Long.MAX_VALUE, 0));
    assertTrue(e.getMessage().contains("out of range"));
    // The same value at nanos precision is only ~292 years, so it stays in range.
    assertEquals(Instant.ofEpochSecond(9223372036L, 854775807L),
        TimestampUtils.fromEpochPrecision(Long.MAX_VALUE, 9));
  }
}
