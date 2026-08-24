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

import com.google.protobuf.Timestamp;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.OffsetDateTime;
import java.time.ZonedDateTime;
import java.time.temporal.Temporal;

/**
 * Conversion helpers backing the extension overloads on the standard {@code timestamp}
 * function. The CEL surface uses the built-in timestamp type
 * ({@link CelTypeLabels#TIMESTAMP_NAME}), whose runtime value is an {@link Instant}; the
 * {@link Timestamp}-returning helpers below back the {@code variants.*} timestamp accessors.
 */
final class TimestampUtils {

  // CEL's timestamp range: 0001-01-01T00:00:00Z through 9999-12-31T23:59:59.999999999Z,
  // the same bounds cel-java's standard timestamp(int) conversion enforces.
  private static final long MIN_EPOCH_SECOND = -62135596800L;
  private static final long MAX_EPOCH_SECOND = 253402300799L;

  private TimestampUtils() {
  }

  /**
   * Runtime dispatch backing the {@code timestamp(timestamp)} overload: the {@code java.time}
   * shapes an Avro or Proto decoder produces, converted to the {@link Instant} that cel-java
   * uses for CEL's timestamp type when canonical types evaluate to native values.
   *
   * <p>The standard library binds this overload to {@link Instant} alone, so every other
   * temporal an Avro logical type can yield would have no matching overload. Partial temporals
   * ({@code LocalDate}, {@code LocalTime}) are not timestamps at all and are refused here
   * rather than guessed at.
   */
  static Instant toInstant(Temporal value) {
    if (value instanceof Instant) {
      return (Instant) value;
    }
    if (value instanceof LocalDateTime) {
      // Avro `local-timestamp-*` produces this; the value carries no timezone.
      // Refusing the conversion is more correct than silently picking UTC and
      // returning wrong results for non-UTC producers. See the design doc
      // "Avro logical-type scope" section.
      throw new IllegalArgumentException(
          "Cannot convert LocalDateTime to Timestamp: local-timestamp values "
              + "carry no timezone. Use the regular timestamp-* logical type "
              + "(UTC by spec), or carry a separate TZ-offset field and use "
              + "timestamp(value, precision) on the offset-adjusted epoch value.");
    }
    if (value instanceof OffsetDateTime) {
      return ((OffsetDateTime) value).toInstant();
    }
    if (value instanceof ZonedDateTime) {
      return ((ZonedDateTime) value).toInstant();
    }
    throw new IllegalArgumentException(
        "Cannot convert " + value.getClass().getName() + " to Timestamp");
  }

  /**
   * Backing {@code timestamp(int, int)}: an epoch value read at a decimal precision, the same
   * {0, 3, 6, 9} scale Flink uses — 0 seconds, 3 millis, 6 micros, 9 nanos. Any other
   * precision is rejected: with the unit now a number rather than a name, that check is the
   * only thing standing between a typo and a silently wrong instant.
   */
  static Instant fromEpochPrecision(long value, long precision) {
    if (precision == 0L) {
      return instantOfEpoch(value, 1L, 1_000_000_000L);
    } else if (precision == 3L) {
      return instantOfEpoch(value, 1_000L, 1_000_000L);
    } else if (precision == 6L) {
      return instantOfEpoch(value, 1_000_000L, 1_000L);
    } else if (precision == 9L) {
      return instantOfEpoch(value, 1_000_000_000L, 1L);
    }
    throw new IllegalArgumentException(
        "Unknown timestamp precision " + precision + "; expected 0 (seconds), 3 (millis), "
            + "6 (micros) or 9 (nanos)");
  }

  /**
   * Epoch value in units of {@code perSecond} per second, each worth {@code nanosPerUnit}.
   */
  private static Instant instantOfEpoch(long epoch, long perSecond, long nanosPerUnit) {
    // floorDiv/floorMod rather than / and %: a pre-epoch value is negative, and the
    // nano-of-second adjustment must stay non-negative.
    long seconds = Math.floorDiv(epoch, perSecond);
    if (seconds < MIN_EPOCH_SECOND || seconds > MAX_EPOCH_SECOND) {
      throw new IllegalArgumentException(
          "Timestamp out of range: " + seconds + " seconds since the epoch is outside "
              + "0001-01-01T00:00:00Z..9999-12-31T23:59:59.999999999Z");
    }
    return Instant.ofEpochSecond(seconds, Math.floorMod(epoch, perSecond) * nanosPerUnit);
  }

  static Timestamp fromEpochMicros(long us) {
    long sec = Math.floorDiv(us, 1_000_000L);
    int nanos = (int) (Math.floorMod(us, 1_000_000L) * 1_000L);
    return Timestamp.newBuilder().setSeconds(sec).setNanos(nanos).build();
  }

  static Timestamp fromEpochNanos(long ns) {
    long sec = Math.floorDiv(ns, 1_000_000_000L);
    int nanos = (int) Math.floorMod(ns, 1_000_000_000L);
    return Timestamp.newBuilder().setSeconds(sec).setNanos(nanos).build();
  }

}
