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
import java.time.format.DateTimeParseException;

/**
 * Conversion helpers backing {@code to_timestamp}. The CEL surface uses the built-in
 * timestamp type ({@link CelTypes#TIMESTAMP}); this client backs it with
 * {@link Timestamp}.
 */
final class TimestampUtils {

  static final String UNIT_MILLIS = "millis";
  static final String UNIT_MICROS = "micros";
  static final String UNIT_NANOS = "nanos";
  static final String UNIT_SECONDS = "seconds";

  private TimestampUtils() {
  }

  static Timestamp fromEpochMillis(long ms) {
    long sec = Math.floorDiv(ms, 1_000L);
    int nanos = (int) (Math.floorMod(ms, 1_000L) * 1_000_000L);
    return Timestamp.newBuilder().setSeconds(sec).setNanos(nanos).build();
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

  static Timestamp fromEpochSeconds(long s) {
    return Timestamp.newBuilder().setSeconds(s).build();
  }

  static Timestamp fromInstant(Instant i) {
    return Timestamp.newBuilder()
        .setSeconds(i.getEpochSecond())
        .setNanos(i.getNano())
        .build();
  }

  /** Construct from epoch numeric value plus unit string. */
  static Timestamp fromEpoch(long value, String unit) {
    if (unit == null) {
      throw new IllegalArgumentException("to_timestamp unit must not be null");
    }
    switch (unit) {
      case UNIT_MILLIS:  return fromEpochMillis(value);
      case UNIT_MICROS:  return fromEpochMicros(value);
      case UNIT_NANOS:   return fromEpochNanos(value);
      case UNIT_SECONDS: return fromEpochSeconds(value);
      default:
        throw new IllegalArgumentException(
            "Unknown to_timestamp unit '" + unit + "'; expected one of millis, micros, "
                + "nanos, seconds");
    }
  }

  /** Parse RFC 3339 (offset-aware) timestamp string. */
  static Timestamp fromRfc3339(String s) {
    try {
      return fromInstant(OffsetDateTime.parse(s).toInstant());
    } catch (DateTimeParseException e) {
      throw new IllegalArgumentException(
          "Cannot parse '" + s + "' as RFC 3339 timestamp", e);
    }
  }

  /**
   * Runtime dispatch backing {@code to_timestamp(dyn)}. Accepts the shapes Proto/Avro
   * decoders typically produce. Raw {@code Long} lacks a unit and must use the two-arg
   * overload instead — we throw with a clear hint.
   */
  static Timestamp toTimestamp(Object o) {
    if (o == null) {
      throw new IllegalArgumentException("Cannot convert null to Timestamp");
    }
    if (o instanceof Timestamp) {
      return (Timestamp) o;
    }
    if (o instanceof Instant) {
      return fromInstant((Instant) o);
    }
    if (o instanceof LocalDateTime) {
      // Avro `local-timestamp-*` produces this; the value carries no timezone.
      // Refusing the conversion is more correct than silently picking UTC and
      // returning wrong results for non-UTC producers. See the design doc
      // "Avro logical-type scope" section.
      throw new IllegalArgumentException(
          "Cannot convert LocalDateTime to Timestamp: local-timestamp values "
              + "carry no timezone. Use the regular timestamp-* logical type "
              + "(UTC by spec), or carry a separate TZ-offset field and use "
              + "to_timestamp(value, unit) on the offset-adjusted epoch value.");
    }
    if (o instanceof OffsetDateTime) {
      return fromInstant(((OffsetDateTime) o).toInstant());
    }
    if (o instanceof ZonedDateTime) {
      return fromInstant(((ZonedDateTime) o).toInstant());
    }
    if (o instanceof String) {
      return fromRfc3339((String) o);
    }
    if (o instanceof Long || o instanceof Integer) {
      throw new IllegalArgumentException(
          "Cannot convert raw " + o.getClass().getSimpleName() + " to Timestamp without "
              + "a unit; use to_timestamp(value, \"millis\"|\"micros\"|\"nanos\"|"
              + "\"seconds\") or set useLogicalTypeConverters=true on the Avro client so "
              + "timestamp fields arrive as Instant");
    }
    throw new IllegalArgumentException(
        "Cannot convert " + o.getClass().getName() + " to Timestamp");
  }
}
