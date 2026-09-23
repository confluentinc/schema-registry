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

package io.confluent.kafka.schemaregistry.client.rest.entities;

import java.math.BigDecimal;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Base64;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * The wire form of a {@link ProvenanceField} default: a normalised Java value, as the schema
 * readers produce it, turned into something that serialises to JSON without losing information.
 *
 * <p>The rules are driven by the value alone, so a server can encode without knowing the column's
 * type; a consumer decodes by the column type it already has. Temporal forms are RFC 3339 and
 * carry as many fractional digits as the value needs — none, three, six or nine — so a microsecond
 * or nanosecond default survives. A decimal is a plain string, never exponent notation. Non-finite
 * floating-point values, which JSON has no number for, are the strings {@code "NaN"}, {@code
 * "Infinity"} and {@code "-Infinity"}, as in Protobuf's JSON mapping.
 */
public final class ProvenanceDefaults {

  private static final Set<Class<?>> PASS_THROUGH = new HashSet<>(Arrays.asList(
      String.class, Boolean.class, Byte.class, Short.class, Integer.class, Long.class));

  private ProvenanceDefaults() {
  }

  /**
   * The wire form of {@code value}, or {@code null} for {@code null}.
   *
   * @throws IllegalArgumentException for a value the schema readers never produce — a struct
   *     default, for instance, which the report deliberately leaves out
   */
  public static Object encode(Object value) {
    if (value == null || passesThrough(value)) {
      return value;
    }
    if (value instanceof Float) {
      float f = (Float) value;
      return Float.isFinite(f) ? value : nonFinite(f);
    }
    if (value instanceof Double) {
      double d = (Double) value;
      return Double.isFinite(d) ? value : nonFinite(d);
    }
    if (value instanceof BigDecimal) {
      return ((BigDecimal) value).toPlainString();
    }
    if (value instanceof LocalDate) {
      return DateTimeFormatter.ISO_LOCAL_DATE.format((LocalDate) value);
    }
    if (value instanceof LocalTime) {
      return DateTimeFormatter.ISO_LOCAL_TIME.format((LocalTime) value);
    }
    if (value instanceof LocalDateTime) {
      return DateTimeFormatter.ISO_LOCAL_DATE_TIME.format((LocalDateTime) value);
    }
    if (value instanceof Instant) {
      return DateTimeFormatter.ISO_INSTANT.format((Instant) value);
    }
    if (value instanceof byte[]) {
      return Base64.getEncoder().encodeToString((byte[]) value);
    }
    if (value instanceof List) {
      List<Object> encoded = new ArrayList<>();
      for (Object element : (List<?>) value) {
        encoded.add(encode(element));
      }
      return encoded;
    }
    if (value instanceof Map) {
      return encodeMap((Map<?, ?>) value);
    }
    throw new IllegalArgumentException(
        "No wire form for a default of type " + value.getClass().getName());
  }

  /**
   * A map with string keys is a JSON object; any other key type cannot be a JSON object key, so
   * the map becomes an array of {@code {"key": ..., "value": ...}} pairs, in iteration order.
   */
  private static Object encodeMap(Map<?, ?> map) {
    boolean stringKeys = map.keySet().stream().allMatch(k -> k instanceof String);
    if (stringKeys) {
      Map<String, Object> encoded = new LinkedHashMap<>();
      map.forEach((k, v) -> encoded.put((String) k, encode(v)));
      return encoded;
    }
    List<Object> pairs = new ArrayList<>();
    map.forEach((k, v) -> {
      Map<String, Object> pair = new LinkedHashMap<>();
      pair.put("key", encode(k));
      pair.put("value", encode(v));
      pairs.add(pair);
    });
    return pairs;
  }

  /** A value JSON represents exactly as it is. */
  private static boolean passesThrough(Object value) {
    return PASS_THROUGH.contains(value.getClass());
  }

  private static String nonFinite(double value) {
    if (Double.isNaN(value)) {
      return "NaN";
    }
    return value > 0 ? "Infinity" : "-Infinity";
  }
}
