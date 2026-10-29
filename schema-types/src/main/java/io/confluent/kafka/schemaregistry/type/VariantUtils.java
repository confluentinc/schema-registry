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

package io.confluent.kafka.schemaregistry.type;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.StreamWriteFeature;
import com.fasterxml.jackson.core.json.JsonReadFeature;
import com.fasterxml.jackson.core.json.JsonWriteFeature;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.json.JsonMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.nio.ByteBuffer;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.ZoneOffset;
import java.util.Base64;
import java.util.Iterator;
import java.util.Locale;
import java.util.Map;

/**
 * Converts between Jackson {@link JsonNode} and {@link Variant} (metadata + value binary pair).
 */
public class VariantUtils {

  /**
   * Converts a Jackson JsonNode into a Variant.
   *
   * @param node the JSON node to convert
   * @return a Variant containing the encoded metadata and value
   */
  private static final JsonNodeFactory FACTORY = JsonNodeFactory.instance;

  /**
   * Shared Jackson mapper for both {@link #fromJson} and {@link #toJsonString}.
   * ALLOW_NON_NUMERIC_NUMBERS reads bareword {@code NaN}/{@code Infinity}/{@code -Infinity};
   * WRITE_BIGDECIMAL_AS_PLAIN renders decimals in fixed-point form (never scientific), the
   * cross-language contract for variant JSON; WRITE_NAN_AS_STRINGS is disabled so non-finite
   * doubles/floats are written as barewords ({@code NaN}) rather than quoted strings
   * ({@code "NaN"}).
   */
  private static final ObjectMapper JSON_MAPPER = JsonMapper.builder()
      .enable(JsonReadFeature.ALLOW_NON_NUMERIC_NUMBERS)
      .enable(StreamWriteFeature.WRITE_BIGDECIMAL_AS_PLAIN)
      .disable(JsonWriteFeature.WRITE_NAN_AS_STRINGS)
      .build();

  /**
   * Converts a Variant into a Jackson JsonNode.
   *
   * <p>Decimals are returned as a {@link com.fasterxml.jackson.databind.node.DecimalNode},
   * so their serialized text depends on the caller's generator: a default generator may emit
   * scientific notation for small-magnitude decimals. {@link #toJsonString} is the canonical
   * string form and always renders decimals in fixed-point.
   *
   * @param variant the Variant to convert
   * @return a JsonNode representing the variant value
   */
  public static JsonNode toJsonNode(Variant variant) {
    switch (variant.getType()) {
      case OBJECT:
        return objectToJson(variant);
      case ARRAY:
        return arrayToJson(variant);
      case STRING:
        return FACTORY.textNode(variant.getString());
      case BYTE:
        return FACTORY.numberNode(variant.getByte());
      case SHORT:
        return FACTORY.numberNode(variant.getShort());
      case INT:
        return FACTORY.numberNode(variant.getInt());
      case LONG:
        return FACTORY.numberNode(variant.getLong());
      case FLOAT:
        return FACTORY.numberNode(variant.getFloat());
      case DOUBLE:
        return FACTORY.numberNode(variant.getDouble());
      case DECIMAL4:
      case DECIMAL8:
      case DECIMAL16:
        return FACTORY.numberNode(variant.getDecimal());
      case BOOLEAN:
        return FACTORY.booleanNode(variant.getBoolean());
      case NULL:
        return FACTORY.nullNode();
      case DATE:
        return FACTORY.textNode(LocalDate.ofEpochDay(checkDateRange(variant.getInt())).toString());
      case TIMESTAMP_TZ:
        return FACTORY.textNode(instantFromMicros(variant.getLong()).toString());
      case TIMESTAMP_NTZ:
        return FACTORY.textNode(formatLocalDateTimeMicros(variant.getLong()));
      case TIMESTAMP_NANOS_TZ:
        return FACTORY.textNode(
            Instant.ofEpochSecond(0, variant.getLong()).toString());
      case TIMESTAMP_NANOS_NTZ:
        return FACTORY.textNode(formatLocalDateTimeNanos(variant.getLong()));
      case TIME:
        return FACTORY.textNode(formatLocalTime(variant.getLong()));
      case BINARY:
        ByteBuffer bin = variant.getBinary();
        byte[] binBytes = new byte[bin.remaining()];
        bin.get(binBytes);
        return FACTORY.textNode(Base64.getEncoder().encodeToString(binBytes));
      case UUID:
        return FACTORY.textNode(variant.getUUID().toString());
      default:
        throw new IllegalArgumentException("Unsupported variant type: " + variant.getType());
    }
  }

  private static JsonNode objectToJson(Variant variant) {
    ObjectNode obj = FACTORY.objectNode();
    for (int i = 0; i < variant.numObjectFields(); i++) {
      Variant.ObjectField field = variant.getFieldAtIndex(i);
      obj.set(field.key, toJsonNode(field.value));
    }
    return obj;
  }

  private static JsonNode arrayToJson(Variant variant) {
    ArrayNode arr = FACTORY.arrayNode();
    for (int i = 0; i < variant.numArrayElements(); i++) {
      arr.add(toJsonNode(variant.getElementAtIndex(i)));
    }
    return arr;
  }

  /**
   * Converts a Variant into a JSON string. This is the canonical cross-language form:
   * decimals are fixed-point (never scientific) and temporal types are ISO-8601 with the
   * seconds field always present.
   *
   * @param variant the Variant to convert
   * @return the JSON string representation
   */
  public static String toJsonString(Variant variant) {
    try {
      return JSON_MAPPER.writeValueAsString(toJsonNode(variant));
    } catch (JsonProcessingException e) {
      // toJsonNode only produces standard scalar/container nodes, so this is not reachable.
      throw new IllegalStateException("Failed to serialize variant to JSON", e);
    }
  }

  /**
   * The range a timestamp may occupy when rendered to JSON, in microseconds since the epoch:
   * 0001-01-01T00:00:00 through 9999-12-31T23:59:59.999999.
   *
   * <p>This is the four-digit-year form every client renders. For the zone-aware types it is
   * RFC 3339 - also {@code google.protobuf.Timestamp}'s range, and the range
   * {@code timestamp(...)} enforces when constructing a CEL timestamp. The zone-less types carry no
   * offset, so they are ISO-8601 local date-times rather than RFC 3339 (which has no zone-less
   * form); they share the range so both stay readable by the same date parsers.
   *
   * <p>A variant TIMESTAMP_TZ / TIMESTAMP_NTZ is an arbitrary int64 of microseconds - roughly
   * +/-292,471 years - so it can hold instants outside that form. Rendering those as ISO-8601's
   * expanded year ({@code +10000-01-01T00:00:00Z}) would emit JSON that most parsers,
   * {@code variants.parseJson} included, reject; so they are refused instead, as protobuf's
   * {@code Timestamps.toString} does. It is also exactly what Python's {@code datetime} and .NET's
   * {@code DateTime} can hold, so every client can enforce it natively.
   *
   * <p>The nanosecond-based types need no such check: an int64 of nanoseconds spans only
   * 1677-2262, which is inside this range at both ends.
   */
  private static final long MIN_TIMESTAMP_MICROS = -62135596800000000L;
  private static final long MAX_TIMESTAMP_MICROS = 253402300799999999L;

  /**
   * The range a DATE may occupy when rendered to JSON, in days since the epoch: 0001-01-01 through
   * 9999-12-31. RFC 3339's {@code full-date} requires {@code date-fullyear = 4DIGIT}, so an
   * expanded or negative year ({@code +10000-01-01}, {@code -0044-01-01}) is not a valid
   * {@code full-date}. A variant DATE is an int32 of days - roughly +/-5.8 million years - so
   * those are reachable, and are refused rather than rendered.
   */
  private static final int MIN_DATE_EPOCH_DAY = -719162;
  private static final int MAX_DATE_EPOCH_DAY = 2932896;

  private static int checkDateRange(int epochDay) {
    if (epochDay < MIN_DATE_EPOCH_DAY || epochDay > MAX_DATE_EPOCH_DAY) {
      throw new IllegalArgumentException(
          "Date is not valid. Epoch day (" + epochDay + ") must be in range ["
              + MIN_DATE_EPOCH_DAY + ", " + MAX_DATE_EPOCH_DAY + "].");
    }
    return epochDay;
  }

  /**
   * The range a TIME may occupy, in microseconds since midnight: 00:00:00 through
   * 23:59:59.999999. RFC 3339's {@code partial-time} requires {@code time-hour = 2DIGIT} in
   * 00-23, so a value at or past 24 hours (or negative) has no valid form. A variant TIME is an
   * int64 of microseconds, so those are reachable; checking also removes an overflow, since
   * {@code micros * 1000} wraps for a large enough value.
   */
  private static final long MIN_TIME_MICROS = 0L;
  private static final long MAX_TIME_MICROS = 86_400_000_000L - 1L;

  private static long checkTimeRange(long micros) {
    if (micros < MIN_TIME_MICROS || micros > MAX_TIME_MICROS) {
      throw new IllegalArgumentException(
          "Time is not valid. Microseconds of day (" + micros + ") must be in range ["
              + MIN_TIME_MICROS + ", " + MAX_TIME_MICROS + "].");
    }
    return micros;
  }

  private static long checkMicrosRange(long micros) {
    if (micros < MIN_TIMESTAMP_MICROS || micros > MAX_TIMESTAMP_MICROS) {
      throw new IllegalArgumentException(
          "Timestamp is not valid. Microseconds (" + micros + ") must be in range ["
              + MIN_TIMESTAMP_MICROS + ", " + MAX_TIMESTAMP_MICROS + "].");
    }
    return micros;
  }

  /**
   * Builds an {@link Instant} from microseconds since the epoch, splitting into whole seconds
   * before scaling to nanoseconds.
   *
   * <p>The direct form, {@code Instant.ofEpochSecond(0, micros * 1000)}, overflows a long for any
   * value past {@code 9223372036854775} micros - 2262-04-11T23:47:16.854775Z - and wrapped
   * silently to a plausible-looking date well in the past (year 10000 rendered as 1816). This is
   * the same floorDiv/floorMod split {@link #formatLocalDateTimeMicros} already used, which is why
   * the zone-less arm never had that bug.
   */
  private static Instant instantFromMicros(long micros) {
    checkMicrosRange(micros);
    return Instant.ofEpochSecond(
        Math.floorDiv(micros, 1_000_000L), Math.floorMod(micros, 1_000_000L) * 1000L);
  }

  // Formats an NTZ timestamp / time to ISO-8601 with the seconds field ALWAYS present. This
  // is the cross-language contract: it deviates from LocalDateTime/LocalTime.toString(),
  // which omit the seconds field when both seconds and fraction are zero, so that the NTZ
  // form stays consistent with the TZ (Instant) form. The fractional-second field uses the
  // same 0/3/6/9-digit grouping as Instant.toString().
  private static String formatLocalDateTimeMicros(long micros) {
    checkMicrosRange(micros);
    return formatLocalDateTime(
        Math.floorDiv(micros, 1_000_000L), (int) Math.floorMod(micros, 1_000_000L) * 1000);
  }

  private static String formatLocalDateTimeNanos(long nanos) {
    return formatLocalDateTime(
        Math.floorDiv(nanos, 1_000_000_000L), (int) Math.floorMod(nanos, 1_000_000_000L));
  }

  private static String formatLocalDateTime(long epochSecond, int nanoOfSecond) {
    LocalDateTime dt = LocalDateTime.ofEpochSecond(epochSecond, nanoOfSecond, ZoneOffset.UTC);
    // Locale.ROOT: %d must emit ASCII digits regardless of the JVM's default locale
    // (a locale with non-ASCII native digits would otherwise corrupt the ISO-8601 output).
    return String.format(Locale.ROOT, "%04d-%02d-%02dT%02d:%02d:%02d%s",
        dt.getYear(), dt.getMonthValue(), dt.getDayOfMonth(),
        dt.getHour(), dt.getMinute(), dt.getSecond(), fractionOfSecond(dt.getNano()));
  }

  private static String formatLocalTime(long micros) {
    LocalTime time = LocalTime.ofNanoOfDay(checkTimeRange(micros) * 1000);
    return String.format(Locale.ROOT, "%02d:%02d:%02d%s",
        time.getHour(), time.getMinute(), time.getSecond(), fractionOfSecond(time.getNano()));
  }

  // Fractional-second suffix using Instant.toString()'s 0/3/6/9-digit grouping (empty for 0).
  private static String fractionOfSecond(int nano) {
    if (nano == 0) {
      return "";
    }
    if (nano % 1_000_000 == 0) {
      return String.format(Locale.ROOT, ".%03d", nano / 1_000_000);
    }
    if (nano % 1_000 == 0) {
      return String.format(Locale.ROOT, ".%06d", nano / 1_000);
    }
    return String.format(Locale.ROOT, ".%09d", nano);
  }

  /**
   * Parses a JSON string into a Variant using the shared mapper. Bareword non-finite
   * numbers ({@code NaN}, {@code Infinity}, {@code -Infinity}) are accepted, as are
   * out-of-range magnitudes ({@code 1e400} becomes {@code Infinity}).
   *
   * @param json the JSON string to parse
   * @return a Variant containing the encoded metadata and value
   * @throws IllegalArgumentException if the JSON cannot be parsed
   */
  public static Variant fromJson(String json) {
    try {
      JsonNode node = JSON_MAPPER.readTree(json);
      if (node == null) {
        // readTree returns null for empty or whitespace-only input.
        throw new IllegalArgumentException("Cannot parse JSON for variant: empty input");
      }
      return fromJsonNode(node);
    } catch (JsonProcessingException | IllegalStateException e) {
      throw new IllegalArgumentException("Cannot parse JSON for variant: " + e.getMessage(), e);
    }
  }

  /**
   * Converts a Jackson JsonNode into a Variant.
   *
   * @param node the JSON node to convert
   * @return a Variant containing the encoded metadata and value
   */
  public static Variant fromJsonNode(JsonNode node) {
    VariantBuilder builder = new VariantBuilder();
    buildValue(builder, node);
    return builder.build();
  }

  private static void buildValue(VariantBuilder builder, JsonNode node) {
    switch (node.getNodeType()) {
      case OBJECT:
        buildObject(builder, node);
        break;
      case ARRAY:
        buildArray(builder, node);
        break;
      case STRING:
        builder.appendString(node.textValue());
        break;
      case NUMBER:
        buildNumber(builder, node);
        break;
      case BOOLEAN:
        builder.appendBoolean(node.booleanValue());
        break;
      case NULL:
        builder.appendNull();
        break;
      default:
        throw new IllegalArgumentException("Unsupported JSON node type: " + node.getNodeType());
    }
  }

  private static void buildObject(VariantBuilder builder, JsonNode node) {
    VariantObjectBuilder obj = builder.startObject();
    Iterator<Map.Entry<String, JsonNode>> fields = node.fields();
    while (fields.hasNext()) {
      Map.Entry<String, JsonNode> field = fields.next();
      obj.appendKey(field.getKey());
      buildValue(obj, field.getValue());
    }
    builder.endObject();
  }

  private static void buildArray(VariantBuilder builder, JsonNode node) {
    VariantArrayBuilder arr = builder.startArray();
    for (JsonNode element : node) {
      buildValue(arr, element);
    }
    builder.endArray();
  }

  private static void buildNumber(VariantBuilder builder, JsonNode node) {
    if (node.isInt()) {
      builder.appendInt(node.intValue());
    } else if (node.isLong()) {
      builder.appendLong(node.longValue());
    } else if (node.isFloat()) {
      builder.appendFloat(node.floatValue());
    } else if (node.isDouble()) {
      builder.appendDouble(node.doubleValue());
    } else if (node.isBigDecimal()) {
      builder.appendDecimal(node.decimalValue());
    } else if (node.isBigInteger()) {
      builder.appendDecimal(new java.math.BigDecimal(node.bigIntegerValue()));
    } else if (node.isShort()) {
      builder.appendShort(node.shortValue());
    } else {
      // Fallback for any other numeric type
      builder.appendDouble(node.doubleValue());
    }
  }
}
