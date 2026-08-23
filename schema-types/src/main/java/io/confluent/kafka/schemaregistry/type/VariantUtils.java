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
   * ({@code "NaN"}) — a deliberate divergence from Spark, keeping the round-trip symmetric.
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
        return FACTORY.textNode(LocalDate.ofEpochDay(variant.getInt()).toString());
      case TIMESTAMP_TZ:
        return FACTORY.textNode(
            Instant.ofEpochSecond(0, variant.getLong() * 1000).toString());
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

  // Formats an NTZ timestamp / time to ISO-8601 with the seconds field ALWAYS present. This
  // is the cross-language contract: it deviates from LocalDateTime/LocalTime.toString(),
  // which omit the seconds field when both seconds and fraction are zero, so that the NTZ
  // form stays consistent with the TZ (Instant) form. The fractional-second field uses the
  // same 0/3/6/9-digit grouping as Instant.toString().
  private static String formatLocalDateTimeMicros(long micros) {
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
    LocalTime time = LocalTime.ofNanoOfDay(micros * 1000);
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
      return fromJsonNode(JSON_MAPPER.readTree(json));
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
