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

import com.fasterxml.jackson.databind.JsonNode;
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
import java.util.Map;

/**
 * Converts between Jackson {@link JsonNode} and {@link Variant} (metadata + value binary pair).
 */
public class VariantJsonConverter {

  /**
   * Converts a Jackson JsonNode into a Variant.
   *
   * @param node the JSON node to convert
   * @return a Variant containing the encoded metadata and value
   */
  private static final JsonNodeFactory FACTORY = JsonNodeFactory.instance;

  /**
   * Converts a Variant into a Jackson JsonNode.
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
      case TIMESTAMP_NTZ: {
        long micros = variant.getLong();
        return FACTORY.textNode(
            LocalDateTime.ofEpochSecond(
                Math.floorDiv(micros, 1_000_000L),
                (int) Math.floorMod(micros, 1_000_000L) * 1000,
                ZoneOffset.UTC).toString());
      }
      case TIMESTAMP_NANOS_TZ:
        return FACTORY.textNode(
            Instant.ofEpochSecond(0, variant.getLong()).toString());
      case TIMESTAMP_NANOS_NTZ: {
        long nanos = variant.getLong();
        return FACTORY.textNode(
            LocalDateTime.ofEpochSecond(
                Math.floorDiv(nanos, 1_000_000_000L),
                (int) Math.floorMod(nanos, 1_000_000_000L),
                ZoneOffset.UTC).toString());
      }
      case TIME:
        return FACTORY.textNode(
            LocalTime.ofNanoOfDay(variant.getLong() * 1000).toString());
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
    for (int i = 0; i < variant.numObjectElements(); i++) {
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
   * Converts a Variant into a JSON string.
   *
   * @param variant the Variant to convert
   * @return the JSON string representation
   */
  public static String toJsonString(Variant variant) {
    return toJsonNode(variant).toString();
  }

  /**
   * Converts a Jackson JsonNode into a Variant.
   *
   * @param node the JSON node to convert
   * @return a Variant containing the encoded metadata and value
   */
  public static Variant toVariant(JsonNode node) {
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
