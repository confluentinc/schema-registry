/*
 * Copyright 2022 Confluent Inc.
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

package io.confluent.kafka.schemaregistry.maven.derive.schema.avro;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

import static io.confluent.kafka.schemaregistry.maven.derive.schema.DeriveSchema.mapper;

public class DeriveAvroSchemaPrimitive {

  private static final Logger logger = LoggerFactory.getLogger(DeriveAvroSchema.class);
  private static final Map<String, String> classToDataTypeAvro = new HashMap<>();
  private static final Map<String, String> classToDataTypeProtoBuf = new HashMap<>();

  static {

    // Mapping class to data type for avro
    classToDataTypeAvro.put(com.fasterxml.jackson.databind.node.DoubleNode.class.getName(),
        "double");
    classToDataTypeAvro.put(com.fasterxml.jackson.databind.node.TextNode.class.getName(),
        "string");
    classToDataTypeAvro.put(com.fasterxml.jackson.databind.node.BigIntegerNode.class.getName(),
        "double");
    classToDataTypeAvro.put(com.fasterxml.jackson.databind.node.IntNode.class.getName(),
        "int");
    classToDataTypeAvro.put(com.fasterxml.jackson.databind.node.LongNode.class.getName(),
        "long");
    classToDataTypeAvro.put(com.fasterxml.jackson.databind.node.BooleanNode.class.getName(),
        "boolean");
    classToDataTypeAvro.put(com.fasterxml.jackson.databind.node.NullNode.class.getName(),
        "null");

    // Mapping class to data type for ProtoBuf
    classToDataTypeProtoBuf.put(com.fasterxml.jackson.databind.node.DoubleNode.class.getName(),
        "double");
    classToDataTypeProtoBuf.put(com.fasterxml.jackson.databind.node.TextNode.class.getName(),
        "string");
    classToDataTypeProtoBuf.put(com.fasterxml.jackson.databind.node.BigIntegerNode.class.getName(),
        "double");
    classToDataTypeProtoBuf.put(com.fasterxml.jackson.databind.node.IntNode.class.getName(),
        "int32");
    classToDataTypeProtoBuf.put(com.fasterxml.jackson.databind.node.LongNode.class.getName(),
        "int64");
    classToDataTypeProtoBuf.put(com.fasterxml.jackson.databind.node.BooleanNode.class.getName(),
        "bool");
    classToDataTypeProtoBuf.put(com.fasterxml.jackson.databind.node.NullNode.class.getName(),
        "google.protobuf.Any");

  }

  private static final String messageOutOfRangeError = "Message %d: Numeric value %s "
      + "out of range of long " + "(-9223372036854775808 9223372036854775807).";

  private static String getOutOfRangeError(int currentMessage, Object field) {
    return String.format(messageOutOfRangeError, currentMessage, field);
  }

  private static String getOutOfRangeWarning(int currentMessage, Object field) {
    return getOutOfRangeError(currentMessage, field) + " Mapping to double.";
  }


  /**
   * Get schema for primitive types - numeric types, boolean, string and null
   * Checks for class and in its presence in Map classToDataType
   * Returns ObjectNode with name and type
   *
   * @param field - message whose schema has to be found
   * @return ObjectNode if type primitive else empty option
   */
  public static Optional<ObjectNode> getPrimitiveSchema(Object field,
                                                        boolean strictCheck,
                                                        boolean typeProtoBuf)
      throws IllegalArgumentException, JsonProcessingException {

    String jsonInferredType;

    if (field == null) {
      jsonInferredType = com.fasterxml.jackson.databind.node.NullNode.class.getName();
    } else {
      jsonInferredType = field.getClass().getName();
    }

    if (jsonInferredType.equals(
        com.fasterxml.jackson.databind.node.BigIntegerNode.class.getName())) {
      if (strictCheck) {
        String errorMessage = getOutOfRangeError(DeriveAvroSchema.getCurrentMessage(), field);
        logger.error(errorMessage);
        throw new IllegalArgumentException(errorMessage);
      } else {
        ObjectNode objectNode = mapper.createObjectNode();
        objectNode.put("type", "double");
        logger.warn(getOutOfRangeWarning(DeriveAvroSchema.getCurrentMessage(), field));
        return Optional.of(objectNode);
      }
    }

    if (classToDataTypeAvro.containsKey(jsonInferredType)) {

      String schemaString;
      if (typeProtoBuf) {
        schemaString = String.format("{\"type\" : \"%s\"}",
            classToDataTypeProtoBuf.get(jsonInferredType));
      } else {
        schemaString = String.format("{\"type\" : \"%s\"}",
            classToDataTypeAvro.get(jsonInferredType));
      }
      return Optional.of(mapper.readValue(schemaString, ObjectNode.class));
    }

    return Optional.empty();
  }

}
