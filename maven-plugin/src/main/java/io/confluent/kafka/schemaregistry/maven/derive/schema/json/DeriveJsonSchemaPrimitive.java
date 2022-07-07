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

package io.confluent.kafka.schemaregistry.maven.derive.schema.json;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.node.ObjectNode;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

import static io.confluent.kafka.schemaregistry.maven.derive.schema.DeriveSchema.mapper;

public class DeriveJsonSchemaPrimitive {
  private static final Map<String, String> classToDataType = new HashMap<>();

  static {

    classToDataType.put(com.fasterxml.jackson.databind.node.DoubleNode.class.getName(), "number");
    classToDataType.put(com.fasterxml.jackson.databind.node.TextNode.class.getName(), "string");
    classToDataType.put(com.fasterxml.jackson.databind.node.BigIntegerNode.class.getName(),
        "number");
    classToDataType.put(com.fasterxml.jackson.databind.node.IntNode.class.getName(), "number");
    classToDataType.put(com.fasterxml.jackson.databind.node.LongNode.class.getName(), "number");
    classToDataType.put(com.fasterxml.jackson.databind.node.BooleanNode.class.getName(),
        "boolean");
    classToDataType.put(com.fasterxml.jackson.databind.node.NullNode.class.getName(), "null");
  }

  /**
   * Primitive schema of message is generated, if possible.
   *
   * @param field - message whose schema has to be found
   * @return ObjectNode if type primitive else empty option
   */
  static Optional<ObjectNode> getPrimitiveSchema(Object field) throws JsonProcessingException {

    String jsonInferredType;
    if (field == null) {
      jsonInferredType = com.fasterxml.jackson.databind.node.NullNode.class.getName();
    } else {
      jsonInferredType = field.getClass().getName();
    }

    if (classToDataType.containsKey(jsonInferredType)) {
      String schemaString = String.format("{\"type\" : \"%s\"}",
          classToDataType.get(jsonInferredType));
      return Optional.of(mapper.readValue(schemaString, ObjectNode.class));
    }

    return Optional.empty();
  }

}
