/*
 * Copyright 2020 Confluent Inc.
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

    classToDataType.put("IntNode", "number");
    classToDataType.put("LongNode", "number");
    classToDataType.put("BooleanNode", "boolean");
    classToDataType.put("NullNode", "null");
    classToDataType.put("DoubleNode", "number");
    classToDataType.put("TextNode", "string");
    classToDataType.put("BigIntegerNode", "number");

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
      jsonInferredType = "NullNode";
    } else {
      jsonInferredType = field.getClass().getSimpleName();
    }

    if (classToDataType.containsKey(jsonInferredType)) {
      String schemaString = String.format("{\"type\" : \"%s\"}",
          classToDataType.get(jsonInferredType));
      return Optional.of(mapper.readValue(schemaString, ObjectNode.class));
    }

    return Optional.empty();
  }
}
