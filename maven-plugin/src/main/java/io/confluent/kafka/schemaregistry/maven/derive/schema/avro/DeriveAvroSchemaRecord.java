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
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import io.confluent.kafka.schemaregistry.maven.derive.schema.DeriveSchema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Optional;

import static io.confluent.kafka.schemaregistry.maven.derive.schema.DeriveSchema.mapper;

public class DeriveAvroSchemaRecord {

  private static final Logger logger = LoggerFactory.getLogger(DeriveAvroSchema.class);

  private static void checkName(String name, boolean typeProtoBuf) {

    if (name.length() == 0) {
      String message = String.format("Message %d: Name of a field cannot be Empty.",
          DeriveAvroSchema.getCurrentMessage());
      logger.error(message);
      throw new IllegalArgumentException(message);
    }

    if (name.matches("\\d.*")) {
      String message = String.format("Message %d: Name of a field cannot begin with digit.",
          DeriveAvroSchema.getCurrentMessage());
      logger.error(message);
      throw new IllegalArgumentException(message);

    }

    if (typeProtoBuf && name.contains(".")) {
      String message = String.format("Message %d: Name of field cannot contain '.'",
          DeriveAvroSchema.getCurrentMessage());
      logger.error(message);
      throw new IllegalArgumentException(message);
    }
  }

  public static ObjectNode getSchemaForRecord(ObjectNode objectNode, String name,
                                              boolean strictCheck, boolean typeProtoBuf)
      throws JsonProcessingException {
    return getSchemaForRecord(objectNode, name, strictCheck, typeProtoBuf, false);
  }

  public static ObjectNode getSchemaForRecord(ObjectNode message,
                                              String name,
                                              boolean strictCheck,
                                              boolean typeProtoBuf,
                                              boolean calledAsField)
      throws JsonProcessingException {

    ObjectNode schema = mapper.createObjectNode();
    if (typeProtoBuf) {
      schema.put("__type", "record");
    }
    schema.put("name", name);

    if (!calledAsField) {
      schema.put("type", "record");
    }
    schema.set("fields", mapper.createArrayNode());

    for (String key : DeriveSchema.getSortedKeys(message)) {

      checkName(key, typeProtoBuf);
      Object field = message.get(key);
      Optional<ObjectNode> primitiveSchema = DeriveAvroSchemaPrimitive.getPrimitiveSchema(
          field, strictCheck, typeProtoBuf);

      if (primitiveSchema.isPresent()) {
        ArrayNode fields = (ArrayNode) schema.get("fields");
        ObjectNode primitiveType = mapper.createObjectNode();
        primitiveType.put("name", key);
        primitiveType.set("type", primitiveSchema.get().get("type"));
        fields.add(primitiveType);
      } else {

        ObjectNode info;
        if (field instanceof ArrayNode) {
          info = DeriveAvroSchemaArray.getSchemaForArray(DeriveSchema.getListFromArray(field),
              key, strictCheck, typeProtoBuf, true, false);
        } else {
          info = getSchemaForRecord(mapper.valueToTree(field), key, strictCheck,
              typeProtoBuf, true);
        }
        ArrayNode fields = (ArrayNode) schema.get("fields");
        fields.add(info);
      }
    }

    if (calledAsField) {
      ObjectNode recursive = mapper.createObjectNode();
      recursive.put("name", name);
      recursive.set("fields", schema.get("fields"));
      recursive.put("type", "record");

      if (typeProtoBuf) {
        recursive.put("__type", "record");
      }

      schema.remove("fields");
      schema.set("type", recursive);
    }

    return schema;
  }
}