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
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import io.confluent.kafka.schemaregistry.maven.derive.schema.DeriveSchema;

import java.util.Optional;

import static io.confluent.kafka.schemaregistry.maven.derive.schema.DeriveSchema.mapper;

public class DeriveJsonSchemaRecord {
  public static ObjectNode getSchemaForRecord(ObjectNode message, String name)
      throws JsonProcessingException {

    ObjectNode schema = mapper.createObjectNode();
    schema.put("type", "object");
    schema.set("properties", mapper.createObjectNode());

    for (String key : DeriveSchema.getSortedKeys(message)) {

      Object field = message.get(key);
      Optional<ObjectNode> primitiveSchema = DeriveJsonSchemaPrimitive.getPrimitiveSchema(field);
      ObjectNode info;

      if (primitiveSchema.isPresent()) {
        info = primitiveSchema.get();
      } else {
        if (field instanceof ArrayNode) {
          info = DeriveJsonSchemaArray.getSchemaForArray(DeriveSchema.getListFromArray(field), key);
        } else {
          info = getSchemaForRecord(mapper.valueToTree(field), key);
        }
      }

      ObjectNode fields = (ObjectNode) schema.get("properties");
      fields.set(key, info);
    }

    return schema;
  }

}