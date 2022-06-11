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

package io.confluent.kafka.schemaregistry.maven.derive.schema.utils;

import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.TextNode;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Objects;

import static io.confluent.kafka.schemaregistry.maven.derive.schema.DeriveSchema.getSortedKeys;

/**
 * Utility class for adjusting int, long, float and double values in avro and protobuf.
 */
public final class MergeNumberUtils {

  /**
   * Merge together fields of type int, long, float and double.
   *
   * @param schemaList list of schemas to merge
   */
  public static void mergeNumberTypes(ArrayList<ObjectNode> schemaList, boolean matchAll) {

    if (schemaList.size() <= 1) {
      return;
    }

    /*
    O(n) checking is done only with 1st schema, will fail in the following scenarios:
    1. A field is absent in the first schema, hence that field is never checked in other messages
    2. A field is present in the first schema, but of type non-Number
    but rest of the messages are of number type.
     */
    if (matchAll) {
      for (ObjectNode currentSchema : schemaList) {
        for (ObjectNode startingSchema : schemaList) {
          adjustNumberTypes(startingSchema, currentSchema);
        }
      }
    } else {

      for (ObjectNode currentSchema : schemaList) {
        adjustNumberTypes(schemaList.get(0), currentSchema);
      }

      for (ObjectNode currentSchema : schemaList) {
        adjustNumberTypes(schemaList.get(0), currentSchema);
      }

    }

  }

  /**
   * Function to match fields of int, long, float and double values.
   *
   * @param schema1 schema to compare
   * @param schema2 schema to compare
   */
  public static void adjustNumberTypes(ObjectNode schema1, ObjectNode schema2) {

    if (checkName(schema1, schema2)) {
      return;
    }

    for (String key : getSortedKeys(schema1)) {

      if (!schema2.has(key) || (schema1.get(key).getClass() != schema2.get(key).getClass())) {
        continue;
      }

      if (schema1.get(key) instanceof TextNode) {
        matchTypes(schema1, schema2, key);
      } else if (schema1.get(key) instanceof ArrayNode) {
        matchJsonArray(schema1, schema2, key);
      } else if (schema1.get(key) instanceof ObjectNode) {
        adjustNumberTypes((ObjectNode) schema1.get(key), (ObjectNode) schema2.get(key));
      }

    }

  }

  private static boolean checkName(ObjectNode schema1, ObjectNode schema2) {

    if (schema1.equals(schema2)) {
      return true;
    }
    return schema1.has("name") && schema2.has("name")
        && !schema1.get("name").equals(schema2.get("name"));
  }

  private static void matchTypes(ObjectNode schema1, ObjectNode schema2, String key) {

    if (key.equals("name")) {
      return;
    }

    String field1Type = schema1.get(key).textValue();
    String field2Type = schema2.get(key).textValue();
    ArrayList<String> dataTypes = new ArrayList<>(Arrays.asList("int", "int32", "int64", "long"));
    if (Objects.equals(field2Type, "double") && dataTypes.contains(field1Type)) {
      schema1.set(key, schema2.get(key));
    } else if (Objects.equals(field1Type, "double") && dataTypes.contains(field2Type)) {
      schema2.set(key, schema1.get(key));
    } else if (Objects.equals(field2Type, "long") && dataTypes.contains(field1Type)) {
      schema1.set(key, schema2.get(key));
    } else if (Objects.equals(field1Type, "long") && dataTypes.contains(field2Type)) {
      schema2.set(key, schema1.get(key));
    }
  }

  private static void matchJsonArray(ObjectNode schema1, ObjectNode schema2, String key) {

    ArrayNode fieldItems1 = (ArrayNode) schema1.get(key);
    ArrayNode fieldsItems2 = (ArrayNode) schema2.get(key);

    for (int i = 0; i < fieldItems1.size(); i++) {
      for (int j = 0; j < fieldsItems2.size(); j++) {

        if ((fieldItems1.get(i) instanceof ObjectNode)
            && (fieldsItems2.get(j) instanceof ObjectNode)) {

          ObjectNode field1 = (ObjectNode) fieldItems1.get(i);
          ObjectNode field2 = (ObjectNode) fieldsItems2.get(j);
          if (field1.get("name").equals(field2.get("name"))) {
            adjustNumberTypes((ObjectNode) fieldItems1.get(i), (ObjectNode) fieldsItems2.get(j));
          }

        }

      }
    }
  }

}
