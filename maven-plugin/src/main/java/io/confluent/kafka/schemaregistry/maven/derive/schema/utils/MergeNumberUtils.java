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

    if (schemaList.size() == 0 || schemaList.size() == 1) {
      return;
    }

    /*
    Using O(n*n) version over O(n) version checking only with 1st schema
    1. A field is absent in the first schema, hence that field is never checked in other messages
    2. A field is present in the first schema, but of type non-Number
    but rest of the messages are of number type.
     */

    if (matchAll) {
      for (ObjectNode curr : schemaList) {
        for (ObjectNode starting : schemaList) {
          adjustNumberTypes(starting, curr);
        }
      }
    } else {

      for (ObjectNode curr : schemaList) {
        adjustNumberTypes(schemaList.get(0), curr);
      }

      for (ObjectNode curr : schemaList) {
        adjustNumberTypes(schemaList.get(0), curr);
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

    String field1 = schema1.get(key).textValue();
    String field2 = schema2.get(key).textValue();

    ArrayList<String> dataTypes = new ArrayList<>(Arrays.asList("int", "int32", "int64", "long"));

    if (Objects.equals(field2, "double") && dataTypes.contains(field1)) {
      schema1.set(key, schema2.get(key));
    } else if (Objects.equals(field1, "double") && dataTypes.contains(field2)) {
      schema2.set(key, schema1.get(key));
    } else if (Objects.equals(field2, "long") && dataTypes.contains(field1)) {
      schema1.set(key, schema2.get(key));
    } else if (Objects.equals(field1, "long") && dataTypes.contains(field2)) {
      schema2.set(key, schema1.get(key));
    }

  }

  private static void matchJsonArray(ObjectNode schema1, ObjectNode schema2, String key) {

    ArrayNode fields1 = (ArrayNode) schema1.get(key);
    ArrayNode fields2 = (ArrayNode) schema2.get(key);

    for (int i = 0; i < fields1.size(); i++) {
      for (int j = 0; j < fields2.size(); j++) {

        if ((fields1.get(i) instanceof ObjectNode) && (fields2.get(j) instanceof ObjectNode)) {

          ObjectNode field1 = (ObjectNode) fields1.get(i);
          ObjectNode field2 = (ObjectNode) fields2.get(j);

          if (field1.get("name").equals(field2.get("name"))) {
            adjustNumberTypes((ObjectNode) fields1.get(i), (ObjectNode) fields2.get(j));
          }

        }
      }
    }

  }


}
