/*
 * Copyright 2026 Confluent Inc.
 *
 * Licensed under the Confluent Community License (the "License"); you may not use
 * this file except in compliance with the License.  You may obtain a copy of the
 * License at
 *
 * http://www.confluent.io/confluent-community-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OF ANY KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations under the License.
 */

package io.confluent.kafka.schemaregistry.json;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import org.everit.json.schema.ArraySchema;
import org.everit.json.schema.CombinedSchema;
import org.everit.json.schema.ObjectSchema;
import org.everit.json.schema.Schema;
import org.junit.Test;

/**
 * Tests that translating a 2019-09/2020-12 schema does not depend on keyword order: keyword
 * schemas such as {@code items} must merge into the matching branch of a multi-type
 * {@code type} wherever they appear relative to it.
 */
public class SchemaTranslatorKeywordOrderTest {

  private static final String[] MODERN_DRAFTS = {
      "https://json-schema.org/draft/2019-09/schema",
      "https://json-schema.org/draft/2020-12/schema"
  };

  private static final String ITEMS = "\"items\":{\"type\":\"string\"}";
  private static final String REQUIRED = "\"required\":[]";
  private static final String TYPE = "\"type\":[\"array\",\"null\"]";

  private static final String[][] ORDERS = {
      {ITEMS, REQUIRED, TYPE},
      {ITEMS, TYPE, REQUIRED},
      {REQUIRED, ITEMS, TYPE},
      {REQUIRED, TYPE, ITEMS},
      {TYPE, ITEMS, REQUIRED},
      {TYPE, REQUIRED, ITEMS}
  };

  @Test
  public void itemsMergeIntoArrayBranch_regardlessOfKeywordOrder() {
    for (String draft : MODERN_DRAFTS) {
      for (String[] order : ORDERS) {
        String schema = "{\"$schema\":\"" + draft + "\",\"type\":\"object\","
            + "\"properties\":{\"list\":{" + String.join(",", order) + "}}}";
        Schema list = ((ObjectSchema) new JsonSchema(schema).rawSchema())
            .getPropertySchemas().get("list");
        List<ArraySchema> arrays = new ArrayList<>();
        collectArraySchemas(list, arrays);
        String msg = draft + " " + Arrays.toString(order) + " -> " + list;
        assertEquals(msg, 1, arrays.size());
        assertNotNull(msg, arrays.get(0).getAllItemSchema());
      }
    }
  }

  private static void collectArraySchemas(Schema schema, List<ArraySchema> arrays) {
    if (schema instanceof ArraySchema) {
      arrays.add((ArraySchema) schema);
    } else if (schema instanceof CombinedSchema) {
      for (Schema subschema : ((CombinedSchema) schema).getSubschemas()) {
        collectArraySchemas(subschema, arrays);
      }
    }
  }
}
