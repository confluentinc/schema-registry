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

import org.everit.json.schema.ObjectSchema;
import org.everit.json.schema.ReferenceSchema;
import org.everit.json.schema.Schema;
import org.junit.Test;

/**
 * Tests that the json-sKema-to-everit translation preserves the root {@code $id} and the everit
 * {@code schemaLocation} on 2019-09/2020-12, matching what the everit loader keeps for Draft-07.
 */
public class SchemaTranslatorMetadataTest {

  private static final String DRAFT_7 = "http://json-schema.org/draft-07/schema#";

  private static final String[] MODERN_DRAFTS = {
      "https://json-schema.org/draft/2019-09/schema",
      "https://json-schema.org/draft/2020-12/schema"
  };

  private static String rootSelfRef(String draft) {
    return "{\"$schema\":\"" + draft + "\","
        + "\"$id\":\"https://example.com/node.json\","
        + "\"type\":\"object\","
        + "\"properties\":{\"name\":{\"type\":\"string\"},\"self\":{\"$ref\":\"#\"}}}";
  }

  private static String defsRecursion(String draft) {
    return "{\"$schema\":\"" + draft + "\","
        + "\"$id\":\"https://example.com/tree.json\","
        + "\"type\":\"object\","
        + "\"properties\":{\"root\":{\"$ref\":\"#/$defs/Node\"}},"
        + "\"$defs\":{\"Node\":{\"type\":\"object\",\"properties\":{"
        + "\"value\":{\"type\":\"string\"},\"next\":{\"$ref\":\"#/$defs/Node\"}}}}}";
  }

  private static String nestedDefsRecursion(String draft) {
    return "{\"$schema\":\"" + draft + "\","
        + "\"$id\":\"https://example.com/nested.json\","
        + "\"type\":\"object\","
        + "\"properties\":{\"root\":{\"$ref\":\"#/$defs/Outer/$defs/Inner\"}},"
        + "\"$defs\":{\"Outer\":{\"$defs\":{\"Inner\":{\"type\":\"object\","
        + "\"properties\":{\"self\":{\"$ref\":\"#/$defs/Outer/$defs/Inner\"}}}}}}}";
  }

  private static String escapedDefName(String draft) {
    return "{\"$schema\":\"" + draft + "\","
        + "\"$id\":\"https://example.com/escaped.json\","
        + "\"type\":\"object\","
        + "\"properties\":{\"root\":{\"$ref\":\"#/$defs/a~1b~0c\"}},"
        + "\"$defs\":{\"a/b~c\":{\"type\":\"object\","
        + "\"properties\":{\"self\":{\"$ref\":\"#/$defs/a~1b~0c\"}}}}}";
  }

  @Test
  public void rootId_preservedLikeDraft7() {
    Schema draft7 = new JsonSchema(rootSelfRef(DRAFT_7)).rawSchema();
    assertNotNull(draft7.getId());
    for (String draft : MODERN_DRAFTS) {
      Schema modern = new JsonSchema(rootSelfRef(draft)).rawSchema();
      assertEquals(draft, draft7.getId(), modern.getId());
    }
  }

  @Test
  public void rootSchemaLocation_preservedLikeDraft7() {
    Schema draft7 = new JsonSchema(rootSelfRef(DRAFT_7)).rawSchema();
    assertNotNull(draft7.getSchemaLocation());
    for (String draft : MODERN_DRAFTS) {
      Schema modern = new JsonSchema(rootSelfRef(draft)).rawSchema();
      assertEquals(draft, draft7.getSchemaLocation(), modern.getSchemaLocation());
    }
  }

  @Test
  public void nestedDefTarget_getsSchemaLocation() {
    for (String draft : MODERN_DRAFTS) {
      Schema root = new JsonSchema(defsRecursion(draft)).rawSchema();
      Schema rootProperty = ((ObjectSchema) root).getPropertySchemas().get("root");
      Schema node = ((ReferenceSchema) rootProperty).getReferredSchema();
      assertEquals(draft, "#/$defs/Node", node.getSchemaLocation());
    }
  }

  @Test
  public void deeplyNestedDefTarget_getsSchemaLocation() {
    for (String draft : MODERN_DRAFTS) {
      Schema root = new JsonSchema(nestedDefsRecursion(draft)).rawSchema();
      Schema rootProperty = ((ObjectSchema) root).getPropertySchemas().get("root");
      Schema inner = ((ReferenceSchema) rootProperty).getReferredSchema();
      assertEquals(draft, "#/$defs/Outer/$defs/Inner", inner.getSchemaLocation());
    }
  }

  @Test
  public void reservedChars_escapedInSchemaLocation() {
    for (String draft : MODERN_DRAFTS) {
      Schema root = new JsonSchema(escapedDefName(draft)).rawSchema();
      Schema rootProperty = ((ObjectSchema) root).getPropertySchemas().get("root");
      Schema target = ((ReferenceSchema) rootProperty).getReferredSchema();
      assertEquals(draft, "#/$defs/a~1b~0c", target.getSchemaLocation());
    }
  }
}
