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

import org.everit.json.schema.Schema;
import org.junit.Test;

/**
 * Verifies that the json-sKema -> everit translation for Draft 2019-09/2020-12 preserves the
 * same schema-identity metadata that the everit loader keeps for Draft-07, namely the root
 * {@code $id} and the everit {@code schemaLocation}.
 *
 * <p>Consumers that read {@link JsonSchema#rawSchema()} (e.g. Flink's cyclic-schema cut to
 * VARIANT) key a {@code $ref} target by its schema location, falling back to the reference
 * value and then to instance identity. When the translator drops the location and the root
 * {@code $id}, a {@code $ref: "#"} under a root {@code $id} is no longer recognized as the
 * root, so a cycle is cut one level late and the inferred type differs from Draft-07.
 *
 * <p>See DGS-25567 / FSE-2054.
 */
public class SchemaTranslatorMetadataTest {

  // The recursive example from DGS-25567: a node whose "self" property refers back to the root.
  private static final String BODY =
      "\"$id\":\"https://example.com/node.json\","
          + "\"type\":\"object\","
          + "\"properties\":{"
          + "\"name\":{\"type\":\"string\"},"
          + "\"self\":{\"$ref\":\"#\"}"
          + "}";

  private static final String DRAFT_7 =
      "{\"$schema\":\"http://json-schema.org/draft-07/schema#\"," + BODY + "}";

  private static final String DRAFT_2020_12 =
      "{\"$schema\":\"https://json-schema.org/draft/2020-12/schema\"," + BODY + "}";

  @Test
  public void draft2020KeepsRootIdLikeDraft7() {
    Schema draft7 = new JsonSchema(DRAFT_7).rawSchema();
    Schema draft2020 = new JsonSchema(DRAFT_2020_12).rawSchema();

    assertNotNull("Draft-07 baseline should carry the root $id", draft7.getId());
    assertEquals(
        "2020-12 root $id should match Draft-07", draft7.getId(), draft2020.getId());
  }

  @Test
  public void draft2020KeepsSchemaLocationLikeDraft7() {
    Schema draft7 = new JsonSchema(DRAFT_7).rawSchema();
    Schema draft2020 = new JsonSchema(DRAFT_2020_12).rawSchema();

    assertNotNull(
        "Draft-07 baseline should carry the root schemaLocation", draft7.getSchemaLocation());
    assertEquals(
        "2020-12 root schemaLocation should match Draft-07",
        draft7.getSchemaLocation(),
        draft2020.getSchemaLocation());
  }
}
