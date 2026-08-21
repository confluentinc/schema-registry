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

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import org.junit.Test;

/**
 * Tests for the {@code blockRemoteRefs} behavior of {@link JsonSchema}: when set, a
 * {@code $ref} to an unregistered http/https document is rejected rather than fetched over
 * the network.
 */
public class JsonSchemaRemoteRefTest {

  private static JsonSchema schema(
      String schemaString,
      Map<String, String> resolvedReferences,
      boolean blockRemoteRefs) {
    return new JsonSchema(
        schemaString,
        Collections.emptyList(),
        resolvedReferences,
        null,
        null,
        null,
        blockRemoteRefs);
  }

  private static void assertBlocked(Throwable t) {
    if (!JsonSchema.isRemoteRefBlocked(t)) {
      throw new AssertionError("Expected a 'remote fetch disabled' cause but was: " + t, t);
    }
  }

  private static void assertNotBlocked(JsonSchema schema) {
    try {
      schema.rawSchema();
    } catch (RuntimeException e) {
      if (JsonSchema.isRemoteRefBlocked(e)) {
        throw new AssertionError("Schema must not be blocked, but it was: " + e, e);
      }
    }
  }

  private static void assertRefRejected(String schemaString) {
    try {
      schema(schemaString, Collections.emptyMap(), true).rawSchema();
      fail("Expected loading to fail because the remote $ref fetch is disabled");
    } catch (RuntimeException e) {
      assertBlocked(e);
    }
  }

  @Test
  public void localSchemaLoadsWhenBlocked_latestDraft() {
    String s = "{\"$schema\":\"https://json-schema.org/draft/2020-12/schema\","
        + "\"type\":\"object\",\"properties\":{\"a\":{\"type\":\"string\"}}}";
    assertNotNull(schema(s, Collections.emptyMap(), true).rawSchema());
  }

  @Test
  public void localSchemaLoadsWhenBlocked_draft7() {
    String s = "{\"$schema\":\"http://json-schema.org/draft-07/schema#\","
        + "\"type\":\"object\",\"properties\":{\"a\":{\"type\":\"string\"}}}";
    assertNotNull(schema(s, Collections.emptyMap(), true).rawSchema());
  }

  @Test
  public void httpRefRejectedWhenBlocked_latestDraft() {
    assertRefRejected("{\"$schema\":\"https://json-schema.org/draft/2020-12/schema\","
        + "\"type\":\"object\",\"properties\":{\"a\":{\"$ref\":\"http://example.com/r.json\"}}}");
  }

  @Test
  public void httpsRefRejectedWhenBlocked_draft7() {
    assertRefRejected("{\"$schema\":\"http://json-schema.org/draft-07/schema#\","
        + "\"type\":\"object\",\"properties\":{\"a\":{\"$ref\":\"https://example.com/r.json\"}}}");
  }

  @Test
  public void registeredHttpRefNotBlocked_latestDraft() {
    String ref = "http://acme.com/widget.json";
    String s = "{\"$schema\":\"https://json-schema.org/draft/2020-12/schema\","
        + "\"type\":\"object\",\"properties\":{\"a\":{\"$ref\":\"" + ref + "\"}}}";
    Map<String, String> resolvedRefs = Collections.singletonMap(
        ref, "{\"$schema\":\"https://json-schema.org/draft/2020-12/schema\",\"type\":\"string\"}");
    // A registered reference resolves from the in-memory mappings, not the network.
    assertNotNull(schema(s, resolvedRefs, true).rawSchema());
  }

  @Test
  public void registeredHttpRefNotBlocked_draft7() {
    String ref = "http://acme.com/widget.json";
    String s = "{\"$schema\":\"http://json-schema.org/draft-07/schema#\","
        + "\"type\":\"object\",\"properties\":{\"a\":{\"$ref\":\"" + ref + "\"}}}";
    Map<String, String> resolvedRefs = Collections.singletonMap(ref, "{\"type\":\"string\"}");
    assertNotNull(schema(s, resolvedRefs, true).rawSchema());
  }

  @Test
  public void notBlockedWhenBlockRemoteRefsFalse() {
    String s = "{\"$schema\":\"https://json-schema.org/draft/2020-12/schema\","
        + "\"type\":\"object\","
        + "\"properties\":{\"a\":{\"$ref\":\"http://nonexistent.invalid/r.json\"}}}";
    assertNotBlocked(schema(s, Collections.emptyMap(), false));
  }

  // The registration pipeline copies/normalizes the parsed schema before it is loaded, so the
  // block must survive those operations.

  @Test
  public void blockSurvivesCopyWithVersion() {
    String s = "{\"$schema\":\"https://json-schema.org/draft/2020-12/schema\","
        + "\"type\":\"object\",\"properties\":{\"a\":{\"$ref\":\"http://example.com/r.json\"}}}";
    JsonSchema copy = schema(s, Collections.emptyMap(), true).copy(0);
    assertTrue(copy instanceof JsonSchema);
    try {
      copy.rawSchema();
      fail("Expected the copy to still block the remote $ref fetch");
    } catch (RuntimeException e) {
      assertBlocked(e);
    }
  }

  @Test
  public void blockSurvivesNormalize() {
    String s = "{\"$schema\":\"https://json-schema.org/draft/2020-12/schema\","
        + "\"type\":\"object\",\"properties\":{\"a\":{\"$ref\":\"http://example.com/r.json\"}}}";
    JsonSchema normalized = schema(s, Collections.emptyMap(), true).normalize();
    try {
      normalized.rawSchema();
      fail("Expected the normalized copy to still block the remote $ref fetch");
    } catch (RuntimeException e) {
      assertBlocked(e);
    }
  }

  @Test
  public void blockedPreviousSurfacesAsIncompatibilityNotError() {
    String previous = "{\"$schema\":\"https://json-schema.org/draft/2020-12/schema\","
        + "\"type\":\"object\",\"properties\":{\"a\":{\"$ref\":\"http://example.com/r.json\"}}}";
    String current = "{\"$schema\":\"https://json-schema.org/draft/2020-12/schema\","
        + "\"type\":\"object\",\"properties\":{\"a\":{\"type\":\"string\"}}}";
    List<String> errors = schema(current, Collections.emptyMap(), true)
        .isBackwardCompatible(schema(previous, Collections.emptyMap(), true));
    assertFalse(errors.isEmpty());
    assertTrue(errors.get(0).contains("unsupported external reference"));
  }
}
