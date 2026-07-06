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

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.util.Collections;
import java.util.Map;
import org.junit.Test;

/**
 * Tests for the {@code fetchRemoteRefs} behavior of {@link JsonSchema}: when remote fetching is
 * disabled, a new schema's {@code $ref}/{@code $dynamicRef} to an unregistered http/https document
 * is rejected rather than fetched over the network.
 */
public class JsonSchemaRemoteRefTest {

  private static final String DISABLED_MESSAGE =
      "Remote schema reference fetching over HTTP(S) is disabled";

  private static JsonSchema schema(
      String schemaString,
      Map<String, String> resolvedReferences,
      boolean fetchRemoteRefs,
      boolean schemaIsNew) {
    return new JsonSchema(
        schemaString,
        Collections.emptyList(),
        resolvedReferences,
        null,
        null,
        null,
        fetchRemoteRefs,
        schemaIsNew);
  }

  private static void assertBlocked(Throwable t) {
    for (Throwable c = t; c != null; c = c.getCause()) {
      if (c.getMessage() != null && c.getMessage().contains(DISABLED_MESSAGE)) {
        return;
      }
    }
    throw new AssertionError("Expected a 'remote fetch disabled' cause but was: " + t, t);
  }

  private static void assertNotBlocked(JsonSchema schema) {
    try {
      schema.rawSchema();
    } catch (RuntimeException e) {
      for (Throwable c = e; c != null; c = c.getCause()) {
        if (c.getMessage() != null && c.getMessage().contains(DISABLED_MESSAGE)) {
          throw new AssertionError("Schema must not be blocked, but it was: " + e, e);
        }
      }
    }
  }

  private static void assertRefRejected(String schemaString) {
    try {
      schema(schemaString, Collections.emptyMap(), false, true).rawSchema();
      fail("Expected loading to fail because the remote $ref fetch is disabled");
    } catch (RuntimeException e) {
      assertBlocked(e);
    }
  }

  @Test
  public void localSchemaLoadsWhenFetchDisabled_latestDraft() {
    String s = "{\"$schema\":\"https://json-schema.org/draft/2020-12/schema\","
        + "\"type\":\"object\",\"properties\":{\"a\":{\"type\":\"string\"}}}";
    assertNotNull(schema(s, Collections.emptyMap(), false, true).rawSchema());
  }

  @Test
  public void localSchemaLoadsWhenFetchDisabled_draft7() {
    String s = "{\"$schema\":\"http://json-schema.org/draft-07/schema#\","
        + "\"type\":\"object\",\"properties\":{\"a\":{\"type\":\"string\"}}}";
    assertNotNull(schema(s, Collections.emptyMap(), false, true).rawSchema());
  }

  @Test
  public void httpRefRejectedWhenFetchDisabled_latestDraft() {
    assertRefRejected("{\"$schema\":\"https://json-schema.org/draft/2020-12/schema\","
        + "\"type\":\"object\",\"properties\":{\"a\":{\"$ref\":\"http://example.com/r.json\"}}}");
  }

  @Test
  public void httpsRefRejectedWhenFetchDisabled_draft7() {
    assertRefRejected("{\"$schema\":\"http://json-schema.org/draft-07/schema#\","
        + "\"type\":\"object\",\"properties\":{\"a\":{\"$ref\":\"https://example.com/r.json\"}}}");
  }

  @Test
  public void dynamicRefRejectedWhenFetchDisabled_latestDraft() {
    assertRefRejected("{\"$schema\":\"https://json-schema.org/draft/2020-12/schema\","
        + "\"type\":\"object\","
        + "\"properties\":{\"a\":{\"$dynamicRef\":\"http://example.com/r.json#node\"}}}");
  }

  @Test
  public void registeredHttpRefNotBlockedWhenFetchDisabled_latestDraft() {
    String ref = "http://acme.com/widget.json";
    String s = "{\"$schema\":\"https://json-schema.org/draft/2020-12/schema\","
        + "\"type\":\"object\",\"properties\":{\"a\":{\"$ref\":\"" + ref + "\"}}}";
    Map<String, String> resolvedRefs = Collections.singletonMap(
        ref, "{\"$schema\":\"https://json-schema.org/draft/2020-12/schema\",\"type\":\"string\"}");
    // A registered reference resolves from the in-memory mappings, not the network.
    assertNotNull(schema(s, resolvedRefs, false, true).rawSchema());
  }

  @Test
  public void registeredHttpRefNotBlockedWhenFetchDisabled_draft7() {
    String ref = "http://acme.com/widget.json";
    String s = "{\"$schema\":\"http://json-schema.org/draft-07/schema#\","
        + "\"type\":\"object\",\"properties\":{\"a\":{\"$ref\":\"" + ref + "\"}}}";
    Map<String, String> resolvedRefs = Collections.singletonMap(ref, "{\"type\":\"string\"}");
    assertNotNull(schema(s, resolvedRefs, false, true).rawSchema());
  }

  @Test
  public void existingSchemaHttpRefNotBlocked() {
    // schemaIsNew=false simulates an already-stored schema: the block must not apply.
    String s = "{\"$schema\":\"https://json-schema.org/draft/2020-12/schema\","
        + "\"type\":\"object\","
        + "\"properties\":{\"a\":{\"$ref\":\"http://nonexistent.invalid/r.json\"}}}";
    assertNotBlocked(schema(s, Collections.emptyMap(), false, false));
  }

  @Test
  public void fetchAllowedByDefaultIsNotBlocked() {
    // fetchRemoteRefs=true (the default) means no blocking even for a new schema.
    String s = "{\"$schema\":\"https://json-schema.org/draft/2020-12/schema\","
        + "\"type\":\"object\","
        + "\"properties\":{\"a\":{\"$ref\":\"http://nonexistent.invalid/r.json\"}}}";
    assertNotBlocked(schema(s, Collections.emptyMap(), true, true));
  }

  // The registration pipeline copies/normalizes the parsed schema before it is loaded, so the
  // block must survive those operations.

  @Test
  public void blockSurvivesCopyWithVersion() {
    String s = "{\"$schema\":\"https://json-schema.org/draft/2020-12/schema\","
        + "\"type\":\"object\",\"properties\":{\"a\":{\"$ref\":\"http://example.com/r.json\"}}}";
    JsonSchema copy = schema(s, Collections.emptyMap(), false, true).copy(0);
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
    JsonSchema normalized = schema(s, Collections.emptyMap(), false, true).normalize();
    try {
      normalized.rawSchema();
      fail("Expected the normalized copy to still block the remote $ref fetch");
    } catch (RuntimeException e) {
      assertBlocked(e);
    }
  }
}
