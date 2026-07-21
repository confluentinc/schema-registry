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
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.confluent.kafka.schemaregistry.client.rest.entities.SchemaReference;
import org.everit.json.schema.ObjectSchema;
import org.everit.json.schema.ReferenceSchema;
import org.everit.json.schema.Schema;
import org.junit.Test;

/**
 * Tests for the {@code definitions → $defs} bridging inside {@link JsonSchema}.
 *
 * <p>Both load paths copy {@code definitions} entries into {@code $defs}, making {@code $defs}
 * the canonical "forgiving" reference form across both drafts. The user-facing rule is:
 *
 * <ul>
 *   <li>{@code $ref: "#/$defs/X"} always resolves, regardless of which bucket the body lives in.</li>
 *   <li>{@code $ref: "#/definitions/X"} resolves only when the body is actually in
 *       {@code definitions}.</li>
 * </ul>
 *
 * <p>This trade-off is deliberate: schema authors and tooling can pick {@code $defs} as the
 * single canonical ref form without worrying about draft compatibility, at the cost of one
 * inverse pattern (body in {@code $defs}, ref via {@code #/definitions/X}) not working in
 * draft 7. {@link #draft7_bodyInDefs_refViaDefinitions_fails} pins that gap as intentional.
 */
public class JsonSchemaDefBridgeTest {

  private static final ObjectMapper MAPPER = new ObjectMapper();

  // -----------------------------------------------------------------------------------------
  // Unit tests on JsonSchema.mergeDefBuckets — exercise the in-production direction.
  // The helper is algorithmically symmetric in (fromKey, toKey); testing one direction is
  // sufficient since the algorithm doesn't branch on the keyword names.
  // -----------------------------------------------------------------------------------------

  @Test
  public void mergeDefBuckets_emptyOrAbsentSource_returnsUnchanged() throws Exception {
    // Source key absent entirely.
    String absent = "{\"type\":\"object\"}";
    assertJsonEquals(absent, JsonSchema.mergeDefBuckets(absent, "definitions", "$defs"));

    // Source key present but empty object — nothing to copy, target unchanged.
    String empty = "{\"definitions\":{}}";
    assertJsonEquals(empty, JsonSchema.mergeDefBuckets(empty, "definitions", "$defs"));
  }

  @Test
  public void mergeDefBuckets_emptyOrAbsentTarget_copiesAllEntries() throws Exception {
    // Target absent — copied wholesale.
    String absentTarget = "{\"definitions\":{\"Foo\":{\"type\":\"string\"}}}";
    JsonNode out = MAPPER.readTree(
        JsonSchema.mergeDefBuckets(absentTarget, "definitions", "$defs"));
    assertNotNull(out.get("$defs"));
    assertEquals("string", out.get("$defs").get("Foo").get("type").asText());

    // Target present but empty — entries copied in.
    String emptyTarget = "{\"definitions\":{\"Foo\":{\"type\":\"string\"}},\"$defs\":{}}";
    out = MAPPER.readTree(JsonSchema.mergeDefBuckets(emptyTarget, "definitions", "$defs"));
    assertEquals("string", out.get("$defs").get("Foo").get("type").asText());
  }

  @Test
  public void mergeDefBuckets_disjointKeys_merged() throws Exception {
    String json = "{\"definitions\":{\"Foo\":{\"type\":\"string\"}},"
        + "\"$defs\":{\"Bar\":{\"type\":\"integer\"}}}";
    JsonNode out = MAPPER.readTree(JsonSchema.mergeDefBuckets(json, "definitions", "$defs"));
    assertEquals("string", out.get("$defs").get("Foo").get("type").asText());
    assertEquals("integer", out.get("$defs").get("Bar").get("type").asText());
  }

  @Test
  public void mergeDefBuckets_collision_targetWins() throws Exception {
    String json = "{\"definitions\":{\"Foo\":{\"type\":\"string\"}},"
        + "\"$defs\":{\"Foo\":{\"type\":\"integer\"}}}";
    JsonNode out = MAPPER.readTree(JsonSchema.mergeDefBuckets(json, "definitions", "$defs"));
    // Target wins: "integer" should remain, not be overwritten by source's "string".
    assertEquals("integer", out.get("$defs").get("Foo").get("type").asText());
  }

  @Test
  public void mergeDefBuckets_nonObjectSourceOrTarget_ignored() throws Exception {
    // Non-object source (e.g. `definitions: 42`): nothing to merge, return as-is.
    String nonObjectSource = "{\"definitions\":42}";
    assertJsonEquals(nonObjectSource,
        JsonSchema.mergeDefBuckets(nonObjectSource, "definitions", "$defs"));

    // Non-object target (e.g. `$defs: []`): preserved unchanged, don't overwrite with object.
    String nonObjectTarget = "{\"definitions\":{\"Foo\":{\"type\":\"string\"}},\"$defs\":[]}";
    JsonNode out = MAPPER.readTree(
        JsonSchema.mergeDefBuckets(nonObjectTarget, "definitions", "$defs"));
    assertTrue("non-object target preserved", out.get("$defs").isArray());

    // Malformed JSON: best-effort fallback returns input unchanged (same instance).
    String malformed = "{not-valid-json";
    assertSame(malformed, JsonSchema.mergeDefBuckets(malformed, "definitions", "$defs"));
  }

  @Test
  public void mergeDefBuckets_booleanSchemaRoot_unchanged() throws Exception {
    assertJsonEquals("true", JsonSchema.mergeDefBuckets("true", "definitions", "$defs"));
    assertJsonEquals("false", JsonSchema.mergeDefBuckets("false", "definitions", "$defs"));
  }

  // -----------------------------------------------------------------------------------------
  // End-to-end tests on JsonSchema — exercise the full load path through Everit / json-sKema.
  // -----------------------------------------------------------------------------------------

  /**
   * For every combination of draft and bucket keyword, a schema whose body lives in that bucket
   * and whose ref uses the matching JSON Pointer keyword must resolve. Covers the "natural" cases
   * across both load paths — native walks for the loader's preferred bucket, literal JSON Pointer
   * fallback for the other.
   */
  @Test
  public void bucketMatchingRef_resolvesAcrossDrafts() {
    String[][] cases = new String[][]{
        // {draft URI, bucket keyword}
        {"http://json-schema.org/draft-04/schema#", "$defs"},
        {"http://json-schema.org/draft-06/schema#", "$defs"},
        {"http://json-schema.org/draft-07/schema#", "$defs"},
        {"http://json-schema.org/draft-07/schema#", "definitions"},
        {"https://json-schema.org/draft/2020-12/schema", "$defs"},
        {"https://json-schema.org/draft/2020-12/schema", "definitions"},
    };
    for (String[] c : cases) {
      String draftUri = c[0];
      String bucket = c[1];
      String schemaStr = "{"
          + "\"$schema\":\"" + draftUri + "\","
          + "\"type\":\"object\","
          + "\"properties\":{\"thing\":{\"$ref\":\"#/" + bucket + "/Thing\"}},"
          + "\"" + bucket + "\":{\"Thing\":{\"type\":\"integer\"}}"
          + "}";
      JsonSchema schema = new JsonSchema(schemaStr);
      assertReferredPropertyResolves(schema, "thing", draftUri + " / " + bucket);
    }
  }

  /**
   * Draft-7 case 3: body in {@code definitions}, ref via {@code #/$defs/X}. The
   * {@code definitions → $defs} bridge in {@code loadPreviousDraft} is what makes this work —
   * after bridging the body is also at {@code root.$defs.Thing}, and Everit's literal JSON
   * Pointer fallback resolves the ref.
   */
  @Test
  public void draft7_bodyInDefinitions_refViaDefs_resolvesRefs() {
    String schemaStr = "{"
        + "\"$schema\":\"http://json-schema.org/draft-07/schema#\","
        + "\"type\":\"object\","
        + "\"properties\":{\"thing\":{\"$ref\":\"#/$defs/Thing\"}},"
        + "\"definitions\":{\"Thing\":{\"type\":\"integer\"}}"
        + "}";
    JsonSchema schema = new JsonSchema(schemaStr);
    assertReferredPropertyResolves(schema, "thing", "draft-7 body-in-definitions, ref via $defs");
  }

  /**
   * Draft-7 case 1: body in {@code $defs}, ref via {@code #/definitions/X}. <b>Intentional
   * gap.</b> The bridge direction (definitions → $defs) does not cover this combination — the
   * body never lands in {@code definitions}, so neither the URI registry nor the literal
   * fallback can find it. Pinned as a regression marker so the gap doesn't change silently.
   *
   * <p>Schema authors should use {@code #/$defs/X} as the canonical ref form — that always
   * resolves regardless of body location.
   */
  @Test
  public void draft7_bodyInDefs_refViaDefinitions_fails() {
    String schemaStr = "{"
        + "\"$schema\":\"http://json-schema.org/draft-07/schema#\","
        + "\"type\":\"object\","
        + "\"properties\":{\"thing\":{\"$ref\":\"#/definitions/Thing\"}},"
        + "\"$defs\":{\"Thing\":{\"type\":\"integer\"}}"
        + "}";
    try {
      new JsonSchema(schemaStr).rawSchema();
      fail("Expected IllegalArgumentException for draft-7 body-in-$defs + ref-via-definitions");
    } catch (IllegalArgumentException expected) {
      // Pinned: this combination is intentionally unsupported.
    }
  }

  /**
   * Draft-2020-12 case 1: body in {@code definitions}, ref via {@code #/$defs/X}. The
   * {@code definitions → $defs} bridge in {@code loadLatestDraft} is what makes this work —
   * after bridging json-sKema sees {@code $defs.Thing} in its native walk and registers the URI.
   */
  @Test
  public void draft2020_bodyInDefinitions_refViaDefs_resolvesRefs() {
    String schemaStr = "{"
        + "\"$schema\":\"https://json-schema.org/draft/2020-12/schema\","
        + "\"type\":\"object\","
        + "\"properties\":{\"thing\":{\"$ref\":\"#/$defs/Thing\"}},"
        + "\"definitions\":{\"Thing\":{\"type\":\"integer\"}}"
        + "}";
    JsonSchema schema = new JsonSchema(schemaStr);
    assertReferredPropertyResolves(schema, "thing", "draft-2020-12 body-in-definitions, ref via $defs");
  }

  /**
   * The bridge applies to {@code resolvedReferences} content too: an external schema brought in
   * via SR references that uses the non-native bucket must remain reachable from the importing
   * root.
   */
  @Test
  public void externalRef_via_resolvedReferences_resolves() {
    String externalSchema = "{"
        + "\"$schema\":\"http://json-schema.org/draft-07/schema#\","
        + "\"type\":\"object\","
        + "\"properties\":{\"x\":{\"$ref\":\"#/$defs/X\"}},"
        + "\"definitions\":{\"X\":{\"type\":\"integer\"}}"
        + "}";
    String rootSchema = "{"
        + "\"$schema\":\"http://json-schema.org/draft-07/schema#\","
        + "\"type\":\"object\","
        + "\"properties\":{\"item\":{\"$ref\":\"external.json\"}}"
        + "}";
    SchemaReference ref = new SchemaReference("external.json", "external", 1);
    JsonSchema schema = new JsonSchema(rootSchema, ImmutableList.of(ref),
        ImmutableMap.of("external.json", externalSchema), null);
    // Schema loads without error — the bridged external schema's body was reachable.
    assertNotNull(schema.rawSchema());
    schema.validate(true);
  }

  /**
   * Cross-document fragment ref: a draft-2020-12 root with {@code $ref:
   * "external.json#/$defs/X"} must resolve into the imported schema's {@code $defs} entry.
   * Pins the {@code base.resolve(dep.getKey())} mapping in {@code loadLatestDraft} — without
   * that registration, json-sKema would resolve the document part {@code external.json}
   * against the document base ({@code mem://input}) into {@code mem://input/external.json},
   * miss the un-resolved entry, and fail the lookup.
   */
  @Test
  public void crossDocumentFragmentRef_resolvesViaBaseResolvedMapping() {
    String external = "{"
        + "\"$schema\":\"https://json-schema.org/draft/2020-12/schema\","
        + "\"type\":\"object\","
        + "\"$defs\":{\"X\":{\"type\":\"integer\"}}"
        + "}";
    String root = "{"
        + "\"$schema\":\"https://json-schema.org/draft/2020-12/schema\","
        + "\"type\":\"object\","
        + "\"properties\":{\"item\":{\"$ref\":\"external.json#/$defs/X\"}}"
        + "}";
    SchemaReference ref = new SchemaReference("external.json", "external", 1);
    JsonSchema schema = new JsonSchema(root, ImmutableList.of(ref),
        ImmutableMap.of("external.json", external), null);
    assertNotNull(schema.rawSchema());
    schema.validate(true);
  }

  /**
   * Cross-draft: a draft-7 root imports a draft-2020-12 schema. The whole load is routed
   * through Everit (the root's draft picks the loader), but the bridge is still applied to the
   * imported content — so a body in {@code definitions} inside the import is reachable via
   * {@code #/$defs/X}.
   */
  @Test
  public void crossDraft_draft7Root_importsDraft2020Schema_resolves() {
    String draft2020Import = "{"
        + "\"$schema\":\"https://json-schema.org/draft/2020-12/schema\","
        + "\"type\":\"object\","
        + "\"properties\":{\"x\":{\"$ref\":\"#/$defs/X\"}},"
        + "\"definitions\":{\"X\":{\"type\":\"integer\"}}"
        + "}";
    String draft7Root = "{"
        + "\"$schema\":\"http://json-schema.org/draft-07/schema#\","
        + "\"type\":\"object\","
        + "\"properties\":{\"item\":{\"$ref\":\"external.json\"}}"
        + "}";
    SchemaReference ref = new SchemaReference("external.json", "external", 1);
    JsonSchema schema = new JsonSchema(draft7Root, ImmutableList.of(ref),
        ImmutableMap.of("external.json", draft2020Import), null);
    assertNotNull(schema.rawSchema());
    schema.validate(true);
  }

  /**
   * Cross-draft: a draft-2020-12 root imports a draft-7 schema. The whole load is routed
   * through json-sKema, but the bridge is still applied to the imported content — so a body in
   * {@code definitions} inside the import is reachable via {@code #/$defs/X}.
   */
  @Test
  public void crossDraft_draft2020Root_importsDraft7Schema_resolves() {
    String draft7Import = "{"
        + "\"$schema\":\"http://json-schema.org/draft-07/schema#\","
        + "\"type\":\"object\","
        + "\"properties\":{\"x\":{\"$ref\":\"#/$defs/X\"}},"
        + "\"definitions\":{\"X\":{\"type\":\"integer\"}}"
        + "}";
    String draft2020Root = "{"
        + "\"$schema\":\"https://json-schema.org/draft/2020-12/schema\","
        + "\"type\":\"object\","
        + "\"properties\":{\"item\":{\"$ref\":\"external.json\"}}"
        + "}";
    SchemaReference ref = new SchemaReference("external.json", "external", 1);
    JsonSchema schema = new JsonSchema(draft2020Root, ImmutableList.of(ref),
        ImmutableMap.of("external.json", draft7Import), null);
    assertNotNull(schema.rawSchema());
    schema.validate(true);
  }

  // -----------------------------------------------------------------------------------------
  // Round-trip / no-mutation: bridging must not leak into the canonical form.
  // -----------------------------------------------------------------------------------------

  /**
   * Bridging mutates an in-memory copy passed to the loader, never the source representation.
   * The schema's canonical form should still reflect the original keyword set, so persisted /
   * fingerprinted schemas don't shift.
   */
  @Test
  public void canonicalForm_unchangedByBridging() throws Exception {
    String schemaStr = "{"
        + "\"$schema\":\"http://json-schema.org/draft-07/schema#\","
        + "\"type\":\"object\","
        + "\"properties\":{\"thing\":{\"$ref\":\"#/$defs/Thing\"}},"
        + "\"definitions\":{\"Thing\":{\"type\":\"integer\"}}"
        + "}";
    JsonSchema schema = new JsonSchema(schemaStr);
    schema.rawSchema(); // force load — triggers bridging
    JsonNode canonical = MAPPER.readTree(schema.canonicalString());
    // The bridge must not have copied entries into `$defs` in the canonical form.
    assertTrue("definitions preserved", canonical.has("definitions"));
    assertTrue("$defs not introduced by bridging", !canonical.has("$defs"));
  }

  // -----------------------------------------------------------------------------------------
  // Helpers
  // -----------------------------------------------------------------------------------------

  private static void assertJsonEquals(String expected, String actual) throws Exception {
    assertEquals(MAPPER.readTree(expected), MAPPER.readTree(actual));
  }

  /**
   * Walk to the named property, follow any {@code ReferenceSchema}, and assert the body is
   * present and resolved (i.e. {@code getReferredSchema()} is non-null). The {@code label}
   * is included in failure messages to identify which case failed.
   */
  private static void assertReferredPropertyResolves(
      JsonSchema schema, String propertyName, String label) {
    Schema raw = schema.rawSchema();
    assertNotNull("rawSchema() returned null for " + label, raw);
    assertTrue("expected ObjectSchema root for " + label, raw instanceof ObjectSchema);
    Schema property = ((ObjectSchema) raw).getPropertySchemas().get(propertyName);
    assertNotNull("property '" + propertyName + "' missing for " + label, property);
    if (property instanceof ReferenceSchema) {
      assertNotNull(
          "ReferenceSchema for '" + propertyName + "' is unresolved for " + label,
          ((ReferenceSchema) property).getReferredSchema());
    }
  }

}
