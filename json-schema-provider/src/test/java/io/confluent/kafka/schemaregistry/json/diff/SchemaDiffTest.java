/*
 * Copyright 2020 Confluent Inc.
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

package io.confluent.kafka.schemaregistry.json.diff;

import static java.util.stream.Collectors.toList;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.assertEquals;

import static org.junit.Assert.assertFalse;

import io.confluent.kafka.schemaregistry.json.JsonSchema;
import java.io.BufferedReader;
import java.util.concurrent.CancellationException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;
import org.everit.json.schema.Schema;
import org.everit.json.schema.loader.SchemaLoader;
import org.json.JSONArray;
import org.json.JSONObject;
import org.junit.Assert;
import org.junit.Test;

public class SchemaDiffTest {

  @Test
  public void checkJsonSchemaCompatibility() {
    final JSONArray testCases = new JSONArray(Objects.requireNonNull(readFile("diff-schema-examples.json")));
    checkJsonSchemaCompatibility(testCases);
  }

  @Test
  public void checkJsonSchemaCompatibility_2020_12() {
    final JSONArray testCases = new JSONArray(Objects.requireNonNull(readFile("diff-schema-examples-2020-12.json")));
    checkJsonSchemaCompatibility(testCases);
  }

  @Test
  public void checkJsonSchemaCompatibilityForCombinedSchemas() {
    final JSONArray testCases = new JSONArray(Objects.requireNonNull(readFile("diff-combined-schema-examples.json")));
    checkJsonSchemaCompatibility(testCases);
  }

  @Test
  public void checkJsonSchemaCompatibilityForCombinedSchemas_2020_12() {
    final JSONArray testCases = new JSONArray(Objects.requireNonNull(readFile("diff-combined-schema-examples-2020-12.json")));
    checkJsonSchemaCompatibility(testCases);
  }

  @Test
  public void interruptedThreadAbortsComparison() {
    final JsonSchema original = new JsonSchema("{\"type\":\"object\"}");
    final JsonSchema update =
        new JsonSchema("{\"type\":\"object\",\"properties\":{\"a\":{\"type\":\"string\"}}}");

    // Simulate the request-timeout interrupt arriving mid-comparison: an interrupted thread must
    // abort the (potentially exponential) diff so it can be reclaimed.
    Thread.currentThread().interrupt();
    try {
      SchemaDiff.compare(original.rawSchema(), update.rawSchema());
      Assert.fail("expected CancellationException when the worker thread is interrupted");
    } catch (CancellationException expected) {
      // expected
    } finally {
      Thread.interrupted(); // ensure no interrupt status leaks to other tests
    }

    // The diff clears the interrupt status as it aborts, so the (pooled) thread returns clean.
    assertFalse(Thread.currentThread().isInterrupted());
  }

  private void checkJsonSchemaCompatibility(JSONArray testCases) {
    for (final Object testCaseObject : testCases) {
      final JSONObject testCase = (JSONObject) testCaseObject;
      final JsonSchema original = new JsonSchema(testCase.getJSONObject("original_schema").toString());
      final JsonSchema update = new JsonSchema(testCase.getJSONObject("update_schema").toString());
      final JSONArray changes = (JSONArray) testCase.get("changes");
      boolean isCompatible = testCase.getBoolean("compatible");
      final List<String> errorMessages = changes.toList()
          .stream()
          .map(Object::toString)
          .collect(toList());
      final String description = (String) testCase.get("description");

      List<Difference> differences = SchemaDiff.compare(original.rawSchema(), update.rawSchema());
      final List<Difference> incompatibleDiffs = differences.stream()
          .filter(diff -> !SchemaDiff.COMPATIBLE_CHANGES_STRICT.contains(diff.getType()))
          .collect(Collectors.toList());
      assertThat(description,
          differences.stream()
              .map(change -> change.getType().toString() + " " + change.getJsonPath())
              .collect(toList()),
          is(errorMessages)
      );
      assertEquals(description, isCompatible, incompatibleDiffs.isEmpty());

      boolean isCompatibleLenient = isCompatible;
      if (testCase.has("compatible_lenient")) {
        isCompatibleLenient = testCase.getBoolean("compatible_lenient");
      }
      List<Difference> differencesLenient = SchemaDiff.compare(SchemaDiff.COMPATIBLE_CHANGES_LENIENT,
          original.rawSchema(), update.rawSchema());
      final List<Difference> incompatibleDiffsLenient = differences.stream()
          .filter(diff -> !SchemaDiff.COMPATIBLE_CHANGES_LENIENT.contains(diff.getType()))
          .collect(Collectors.toList());
      assertEquals(description, isCompatibleLenient, incompatibleDiffsLenient.isEmpty());
    }
  }

  @Test
  public void testRecursiveCheck() {
    final Schema original = SchemaLoader.load(new JSONObject(Objects.requireNonNull(readFile("recursive-schema.json"))));
    final Schema newOne = SchemaLoader.load(new JSONObject(Objects.requireNonNull(readFile("recursive-schema.json"))));
    Assert.assertTrue(SchemaDiff.compare(original, newOne).isEmpty());
  }

  @Test(timeout = 10000)
  public void testRecursiveOneOfUnionTerminates() {
    final Schema original = SchemaLoader.load(recursiveOneOfTradeSchema());
    final Schema update = SchemaLoader.load(recursiveOneOfTradeSchema());
    Assert.assertTrue(SchemaDiff.compare(original, update).isEmpty());
  }

  // Memoization must not mask real differences in the recursive structure: a type change deep in
  // one product type is still detected as incompatible.
  @Test(timeout = 10000)
  public void testRecursiveOneOfUnionDetectsIncompatibleChange() {
    final Schema original = SchemaLoader.load(recursiveOneOfTradeSchema());
    final JSONObject changed = recursiveOneOfTradeSchema();
    changed.getJSONObject("definitions").getJSONObject("Asset")
        .getJSONObject("properties").getJSONObject("id").put("type", "number");
    final Schema update = SchemaLoader.load(changed);
    Assert.assertFalse(SchemaDiff.compare(original, update).isEmpty());
  }

  // A deterministic reproducer for cross-branch memoization. The genuine type changes to T0.id and
  // T1.id sit behind a recursive oneOf cycle (T0 -> T2 -> T0). A single memoized pass caches the
  // (T0,T0) sub-comparison as compatible while the cycle back into it is optimistically truncated,
  // then the root oneOf[T0,T1] matching reuses that cached edge and wrongly reports the evolution
  // compatible. The greatest-fixed-point iteration in SchemaDiff.compare corrects this; without it
  // this assertion fails (the comparison returns no incompatible differences).
  @Test(timeout = 10000)
  public void testRecursiveOneOfMemoizationDoesNotMaskIncompatibility() {
    final Schema original = SchemaLoader.load(memoMaskingSchema("integer", "boolean"));
    final Schema update = SchemaLoader.load(memoMaskingSchema("boolean", "number"));
    final boolean incompatible = SchemaDiff.compare(original, update).stream()
        .anyMatch(d -> !SchemaDiff.COMPATIBLE_CHANGES_STRICT.contains(d.getType()));
    Assert.assertTrue(
        "a recursive oneOf incompatibility must not be masked by cross-branch memoization",
        incompatible);
  }

  // T0 and T1 carry an "id" of the given types and a "ref" that is a singleton oneOf; T0.ref -> T2,
  // T1.ref -> T2, T2.ref -> T0 (so T0 -> T2 -> T0 is a cycle). The root is oneOf[T0, T1].
  private static JSONObject memoMaskingSchema(final String t0Id, final String t1Id) {
    final JSONObject definitions = new JSONObject();
    definitions.put("T0", recursiveObject(t0Id, "T2"));
    definitions.put("T1", recursiveObject(t1Id, "T2"));
    definitions.put("T2", recursiveObject("integer", "T0"));
    final JSONArray rootUnion = new JSONArray()
        .put(new JSONObject().put("$ref", "#/definitions/T0"))
        .put(new JSONObject().put("$ref", "#/definitions/T1"));
    return new JSONObject()
        .put("$schema", "http://json-schema.org/draft-07/schema#")
        .put("type", "object")
        .put("additionalProperties", true)
        .put("definitions", definitions)
        .put("properties", new JSONObject().put("root", new JSONObject().put("oneOf", rootUnion)));
  }

  private static JSONObject recursiveObject(final String idType, final String refTarget) {
    final JSONObject properties = new JSONObject();
    properties.put("id", new JSONObject().put("type", idType));
    properties.put("ref", new JSONObject().put("oneOf", new JSONArray()
        .put(new JSONObject().put("$ref", "#/definitions/" + refTarget))));
    return new JSONObject()
        .put("type", "object")
        .put("additionalProperties", true)
        .put("properties", properties);
  }

  private static JSONObject recursiveOneOfTradeSchema() {
    final String[] types = {"Asset", "Basket", "Swap", "Bond", "Option", "Future",
        "Forward", "Loan", "Repo", "Equity", "Index", "CommodityLeg"};
    final JSONObject definitions = new JSONObject();
    for (String type : types) {
      JSONObject properties = new JSONObject();
      properties.put("id", new JSONObject().put("type", "string"));
      properties.put("underlying", inlineUnion(types));
      properties.put("referenceProduct", inlineUnion(types));
      definitions.put(type, new JSONObject().put("type", "object").put("properties", properties));
    }
    JSONObject rootProperties = new JSONObject();
    rootProperties.put("tradeId", new JSONObject().put("type", "string"));
    rootProperties.put("product", inlineUnion(types));
    return new JSONObject()
        .put("$schema", "http://json-schema.org/draft-07/schema#")
        .put("title", "Trade")
        .put("type", "object")
        .put("additionalProperties", true)
        .put("definitions", definitions)
        .put("properties", rootProperties);
  }

  private static JSONObject inlineUnion(final String[] types) {
    JSONArray oneOf = new JSONArray();
    for (String type : types) {
      oneOf.put(new JSONObject().put("$ref", "#/definitions/" + type));
    }
    return new JSONObject().put("oneOf", oneOf);
  }

  @Test
  public void testSchemaAddsProperties() {
    final Schema first = SchemaLoader.load(new JSONObject("{}"));

    final Schema second = SchemaLoader.load(new JSONObject(("{\"properties\": {}}")));
    final List<Difference> changes = SchemaDiff.compare(first, second);
    // Changing from empty schema to empty object schema is incompatible
    Assert.assertFalse(changes.isEmpty());
  }

  @Test
  public void testConnectTypeAsBytes() {
    String firstSchema = "{\"type\":\"string\",\"title\":\"org.apache.kafka.connect.data.Decimal\","
        + "\"connect.version\":1,\"connect.type\":\"bytes\",\"connect.parameters\":{\"scale\":\"2\"}}";
    String secondSchema = "{\"type\":\"number\",\"title\":\"org.apache.kafka.connect.data.Decimal\","
        + "\"connect.version\":1,\"connect.type\":\"bytes\",\"connect.parameters\":{\"scale\":\"2\"}}";
    final Schema first = SchemaLoader.load(new JSONObject(firstSchema));
    final Schema second = SchemaLoader.load(new JSONObject(secondSchema));
    final List<Difference> changes = SchemaDiff.compare(first, second);
    Assert.assertTrue(changes.isEmpty());
  }

  public static String readFile(String fileName) {
    ClassLoader classLoader = ClassLoader.getSystemClassLoader();
    InputStream is = classLoader.getResourceAsStream(fileName);
    if (is != null) {
      BufferedReader reader = new BufferedReader(new InputStreamReader(is));
      return reader.lines().collect(Collectors.joining(System.lineSeparator()));
    }
    return null;
  }
}