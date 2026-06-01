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

package io.confluent.kafka.schemaregistry.json.diff;

import io.confluent.kafka.schemaregistry.json.JsonSchema;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import org.junit.Test;

import static org.junit.Assert.fail;

/**
 * Tests that SchemaDiff.compare does not hang on JSON Schemas with circular $ref references.
 *
 * <p>Reproduces a production incident where a Draft 2020-12 schema with a recursive oneOf
 * structure (12 product types, each with ~5 properties referencing ProductOrCuid, which
 * references back to Product via oneOf) caused the compatibility checker to hang for hours,
 * holding a per-tenant lock and blocking all writes.
 *
 * <p><b>Root cause:</b> {@code SchemaTranslator.close()} calls {@code builder.build()} separately
 * for each {@code $ref} resolution, creating N distinct Java objects for the same logical
 * definition (where N = number of {@code $ref}s pointing to that definition). Since
 * {@link Context} uses an {@code IdentityHashMap} for cycle detection via
 * {@code enterSchema()}, it cannot recognize these distinct objects as the same schema.
 * Combined with the O(n²) cross-product matching in {@link CombinedSchemaDiff} for
 * oneOf/anyOf, this creates a combinatorial explosion that grows exponentially with the
 * number of oneOf branches and circular properties.
 */
public class CircularRefSchemaDiffTest {

  /**
   * Small schema (2 branches, 1 circular prop) — should complete almost instantly.
   * Serves as a baseline to verify the test harness works.
   */
  @Test
  public void testCircularSchemaComparisonWith2Branches() {
    String schema = circularSchema(2, 1);
    assertComparisonTerminates("2 branches x 1 prop", schema, schema);
  }

  /**
   * Production-scale schema (12 branches, 5 circular props) — reproduces the hang.
   * This matches the customer's schema which had 12 product types in oneOf, each with
   * ~5 properties referencing ProductOrCuid.
   */
  @Test
  public void testCircularSchema12BranchesX5Props() {
    String schema = circularSchema(12, 5);
    assertComparisonTerminates("12 branches x 5 circular props", schema, schema);
  }


  /**
   * Generates a Draft 2020-12 JSON Schema with circular $ref through oneOf.
   *
   * <p>Structure:
   * <pre>
   *   Product (oneOf) -> TypeN ($ref Base + properties with refP -> ProductOrId)
   *   ProductOrId (oneOf) -> [SimpleId, Product]  (circular back to Product)
   * </pre>
   *
   * Each TypeN uses $ref alongside properties (Draft 2020-12 semantics), causing
   * SchemaTranslator to create allOf wrappers (CombinedSchemaExt).
   *
   * @param numBranches     number of oneOf branches in Product
   * @param numCircularProps number of properties per type that reference ProductOrId
   */
  private static String circularSchema(int numBranches, int numCircularProps) {
    StringBuilder sb = new StringBuilder();
    sb.append("{");
    sb.append("\"$schema\":\"https://json-schema.org/draft/2020-12/schema\",");
    sb.append("\"$defs\":{");
    sb.append("\"SimpleId\":{\"type\":\"object\",\"properties\":{\"id\":{\"type\":\"string\"}},\"required\":[\"id\"]},");
    sb.append("\"Base\":{\"type\":\"object\",\"properties\":{\"name\":{\"type\":\"string\"}}},");
    sb.append("\"ProductOrId\":{\"oneOf\":[{\"$ref\":\"#/$defs/SimpleId\"},{\"$ref\":\"#/$defs/Product\"}]},");

    for (int i = 0; i < numBranches; i++) {
      sb.append("\"Type").append(i).append("\":{");
      sb.append("\"$ref\":\"#/$defs/Base\",");
      sb.append("\"type\":\"object\",");
      sb.append("\"properties\":{");
      for (int p = 0; p < numCircularProps; p++) {
        sb.append("\"ref").append(p).append("\":{\"$ref\":\"#/$defs/ProductOrId\"},");
      }
      sb.append("\"tag\":{\"type\":\"string\",\"const\":\"type").append(i).append("\"}");
      sb.append("},");
      sb.append("\"required\":[\"ref0\",\"tag\"]},");
    }

    sb.append("\"Product\":{\"oneOf\":[");
    for (int i = 0; i < numBranches; i++) {
      sb.append("{\"$ref\":\"#/$defs/Type").append(i).append("\"}");
      if (i < numBranches - 1) sb.append(",");
    }
    sb.append("]}},");
    sb.append("\"$ref\":\"#/$defs/Product\"}");
    return sb.toString();
  }

  private void assertComparisonTerminates(
      String description, String originalSchemaStr, String updateSchemaStr) {
    JsonSchema original = new JsonSchema(originalSchemaStr);
    JsonSchema update = new JsonSchema(updateSchemaStr);

    ExecutorService executor = Executors.newSingleThreadExecutor();
    try {
      Future<List<Difference>> future = executor.submit(
          () -> SchemaDiff.compare(original.rawSchema(), update.rawSchema()));
      try {
        List<Difference> result = future.get(10, TimeUnit.SECONDS);
      } catch (TimeoutException e) {
        future.cancel(true);
        fail(description + ": SchemaDiff.compare did not terminate within 10 seconds. "
            + "Circular $ref caused combinatorial explosion.");
      } catch (ExecutionException e) {
        if (e.getCause() instanceof StackOverflowError) {
          fail(description + ": StackOverflowError from circular $ref recursion.");
        }
        throw new RuntimeException(description + ": unexpected exception", e);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        fail(description + ": interrupted");
      }
    } finally {
      executor.shutdownNow();
    }
  }

}