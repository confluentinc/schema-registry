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

import io.confluent.kafka.schemaregistry.json.JsonSchema;
import io.confluent.kafka.schemaregistry.json.utils.Edge;
import io.confluent.kafka.schemaregistry.json.utils.MaximumCardinalityMatch;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import org.everit.json.schema.CombinedSchema;
import org.everit.json.schema.CombinedSchema.ValidationCriterion;

import static io.confluent.kafka.schemaregistry.json.diff.Difference.Type.COMBINED_TYPE_CHANGED;
import static io.confluent.kafka.schemaregistry.json.diff.Difference.Type.COMBINED_TYPE_EXTENDED;
import static io.confluent.kafka.schemaregistry.json.diff.Difference.Type.COMBINED_TYPE_SUBSCHEMAS_CHANGED;
import static io.confluent.kafka.schemaregistry.json.diff.Difference.Type.PRODUCT_TYPE_EXTENDED;
import static io.confluent.kafka.schemaregistry.json.diff.Difference.Type.PRODUCT_TYPE_NARROWED;
import static io.confluent.kafka.schemaregistry.json.diff.Difference.Type.SUM_TYPE_EXTENDED;
import static io.confluent.kafka.schemaregistry.json.diff.Difference.Type.SUM_TYPE_NARROWED;

class CombinedSchemaDiff {
  static void compare(
      final Context ctx,
      final CombinedSchema original,
      final CombinedSchema update
  ) {
    Difference.Type type = compareCriteria(ctx, original, update);
    if (type != COMBINED_TYPE_CHANGED) {
      // Use sets to collapse duplicate entries
      final Set<JsonSchema> originalSubschemas = original.getSubschemas().stream()
          .map(JsonSchema::new).collect(Collectors.toCollection(LinkedHashSet::new));
      final Set<JsonSchema> updateSubschemas = update.getSubschemas().stream()
          .map(JsonSchema::new).collect(Collectors.toCollection(LinkedHashSet::new));
      int originalSize = originalSubschemas.size();
      int updateSize = updateSubschemas.size();
      if (originalSize < updateSize) {
        if (update.getCriterion() == CombinedSchema.ALL_CRITERION) {
          ctx.addDifference(PRODUCT_TYPE_EXTENDED);
        } else {
          ctx.addDifference(SUM_TYPE_EXTENDED);
        }
      } else if (originalSize > updateSize) {
        if (original.getCriterion() == CombinedSchema.ANY_CRITERION
            || original.getCriterion() == CombinedSchema.ONE_CRITERION) {
          ctx.addDifference(SUM_TYPE_NARROWED);
        } else {
          ctx.addDifference(PRODUCT_TYPE_NARROWED);
        }
      }

      int index = 0;
      Set<Edge<JsonSchema, List<Difference>>> compatibleEdges = new HashSet<>();
      for (JsonSchema originalSubschema : originalSubschemas) {
        try (Context.PathScope pathScope = ctx.enterPath(original.getCriterion() + "/" + index)) {
          for (JsonSchema updateSubschema : updateSubschemas) {
            final Context subctx = ctx.getSubcontext();
            SchemaDiff.compare(subctx, originalSubschema.rawSchema(), updateSubschema.rawSchema());
            if (subctx.isCompatible()) {
              compatibleEdges.add(
                  new Edge<>(originalSubschema, updateSubschema, subctx.getDifferences()));
            }
          }
        }
        index++;
      }

      MaximumCardinalityMatch<JsonSchema, List<Difference>> match =
          new MaximumCardinalityMatch<>(compatibleEdges, originalSubschemas, updateSubschemas);
      Set<Edge<JsonSchema, List<Difference>>> matching = match.getMatching();

      for (Edge<JsonSchema, List<Difference>> matchingEdge : matching) {
        ctx.addDifferences(matchingEdge.value());
      }
      if (matching.size() < Math.min(originalSize, updateSize)) {
        // Did not find a matching that covers the smaller partition,
        // ensure the result is incompatible
        ctx.addDifference(COMBINED_TYPE_SUBSCHEMAS_CHANGED);
      }
    }
  }

  private static Difference.Type compareCriteria(
      final Context ctx,
      final CombinedSchema original,
      final CombinedSchema update
  ) {
    ValidationCriterion originalCriterion = original.getCriterion();
    ValidationCriterion updateCriterion = update.getCriterion();
    Difference.Type type;
    if (originalCriterion.equals(updateCriterion)) {
      type = null;
    } else if (updateCriterion == CombinedSchema.ANY_CRITERION
        || (isSingleton(original) && isSingleton(update))
        || (isSingleton(original) && updateCriterion == CombinedSchema.ONE_CRITERION)
        || (isSingleton(update) && originalCriterion == CombinedSchema.ALL_CRITERION)) {
      type = COMBINED_TYPE_EXTENDED;
    } else {
      type = COMBINED_TYPE_CHANGED;
    }
    if (type != null) {
      ctx.addDifference(type);
    }
    return type;
  }

  private static boolean isSingleton(CombinedSchema schema) {
    return schema.getSubschemas().size() == 1;
  }
}
