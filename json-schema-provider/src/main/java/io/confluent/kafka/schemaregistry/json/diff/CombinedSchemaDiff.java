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

import io.confluent.kafka.schemaregistry.json.utils.Edge;
import io.confluent.kafka.schemaregistry.json.utils.MaximumCardinalityMatch;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import org.everit.json.schema.CombinedSchema;
import org.everit.json.schema.CombinedSchema.ValidationCriterion;
import org.everit.json.schema.Schema;

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
    Difference.Type type = compareCriteria(ctx, original.getCriterion(), update.getCriterion());
    if (type != COMBINED_TYPE_CHANGED) {
      // Use sets to collapse duplicate entries
      final Set<Schema> originalSubschemas = new LinkedHashSet<>(original.getSubschemas());
      final Set<Schema> updateSubschemas = new LinkedHashSet<>(update.getSubschemas());
      int originalSize = originalSubschemas.size();
      int updateSize = updateSubschemas.size();
      if (originalSize < updateSize) {
        if (update.getCriterion() == CombinedSchema.ALL_CRITERION) {
          ctx.addDifference(PRODUCT_TYPE_EXTENDED);
        } else {
          ctx.addDifference(SUM_TYPE_EXTENDED);
        }
      } else if (originalSize > updateSize) {
        if (update.getCriterion() == CombinedSchema.ALL_CRITERION) {
          ctx.addDifference(PRODUCT_TYPE_NARROWED);
        } else {
          ctx.addDifference(SUM_TYPE_NARROWED);
        }
      }

      int index = 0;
      Set<Edge<Schema, List<Difference>>> compatibleEdges = new HashSet<>();
      for (Schema originalSubschema : originalSubschemas) {
        try (Context.PathScope pathScope = ctx.enterPath(original.getCriterion() + "/" + index)) {
          for (Schema updateSubschema : updateSubschemas) {
            final Context subctx = ctx.getSubcontext();
            SchemaDiff.compare(subctx, originalSubschema, updateSubschema);
            if (subctx.isCompatible()) {
              compatibleEdges.add(
                  new Edge(originalSubschema, updateSubschema, subctx.getDifferences()));
            }
          }
        }
        index++;
      }

      MaximumCardinalityMatch<Schema, List<Difference>> match =
          new MaximumCardinalityMatch<>(compatibleEdges, originalSubschemas, updateSubschemas);
      Set<Edge<Schema, List<Difference>>> matching = match.getMatching();

      for (Edge<Schema, List<Difference>> matchingEdge : matching) {
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
      final ValidationCriterion original,
      final ValidationCriterion update
  ) {
    Difference.Type type;
    if (original.equals(update)) {
      type = null;
    } else if (update == CombinedSchema.ANY_CRITERION) {
      type = COMBINED_TYPE_EXTENDED;
    } else {
      type = COMBINED_TYPE_CHANGED;
    }
    if (type != null) {
      ctx.addDifference(type);
    }
    return type;
  }
}
