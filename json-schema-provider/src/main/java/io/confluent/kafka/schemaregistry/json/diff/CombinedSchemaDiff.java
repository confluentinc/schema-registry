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

import org.everit.json.schema.CombinedSchema;
import org.everit.json.schema.Schema;

import java.util.Iterator;

import static io.confluent.kafka.schemaregistry.json.diff.Difference.Type.COMPOSITION_METHOD_CHANGED;
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
    if (!original.getCriterion().equals(update.getCriterion())) {
      ctx.addDifference(COMPOSITION_METHOD_CHANGED);
    } else {
      int originalSize = original.getSubschemas().size();
      int updateSize = update.getSubschemas().size();
      if (originalSize < updateSize) {
        if (original.getCriterion() == CombinedSchema.ALL_CRITERION) {
          ctx.addDifference(PRODUCT_TYPE_EXTENDED);
        } else {
          ctx.addDifference(SUM_TYPE_EXTENDED);
        }
      } else if (originalSize > updateSize) {
        if (original.getCriterion() == CombinedSchema.ALL_CRITERION) {
          ctx.addDifference(PRODUCT_TYPE_NARROWED);
        } else {
          ctx.addDifference(SUM_TYPE_NARROWED);
        }
      }

      final Iterator<Schema> originalIterator = original.getSubschemas().iterator();
      final Iterator<Schema> updateIterator = update.getSubschemas().iterator();
      int index = 0;
      while (originalIterator.hasNext() && index < Math.min(originalSize, updateSize)) {
        try (Context.PathScope pathScope = ctx.enterPath(original.getCriterion() + "/" + index)) {
          SchemaDiff.compare(ctx, originalIterator.next(), updateIterator.next());
        }
        index++;
      }
    }
  }
}
