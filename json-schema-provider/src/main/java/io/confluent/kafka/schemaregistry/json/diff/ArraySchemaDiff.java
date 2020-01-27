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

import org.everit.json.schema.ArraySchema;
import org.everit.json.schema.Schema;

import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;

import static io.confluent.kafka.schemaregistry.json.diff.Difference.Type.ADDITIONAL_ITEMS_ADDED;
import static io.confluent.kafka.schemaregistry.json.diff.Difference.Type.ADDITIONAL_ITEMS_EXTENDED;
import static io.confluent.kafka.schemaregistry.json.diff.Difference.Type.ADDITIONAL_ITEMS_NARROWED;
import static io.confluent.kafka.schemaregistry.json.diff.Difference.Type.ADDITIONAL_ITEMS_REMOVED;
import static io.confluent.kafka.schemaregistry.json.diff.Difference.Type.ITEMS_ADDED_TO_CLOSED_CONTENT_MODEL;
import static io.confluent.kafka.schemaregistry.json.diff.Difference.Type.ITEMS_ADDED_TO_OPEN_CONTENT_MODEL;
import static io.confluent.kafka.schemaregistry.json.diff.Difference.Type.ITEMS_REMOVED_FROM_CLOSED_CONTENT_MODEL;
import static io.confluent.kafka.schemaregistry.json.diff.Difference.Type.ITEMS_REMOVED_FROM_OPEN_CONTENT_MODEL;
import static io.confluent.kafka.schemaregistry.json.diff.Difference.Type.MAX_ITEMS_ADDED;
import static io.confluent.kafka.schemaregistry.json.diff.Difference.Type.MAX_ITEMS_DECREASED;
import static io.confluent.kafka.schemaregistry.json.diff.Difference.Type.MAX_ITEMS_INCREASED;
import static io.confluent.kafka.schemaregistry.json.diff.Difference.Type.MAX_ITEMS_REMOVED;
import static io.confluent.kafka.schemaregistry.json.diff.Difference.Type.MIN_ITEMS_ADDED;
import static io.confluent.kafka.schemaregistry.json.diff.Difference.Type.MIN_ITEMS_DECREASED;
import static io.confluent.kafka.schemaregistry.json.diff.Difference.Type.MIN_ITEMS_INCREASED;
import static io.confluent.kafka.schemaregistry.json.diff.Difference.Type.MIN_ITEMS_REMOVED;
import static io.confluent.kafka.schemaregistry.json.diff.Difference.Type.UNIQUE_ITEMS_ADDED;
import static io.confluent.kafka.schemaregistry.json.diff.Difference.Type.UNIQUE_ITEMS_REMOVED;

public class ArraySchemaDiff {
  static void compare(final Context ctx, final ArraySchema original, final ArraySchema update) {
    compareItemSchemaObject(ctx, original, update);
    compareItemSchemaArray(ctx, original, update);
    compareAdditionalItems(ctx, original, update);
    compareAttributes(ctx, original, update);
  }

  private static void compareAttributes(
      final Context ctx, final ArraySchema original, final ArraySchema update
  ) {
    if (!Objects.equals(original.getMaxItems(), update.getMaxItems())) {
      if (original.getMaxItems() == null && update.getMaxItems() != null) {
        ctx.addDifference("maxItems", MAX_ITEMS_ADDED);
      } else if (original.getMaxItems() != null && update.getMaxItems() == null) {
        ctx.addDifference("maxItems", MAX_ITEMS_REMOVED);
      } else if (original.getMaxItems() < update.getMaxItems()) {
        ctx.addDifference("maxItems", MAX_ITEMS_INCREASED);
      } else if (original.getMaxItems() > update.getMaxItems()) {
        ctx.addDifference("maxItems", MAX_ITEMS_DECREASED);
      }
    }
    if (!Objects.equals(original.getMinItems(), update.getMinItems())) {
      if (original.getMinItems() == null && update.getMinItems() != null) {
        ctx.addDifference("minItems", MIN_ITEMS_ADDED);
      } else if (original.getMinItems() != null && update.getMinItems() == null) {
        ctx.addDifference("minItems", MIN_ITEMS_REMOVED);
      } else if (original.getMinItems() < update.getMinItems()) {
        ctx.addDifference("minItems", MIN_ITEMS_INCREASED);
      } else if (original.getMinItems() > update.getMinItems()) {
        ctx.addDifference("minItems", MIN_ITEMS_DECREASED);
      }
    }
    if (original.needsUniqueItems() != update.needsUniqueItems()) {
      if (original.needsUniqueItems()) {
        ctx.addDifference("uniqueItems", UNIQUE_ITEMS_REMOVED);
      } else {
        ctx.addDifference("uniqueItems", UNIQUE_ITEMS_ADDED);
      }
    }
  }

  private static void compareAdditionalItems(
      final Context ctx, final ArraySchema original, final ArraySchema update
  ) {
    try (Context.PathScope pathScope = ctx.enterPath("additionalItems")) {
      if (original.permitsAdditionalItems() != update.permitsAdditionalItems()) {
        if (original.permitsAdditionalItems()) {
          ctx.addDifference(ADDITIONAL_ITEMS_REMOVED);
        } else {
          ctx.addDifference(ADDITIONAL_ITEMS_ADDED);
        }
      } else if (original.getSchemaOfAdditionalItems() == null
          && update.getSchemaOfAdditionalItems() != null) {
        ctx.addDifference(ADDITIONAL_ITEMS_NARROWED);
      } else if (update.getSchemaOfAdditionalItems() == null
          && original.getSchemaOfAdditionalItems() != null) {
        ctx.addDifference(ADDITIONAL_ITEMS_EXTENDED);
      } else {
        SchemaDiff.compare(
            ctx,
            original.getSchemaOfAdditionalItems(),
            update.getSchemaOfAdditionalItems()
        );
      }
    }
  }

  private static void compareItemSchemaArray(
      final Context ctx, final ArraySchema original, final ArraySchema update
  ) {
    List<Schema> originalSchemas = original.getItemSchemas();
    if (originalSchemas == null) {
      originalSchemas = Collections.emptyList();
    }
    List<Schema> updateSchemas = update.getItemSchemas();
    if (updateSchemas == null) {
      updateSchemas = Collections.emptyList();
    }
    int originalSize = originalSchemas.size();
    int updateSize = updateSchemas.size();

    if (originalSize < updateSize) {
      ctx.addDifference(original.permitsAdditionalItems()
                        ? ITEMS_ADDED_TO_OPEN_CONTENT_MODEL
                        : ITEMS_ADDED_TO_CLOSED_CONTENT_MODEL);
    } else if (originalSize > updateSize) {
      ctx.addDifference(original.permitsAdditionalItems()
                        ? ITEMS_REMOVED_FROM_OPEN_CONTENT_MODEL
                        : ITEMS_REMOVED_FROM_CLOSED_CONTENT_MODEL);
    }

    final Iterator<Schema> originalIterator = originalSchemas.iterator();
    final Iterator<Schema> updateIterator = updateSchemas.iterator();
    int index = 0;
    while (originalIterator.hasNext() && index < Math.min(originalSize, updateSize)) {
      try (Context.PathScope pathScope = ctx.enterPath("items/" + index)) {
        SchemaDiff.compare(ctx, originalIterator.next(), updateIterator.next());
      }
      index++;
    }
  }

  private static void compareItemSchemaObject(
      final Context ctx, final ArraySchema original, final ArraySchema update
  ) {
    try (Context.PathScope pathScope = ctx.enterPath("items")) {
      SchemaDiff.compare(ctx, original.getAllItemSchema(), update.getAllItemSchema());
    }
  }
}
