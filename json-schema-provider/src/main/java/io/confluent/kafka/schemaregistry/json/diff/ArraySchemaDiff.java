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
import org.everit.json.schema.EmptySchema;
import org.everit.json.schema.FalseSchema;
import org.everit.json.schema.Schema;

import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;

import static io.confluent.kafka.schemaregistry.json.diff.Difference.Type.ADDITIONAL_ITEMS_ADDED;
import static io.confluent.kafka.schemaregistry.json.diff.Difference.Type.ADDITIONAL_ITEMS_EXTENDED;
import static io.confluent.kafka.schemaregistry.json.diff.Difference.Type.ADDITIONAL_ITEMS_NARROWED;
import static io.confluent.kafka.schemaregistry.json.diff.Difference.Type.ADDITIONAL_ITEMS_REMOVED;
import static io.confluent.kafka.schemaregistry.json.diff.Difference.Type.ITEM_ADDED_IS_COVERED_BY_PARTIALLY_OPEN_CONTENT_MODEL;
import static io.confluent.kafka.schemaregistry.json.diff.Difference.Type.ITEM_ADDED_NOT_COVERED_BY_PARTIALLY_OPEN_CONTENT_MODEL;
import static io.confluent.kafka.schemaregistry.json.diff.Difference.Type.ITEM_ADDED_TO_CLOSED_CONTENT_MODEL;
import static io.confluent.kafka.schemaregistry.json.diff.Difference.Type.ITEM_ADDED_TO_OPEN_CONTENT_MODEL;
import static io.confluent.kafka.schemaregistry.json.diff.Difference.Type.ITEM_REMOVED_FROM_CLOSED_CONTENT_MODEL;
import static io.confluent.kafka.schemaregistry.json.diff.Difference.Type.ITEM_REMOVED_FROM_OPEN_CONTENT_MODEL;
import static io.confluent.kafka.schemaregistry.json.diff.Difference.Type.ITEM_REMOVED_IS_COVERED_BY_PARTIALLY_OPEN_CONTENT_MODEL;
import static io.confluent.kafka.schemaregistry.json.diff.Difference.Type.ITEM_REMOVED_NOT_COVERED_BY_PARTIALLY_OPEN_CONTENT_MODEL;
import static io.confluent.kafka.schemaregistry.json.diff.Difference.Type.ITEM_WITH_EMPTY_SCHEMA_ADDED_TO_OPEN_CONTENT_MODEL;
import static io.confluent.kafka.schemaregistry.json.diff.Difference.Type.ITEM_WITH_FALSE_REMOVED_FROM_CLOSED_CONTENT_MODEL;
import static io.confluent.kafka.schemaregistry.json.diff.Difference.Type.MAX_CONTAINS_ADDED;
import static io.confluent.kafka.schemaregistry.json.diff.Difference.Type.MAX_CONTAINS_DECREASED;
import static io.confluent.kafka.schemaregistry.json.diff.Difference.Type.MAX_CONTAINS_INCREASED;
import static io.confluent.kafka.schemaregistry.json.diff.Difference.Type.MAX_CONTAINS_REMOVED;
import static io.confluent.kafka.schemaregistry.json.diff.Difference.Type.MAX_ITEMS_ADDED;
import static io.confluent.kafka.schemaregistry.json.diff.Difference.Type.MAX_ITEMS_DECREASED;
import static io.confluent.kafka.schemaregistry.json.diff.Difference.Type.MAX_ITEMS_INCREASED;
import static io.confluent.kafka.schemaregistry.json.diff.Difference.Type.MAX_ITEMS_REMOVED;
import static io.confluent.kafka.schemaregistry.json.diff.Difference.Type.MIN_CONTAINS_ADDED;
import static io.confluent.kafka.schemaregistry.json.diff.Difference.Type.MIN_CONTAINS_DECREASED;
import static io.confluent.kafka.schemaregistry.json.diff.Difference.Type.MIN_CONTAINS_INCREASED;
import static io.confluent.kafka.schemaregistry.json.diff.Difference.Type.MIN_CONTAINS_REMOVED;
import static io.confluent.kafka.schemaregistry.json.diff.Difference.Type.MIN_ITEMS_ADDED;
import static io.confluent.kafka.schemaregistry.json.diff.Difference.Type.MIN_ITEMS_DECREASED;
import static io.confluent.kafka.schemaregistry.json.diff.Difference.Type.MIN_ITEMS_INCREASED;
import static io.confluent.kafka.schemaregistry.json.diff.Difference.Type.MIN_ITEMS_REMOVED;
import static io.confluent.kafka.schemaregistry.json.diff.Difference.Type.UNIQUE_ITEMS_ADDED;
import static io.confluent.kafka.schemaregistry.json.diff.Difference.Type.UNEVALUATED_ITEMS_ADDED;
import static io.confluent.kafka.schemaregistry.json.diff.Difference.Type.UNEVALUATED_ITEMS_EXTENDED;
import static io.confluent.kafka.schemaregistry.json.diff.Difference.Type.UNEVALUATED_ITEMS_NARROWED;
import static io.confluent.kafka.schemaregistry.json.diff.Difference.Type.UNEVALUATED_ITEMS_REMOVED;
import static io.confluent.kafka.schemaregistry.json.diff.Difference.Type.UNIQUE_ITEMS_REMOVED;

public class ArraySchemaDiff {
  static void compare(final Context ctx, final ArraySchema original, final ArraySchema update) {
    compareItemSchemaObject(ctx, original, update);
    compareItemSchemaArray(ctx, original, update);
    compareAdditionalItems(ctx, original, update);
    compareUnevaluatedItems(ctx, original, update);
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
    compareContainsAttribute(ctx, "maxContains",
        getMaxContains(original), getMaxContains(update),
        MAX_CONTAINS_ADDED, MAX_CONTAINS_REMOVED,
        MAX_CONTAINS_INCREASED, MAX_CONTAINS_DECREASED);
    compareContainsAttribute(ctx, "minContains",
        getMinContains(original), getMinContains(update),
        MIN_CONTAINS_ADDED, MIN_CONTAINS_REMOVED,
        MIN_CONTAINS_INCREASED, MIN_CONTAINS_DECREASED);
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

  private static void compareUnevaluatedItems(
      final Context ctx, final ArraySchema original, final ArraySchema update
  ) {
    Schema originalUneval = getUnevaluatedItems(original);
    Schema updateUneval = getUnevaluatedItems(update);
    if (originalUneval == null && updateUneval == null) {
      return;
    }
    try (Context.PathScope pathScope = ctx.enterPath("unevaluatedItems")) {
      if (originalUneval == null) {
        if (updateUneval instanceof FalseSchema) {
          ctx.addDifference(UNEVALUATED_ITEMS_REMOVED);
        } else {
          ctx.addDifference(UNEVALUATED_ITEMS_NARROWED);
        }
      } else if (updateUneval == null) {
        if (originalUneval instanceof FalseSchema) {
          ctx.addDifference(UNEVALUATED_ITEMS_ADDED);
        } else {
          ctx.addDifference(UNEVALUATED_ITEMS_EXTENDED);
        }
      } else if (originalUneval instanceof FalseSchema && !(updateUneval instanceof FalseSchema)) {
        ctx.addDifference(UNEVALUATED_ITEMS_ADDED);
      } else if (!(originalUneval instanceof FalseSchema) && updateUneval instanceof FalseSchema) {
        ctx.addDifference(UNEVALUATED_ITEMS_REMOVED);
      } else if (originalUneval instanceof EmptySchema
          && !(updateUneval instanceof EmptySchema)) {
        ctx.addDifference(UNEVALUATED_ITEMS_NARROWED);
      } else if (!(originalUneval instanceof EmptySchema)
          && updateUneval instanceof EmptySchema) {
        ctx.addDifference(UNEVALUATED_ITEMS_EXTENDED);
      } else {
        SchemaDiff.compare(ctx, originalUneval, updateUneval);
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

    final Iterator<Schema> originalIterator = originalSchemas.iterator();
    final Iterator<Schema> updateIterator = updateSchemas.iterator();
    int index = 0;
    while (originalIterator.hasNext() && index < Math.min(originalSize, updateSize)) {
      try (Context.PathScope pathScope = ctx.enterPath("items/" + index)) {
        SchemaDiff.compare(ctx, originalIterator.next(), updateIterator.next());
      }
      index++;
    }
    while (originalIterator.hasNext()) {
      try (Context.PathScope pathScope = ctx.enterPath("items/" + index)) {
        Schema originalSchema = originalIterator.next();
        if (isOpenContentModel(update)) {
          // compatible
          ctx.addDifference(ITEM_REMOVED_FROM_OPEN_CONTENT_MODEL);
        } else {
          Schema schemaFromPartial = schemaFromPartiallyOpenContentModel(update);
          if (schemaFromPartial != null) {
            final Context subctx = ctx.getSubcontext();
            SchemaDiff.compare(subctx, originalSchema, schemaFromPartial);
            ctx.addDifferences(subctx.getDifferences());
            if (subctx.isCompatible()) {
              // compatible
              ctx.addDifference(ITEM_REMOVED_IS_COVERED_BY_PARTIALLY_OPEN_CONTENT_MODEL);
            } else {
              // incompatible
              ctx.addDifference(ITEM_REMOVED_NOT_COVERED_BY_PARTIALLY_OPEN_CONTENT_MODEL);
            }
          } else {
            if (originalSchema instanceof FalseSchema) {
              // compatible
              ctx.addDifference(ITEM_WITH_FALSE_REMOVED_FROM_CLOSED_CONTENT_MODEL);
            } else {
              // incompatible
              ctx.addDifference(ITEM_REMOVED_FROM_CLOSED_CONTENT_MODEL);
            }
          }
        }
      }
      index++;
    }
    while (updateIterator.hasNext()) {
      try (Context.PathScope pathScope = ctx.enterPath("items/" + index)) {
        Schema updateSchema = updateIterator.next();
        if (isOpenContentModel(original)) {
          if (updateSchema instanceof EmptySchema) {
            // compatible
            ctx.addDifference(ITEM_WITH_EMPTY_SCHEMA_ADDED_TO_OPEN_CONTENT_MODEL);
          } else {
            // incompatible
            ctx.addDifference(ITEM_ADDED_TO_OPEN_CONTENT_MODEL);
          }
        } else {
          Schema schemaFromPartial = schemaFromPartiallyOpenContentModel(original);
          if (schemaFromPartial != null) {
            final Context subctx = ctx.getSubcontext();
            SchemaDiff.compare(subctx, schemaFromPartial, updateSchema);
            ctx.addDifferences(subctx.getDifferences());
            if (subctx.isCompatible()) {
              // compatible
              ctx.addDifference(ITEM_ADDED_IS_COVERED_BY_PARTIALLY_OPEN_CONTENT_MODEL);
            } else {
              // incompatible
              ctx.addDifference(ITEM_ADDED_NOT_COVERED_BY_PARTIALLY_OPEN_CONTENT_MODEL);
            }
          } else {
            // compatible
            ctx.addDifference(ITEM_ADDED_TO_CLOSED_CONTENT_MODEL);
          }
        }
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

  private static void compareContainsAttribute(
      final Context ctx, final String name,
      final Number originalVal, final Number updateVal,
      final Difference.Type added, final Difference.Type removed,
      final Difference.Type increased, final Difference.Type decreased) {
    if (!Objects.equals(originalVal, updateVal)) {
      if (originalVal == null) {
        ctx.addDifference(name, added);
      } else if (updateVal == null) {
        ctx.addDifference(name, removed);
      } else if (originalVal.intValue() < updateVal.intValue()) {
        ctx.addDifference(name, increased);
      } else if (originalVal.intValue() > updateVal.intValue()) {
        ctx.addDifference(name, decreased);
      }
    }
  }

  private static Number getMaxContains(final ArraySchema schema) {
    Object val = schema.getUnprocessedProperties().get("maxContains");
    return val instanceof Number ? (Number) val : null;
  }

  private static Number getMinContains(final ArraySchema schema) {
    Object val = schema.getUnprocessedProperties().get("minContains");
    return val instanceof Number ? (Number) val : null;
  }

  private static Schema getUnevaluatedItems(final ArraySchema schema) {
    Object uneval = schema.getUnprocessedProperties().get("unevaluatedItems");
    return uneval instanceof Schema ? (Schema) uneval : null;
  }

  private static boolean isAdditionalItemsAbsent(final ArraySchema schema) {
    return schema.permitsAdditionalItems()
        && schema.getSchemaOfAdditionalItems() == null;
  }

  static boolean isOpenContentModel(final ArraySchema schema) {
    // Fully open = (A = true) or (A is missing and U is missing or true)
    if (!schema.permitsAdditionalItems()) {
      return false;
    }
    if (schema.getSchemaOfAdditionalItems() != null) {
      return false;
    }
    // A is absent — check U
    Schema uneval = getUnevaluatedItems(schema);
    return uneval == null || uneval instanceof EmptySchema;
  }

  private static Schema schemaFromPartiallyOpenContentModel(final ArraySchema schema) {
    // Partially open = (A = S) or (A is missing and U = S)
    if (schema.getSchemaOfAdditionalItems() != null) {
      return schema.getSchemaOfAdditionalItems();
    }
    // Check unevaluatedItems (A is missing and U = S)
    if (isAdditionalItemsAbsent(schema)) {
      Schema uneval = getUnevaluatedItems(schema);
      if (uneval != null && !(uneval instanceof FalseSchema)
          && !(uneval instanceof EmptySchema)) {
        return uneval;
      }
    }
    return null;
  }
}
