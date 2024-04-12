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

import io.confluent.kafka.schemaregistry.json.diff.Difference.Type;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import org.everit.json.schema.ArraySchema;
import org.everit.json.schema.CombinedSchema;
import org.everit.json.schema.EmptySchema;
import org.everit.json.schema.EnumSchema;
import org.everit.json.schema.FalseSchema;
import org.everit.json.schema.NotSchema;
import org.everit.json.schema.NumberSchema;
import org.everit.json.schema.ObjectSchema;
import org.everit.json.schema.ReferenceSchema;
import org.everit.json.schema.Schema;
import org.everit.json.schema.StringSchema;

public class SchemaDiff {
  public static final Set<Difference.Type> COMPATIBLE_CHANGES;

  static {
    Set<Difference.Type> changes = new HashSet<>();

    changes.add(Type.ID_CHANGED);
    changes.add(Type.DESCRIPTION_CHANGED);
    changes.add(Type.TITLE_CHANGED);
    changes.add(Type.DEFAULT_CHANGED);
    changes.add(Type.SCHEMA_REMOVED);
    changes.add(Type.TYPE_EXTENDED);

    changes.add(Type.MAX_LENGTH_INCREASED);
    changes.add(Type.MAX_LENGTH_REMOVED);
    changes.add(Type.MIN_LENGTH_DECREASED);
    changes.add(Type.MIN_LENGTH_REMOVED);
    changes.add(Type.PATTERN_REMOVED);

    changes.add(Type.MAXIMUM_INCREASED);
    changes.add(Type.MAXIMUM_REMOVED);
    changes.add(Type.MINIMUM_DECREASED);
    changes.add(Type.MINIMUM_REMOVED);
    changes.add(Type.EXCLUSIVE_MAXIMUM_INCREASED);
    changes.add(Type.EXCLUSIVE_MAXIMUM_REMOVED);
    changes.add(Type.EXCLUSIVE_MINIMUM_DECREASED);
    changes.add(Type.EXCLUSIVE_MINIMUM_REMOVED);
    changes.add(Type.MULTIPLE_OF_REDUCED);
    changes.add(Type.MULTIPLE_OF_REMOVED);

    changes.add(Type.REQUIRED_ATTRIBUTE_WITH_DEFAULT_ADDED);
    changes.add(Type.REQUIRED_ATTRIBUTE_REMOVED);
    changes.add(Type.DEPENDENCY_ARRAY_NARROWED);
    changes.add(Type.DEPENDENCY_ARRAY_REMOVED);
    changes.add(Type.DEPENDENCY_SCHEMA_REMOVED);
    changes.add(Type.MAX_PROPERTIES_INCREASED);
    changes.add(Type.MAX_PROPERTIES_REMOVED);
    changes.add(Type.MIN_PROPERTIES_DECREASED);
    changes.add(Type.MIN_PROPERTIES_REMOVED);
    changes.add(Type.ADDITIONAL_PROPERTIES_ADDED);
    changes.add(Type.ADDITIONAL_PROPERTIES_EXTENDED);
    changes.add(Type.PROPERTY_WITH_EMPTY_SCHEMA_ADDED_TO_OPEN_CONTENT_MODEL);
    changes.add(Type.REQUIRED_PROPERTY_WITH_DEFAULT_ADDED_TO_UNOPEN_CONTENT_MODEL);
    changes.add(Type.OPTIONAL_PROPERTY_ADDED_TO_UNOPEN_CONTENT_MODEL);
    changes.add(Type.PROPERTY_WITH_FALSE_REMOVED_FROM_CLOSED_CONTENT_MODEL);
    changes.add(Type.PROPERTY_REMOVED_FROM_OPEN_CONTENT_MODEL);
    changes.add(Type.PROPERTY_ADDED_IS_COVERED_BY_PARTIALLY_OPEN_CONTENT_MODEL);
    changes.add(Type.PROPERTY_REMOVED_IS_COVERED_BY_PARTIALLY_OPEN_CONTENT_MODEL);

    changes.add(Type.MAX_ITEMS_INCREASED);
    changes.add(Type.MAX_ITEMS_REMOVED);
    changes.add(Type.MIN_ITEMS_DECREASED);
    changes.add(Type.MIN_ITEMS_REMOVED);
    changes.add(Type.UNIQUE_ITEMS_REMOVED);
    changes.add(Type.ADDITIONAL_ITEMS_ADDED);
    changes.add(Type.ADDITIONAL_ITEMS_EXTENDED);
    changes.add(Type.ITEM_WITH_EMPTY_SCHEMA_ADDED_TO_OPEN_CONTENT_MODEL);
    changes.add(Type.ITEM_ADDED_TO_CLOSED_CONTENT_MODEL);
    changes.add(Type.ITEM_WITH_FALSE_REMOVED_FROM_CLOSED_CONTENT_MODEL);
    changes.add(Type.ITEM_REMOVED_FROM_OPEN_CONTENT_MODEL);
    changes.add(Type.ITEM_ADDED_IS_COVERED_BY_PARTIALLY_OPEN_CONTENT_MODEL);
    changes.add(Type.ITEM_REMOVED_IS_COVERED_BY_PARTIALLY_OPEN_CONTENT_MODEL);

    changes.add(Type.ENUM_ARRAY_EXTENDED);

    changes.add(Type.COMBINED_TYPE_EXTENDED);
    changes.add(Type.PRODUCT_TYPE_NARROWED);
    changes.add(Type.SUM_TYPE_EXTENDED);
    changes.add(Type.NOT_TYPE_NARROWED);

    COMPATIBLE_CHANGES = Collections.unmodifiableSet(changes);
  }

  public static List<Difference> compare(final Schema original, final Schema update) {
    final Context ctx = new Context(COMPATIBLE_CHANGES);
    compare(ctx, original, update);
    return ctx.getDifferences();
  }

  @SuppressWarnings("ConstantConditions")
  static void compare(final Context ctx, Schema original, Schema update) {
    if (original == null && update == null) {
      return;
    } else if (original == null) {
      ctx.addDifference(Type.SCHEMA_ADDED);
      return;
    } else if (update == null) {
      ctx.addDifference(Type.SCHEMA_REMOVED);
      return;
    }

    original = normalizeSchema(original);
    update = normalizeSchema(update);

    if (!(original instanceof CombinedSchema) && update instanceof CombinedSchema) {
      CombinedSchema combinedSchema = (CombinedSchema) update;
      // Special case of singleton unions
      if (combinedSchema.getSubschemas().size() == 1) {
        final Context subctx = ctx.getSubcontext();
        compare(subctx, original, combinedSchema.getSubschemas().iterator().next());
        if (subctx.isCompatible()) {
          ctx.addDifferences(subctx.getDifferences());
          return;
        }
      } else if (combinedSchema.getCriterion() == CombinedSchema.ANY_CRITERION
          || combinedSchema.getCriterion() == CombinedSchema.ONE_CRITERION) {
        for (Schema subschema : combinedSchema.getSubschemas()) {
          final Context subctx = ctx.getSubcontext();
          compare(subctx, original, subschema);
          if (subctx.isCompatible()) {
            ctx.addDifferences(subctx.getDifferences());
            ctx.addDifference(Type.SUM_TYPE_EXTENDED);
            return;
          }
        }
      }
    } else if (original instanceof CombinedSchema && !(update instanceof CombinedSchema)) {
      // Special case of singleton unions
      CombinedSchema combinedSchema = (CombinedSchema) original;
      if (combinedSchema.getSubschemas().size() == 1) {
        final Context subctx = ctx.getSubcontext();
        compare(subctx, combinedSchema.getSubschemas().iterator().next(), update);
        if (subctx.isCompatible()) {
          ctx.addDifferences(subctx.getDifferences());
          return;
        }
      } else if (combinedSchema.getCriterion() == CombinedSchema.ALL_CRITERION) {
        for (Schema subschema : combinedSchema.getSubschemas()) {
          final Context subctx = ctx.getSubcontext();
          compare(subctx, subschema, update);
          if (subctx.isCompatible()) {
            ctx.addDifferences(subctx.getDifferences());
            ctx.addDifference(Type.PRODUCT_TYPE_NARROWED);
            return;
          }
        }
      }
    }

    if (!original.getClass().equals(update.getClass())) {
      // TrueSchema extends EmptySchema
      if (!(original instanceof FalseSchema) && !(update instanceof EmptySchema)) {
        ctx.addDifference(Type.TYPE_CHANGED);
      }
      return;
    }

    try (Context.SchemaScope schemaScope = ctx.enterSchema(original)) {
      if (schemaScope != null) {
        if (!Objects.equals(original.getId(), update.getId())) {
          ctx.addDifference(Type.ID_CHANGED);
        }
        if (!Objects.equals(original.getTitle(), update.getTitle())) {
          ctx.addDifference(Type.TITLE_CHANGED);
        }
        if (!Objects.equals(original.getDescription(), update.getDescription())) {
          ctx.addDifference(Type.DESCRIPTION_CHANGED);
        }
        if (!Objects.equals(original.getDefaultValue(), update.getDefaultValue())) {
          ctx.addDifference(Type.DEFAULT_CHANGED);
        }

        if (original instanceof StringSchema) {
          StringSchemaDiff.compare(ctx, (StringSchema) original, (StringSchema) update);
        } else if (original instanceof NumberSchema) {
          NumberSchemaDiff.compare(ctx, (NumberSchema) original, (NumberSchema) update);
        } else if (original instanceof EnumSchema) {
          EnumSchemaDiff.compare(ctx, (EnumSchema) original, (EnumSchema) update);
        } else if (original instanceof CombinedSchema) {
          CombinedSchemaDiff.compare(ctx, (CombinedSchema) original, (CombinedSchema) update);
        } else if (original instanceof NotSchema) {
          NotSchemaDiff.compare(ctx, (NotSchema) original, (NotSchema) update);
        } else if (original instanceof ObjectSchema) {
          ObjectSchemaDiff.compare(ctx, (ObjectSchema) original, (ObjectSchema) update);
        } else if (original instanceof ArraySchema) {
          ArraySchemaDiff.compare(ctx, (ArraySchema) original, (ArraySchema) update);
        }
      }
    }
  }

  private static Schema normalizeSchema(final Schema schema) {
    if (schema instanceof ReferenceSchema) {
      return ((ReferenceSchema) schema).getReferredSchema();
    } else {
      return schema;
    }
  }
}
