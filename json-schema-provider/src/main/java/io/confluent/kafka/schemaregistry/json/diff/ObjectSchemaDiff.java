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

import java.util.Map;
import java.util.regex.Pattern;
import org.everit.json.schema.EmptySchema;
import org.everit.json.schema.FalseSchema;
import org.everit.json.schema.ObjectSchema;
import org.everit.json.schema.Schema;

import java.util.HashSet;
import java.util.Objects;
import java.util.Set;

import static io.confluent.kafka.schemaregistry.json.diff.Difference.Type.ADDITIONAL_PROPERTIES_ADDED;
import static io.confluent.kafka.schemaregistry.json.diff.Difference.Type.ADDITIONAL_PROPERTIES_EXTENDED;
import static io.confluent.kafka.schemaregistry.json.diff.Difference.Type.ADDITIONAL_PROPERTIES_NARROWED;
import static io.confluent.kafka.schemaregistry.json.diff.Difference.Type.ADDITIONAL_PROPERTIES_REMOVED;
import static io.confluent.kafka.schemaregistry.json.diff.Difference.Type.DEPENDENCY_ARRAY_ADDED;
import static io.confluent.kafka.schemaregistry.json.diff.Difference.Type.DEPENDENCY_ARRAY_CHANGED;
import static io.confluent.kafka.schemaregistry.json.diff.Difference.Type.DEPENDENCY_ARRAY_EXTENDED;
import static io.confluent.kafka.schemaregistry.json.diff.Difference.Type.DEPENDENCY_ARRAY_NARROWED;
import static io.confluent.kafka.schemaregistry.json.diff.Difference.Type.DEPENDENCY_ARRAY_REMOVED;
import static io.confluent.kafka.schemaregistry.json.diff.Difference.Type.DEPENDENCY_SCHEMA_ADDED;
import static io.confluent.kafka.schemaregistry.json.diff.Difference.Type.DEPENDENCY_SCHEMA_REMOVED;
import static io.confluent.kafka.schemaregistry.json.diff.Difference.Type.MAX_PROPERTIES_ADDED;
import static io.confluent.kafka.schemaregistry.json.diff.Difference.Type.MAX_PROPERTIES_DECREASED;
import static io.confluent.kafka.schemaregistry.json.diff.Difference.Type.MAX_PROPERTIES_INCREASED;
import static io.confluent.kafka.schemaregistry.json.diff.Difference.Type.MAX_PROPERTIES_REMOVED;
import static io.confluent.kafka.schemaregistry.json.diff.Difference.Type.MIN_PROPERTIES_ADDED;
import static io.confluent.kafka.schemaregistry.json.diff.Difference.Type.MIN_PROPERTIES_DECREASED;
import static io.confluent.kafka.schemaregistry.json.diff.Difference.Type.MIN_PROPERTIES_INCREASED;
import static io.confluent.kafka.schemaregistry.json.diff.Difference.Type.MIN_PROPERTIES_REMOVED;
import static io.confluent.kafka.schemaregistry.json.diff.Difference.Type.OPTIONAL_PROPERTY_ADDED_TO_UNOPEN_CONTENT_MODEL;
import static io.confluent.kafka.schemaregistry.json.diff.Difference.Type.PROPERTY_ADDED_IS_COVERED_BY_PARTIALLY_OPEN_CONTENT_MODEL;
import static io.confluent.kafka.schemaregistry.json.diff.Difference.Type.PROPERTY_ADDED_NOT_COVERED_BY_PARTIALLY_OPEN_CONTENT_MODEL;
import static io.confluent.kafka.schemaregistry.json.diff.Difference.Type.PROPERTY_ADDED_TO_OPEN_CONTENT_MODEL;
import static io.confluent.kafka.schemaregistry.json.diff.Difference.Type.PROPERTY_REMOVED_FROM_CLOSED_CONTENT_MODEL;
import static io.confluent.kafka.schemaregistry.json.diff.Difference.Type.PROPERTY_REMOVED_FROM_OPEN_CONTENT_MODEL;
import static io.confluent.kafka.schemaregistry.json.diff.Difference.Type.PROPERTY_REMOVED_IS_COVERED_BY_PARTIALLY_OPEN_CONTENT_MODEL;
import static io.confluent.kafka.schemaregistry.json.diff.Difference.Type.PROPERTY_REMOVED_NOT_COVERED_BY_PARTIALLY_OPEN_CONTENT_MODEL;
import static io.confluent.kafka.schemaregistry.json.diff.Difference.Type.PROPERTY_WITH_EMPTY_SCHEMA_ADDED_TO_OPEN_CONTENT_MODEL;
import static io.confluent.kafka.schemaregistry.json.diff.Difference.Type.PROPERTY_WITH_FALSE_REMOVED_FROM_CLOSED_CONTENT_MODEL;
import static io.confluent.kafka.schemaregistry.json.diff.Difference.Type.REQUIRED_ATTRIBUTE_ADDED;
import static io.confluent.kafka.schemaregistry.json.diff.Difference.Type.REQUIRED_ATTRIBUTE_REMOVED;
import static io.confluent.kafka.schemaregistry.json.diff.Difference.Type.REQUIRED_ATTRIBUTE_WITH_DEFAULT_ADDED;
import static io.confluent.kafka.schemaregistry.json.diff.Difference.Type.REQUIRED_PROPERTY_ADDED_TO_UNOPEN_CONTENT_MODEL;
import static io.confluent.kafka.schemaregistry.json.diff.Difference.Type.REQUIRED_PROPERTY_WITH_DEFAULT_ADDED_TO_UNOPEN_CONTENT_MODEL;

public class ObjectSchemaDiff {
  static void compare(final Context ctx, final ObjectSchema original, final ObjectSchema update) {
    compareRequired(ctx, original, update);
    compareProperties(ctx, original, update);
    compareDependencies(ctx, original, update);
    compareAdditionalProperties(ctx, original, update);
    compareAttributes(ctx, original, update);
  }

  private static void compareAttributes(
      final Context ctx, final ObjectSchema original, final ObjectSchema update
  ) {
    if (!Objects.equals(original.getMaxProperties(), update.getMaxProperties())) {
      if (original.getMaxProperties() == null && update.getMaxProperties() != null) {
        ctx.addDifference("maxProperties", MAX_PROPERTIES_ADDED);
      } else if (original.getMaxProperties() != null && update.getMaxProperties() == null) {
        ctx.addDifference("maxProperties", MAX_PROPERTIES_REMOVED);
      } else if (original.getMaxProperties() < update.getMaxProperties()) {
        ctx.addDifference("maxProperties", MAX_PROPERTIES_INCREASED);
      } else if (original.getMaxProperties() > update.getMaxProperties()) {
        ctx.addDifference("maxProperties", MAX_PROPERTIES_DECREASED);
      }
    }
    if (!Objects.equals(original.getMinProperties(), update.getMinProperties())) {
      if (original.getMinProperties() == null && update.getMinProperties() != null) {
        ctx.addDifference("minProperties", MIN_PROPERTIES_ADDED);
      } else if (original.getMinProperties() != null && update.getMinProperties() == null) {
        ctx.addDifference("minProperties", MIN_PROPERTIES_REMOVED);
      } else if (original.getMinProperties() < update.getMinProperties()) {
        ctx.addDifference("minProperties", MIN_PROPERTIES_INCREASED);
      } else if (original.getMinProperties() > update.getMinProperties()) {
        ctx.addDifference("minProperties", MIN_PROPERTIES_DECREASED);
      }
    }
  }

  private static void compareAdditionalProperties(
      final Context ctx, final ObjectSchema original, final ObjectSchema update
  ) {
    try (Context.PathScope pathScope = ctx.enterPath("additionalProperties")) {
      if (original.permitsAdditionalProperties() != update.permitsAdditionalProperties()) {
        if (update.permitsAdditionalProperties()) {
          ctx.addDifference(ADDITIONAL_PROPERTIES_ADDED);
        } else {
          ctx.addDifference(ADDITIONAL_PROPERTIES_REMOVED);
        }
      } else if (original.getSchemaOfAdditionalProperties() == null
          && update.getSchemaOfAdditionalProperties() != null) {
        ctx.addDifference(ADDITIONAL_PROPERTIES_NARROWED);
      } else if (update.getSchemaOfAdditionalProperties() == null
          && original.getSchemaOfAdditionalProperties() != null) {
        ctx.addDifference(ADDITIONAL_PROPERTIES_EXTENDED);
      } else {
        SchemaDiff.compare(
            ctx,
            original.getSchemaOfAdditionalProperties(),
            update.getSchemaOfAdditionalProperties()
        );
      }
    }
  }

  private static void compareDependencies(
      final Context ctx, final ObjectSchema original, final ObjectSchema update
  ) {
    try (Context.PathScope pathScope = ctx.enterPath("dependencies")) {
      Set<String> propertyKeys = new HashSet<>(original.getPropertyDependencies().keySet());
      propertyKeys.addAll(update.getPropertyDependencies().keySet());

      for (String propertyKey : propertyKeys) {
        try (Context.PathScope pathScope2 = ctx.enterPath(propertyKey)) {
          Set<String> originalDependencies = original.getPropertyDependencies().get(propertyKey);
          Set<String> updateDependencies = update.getPropertyDependencies().get(propertyKey);
          if (updateDependencies == null) {
            ctx.addDifference(DEPENDENCY_ARRAY_REMOVED);
          } else if (originalDependencies == null) {
            ctx.addDifference(DEPENDENCY_ARRAY_ADDED);
          } else {
            if (!originalDependencies.equals(updateDependencies)) {
              if (updateDependencies.containsAll(originalDependencies)) {
                ctx.addDifference(DEPENDENCY_ARRAY_EXTENDED);
              } else if (originalDependencies.containsAll(updateDependencies)) {
                ctx.addDifference(DEPENDENCY_ARRAY_NARROWED);
              } else {
                ctx.addDifference(DEPENDENCY_ARRAY_CHANGED);
              }
            }
          }
        }
      }

      propertyKeys = new HashSet<>(original.getSchemaDependencies().keySet());
      propertyKeys.addAll(update.getSchemaDependencies().keySet());

      for (String propertyKey : propertyKeys) {
        try (Context.PathScope pathScope2 = ctx.enterPath(propertyKey)) {
          Schema originalSchema = original.getSchemaDependencies().get(propertyKey);
          Schema updateSchema = update.getSchemaDependencies().get(propertyKey);
          if (updateSchema == null) {
            ctx.addDifference(DEPENDENCY_SCHEMA_REMOVED);
          } else if (originalSchema == null) {
            ctx.addDifference(DEPENDENCY_SCHEMA_ADDED);
          } else {
            SchemaDiff.compare(ctx, originalSchema, updateSchema);
          }
        }
      }
    }
  }

  private static void compareProperties(
      final Context ctx, final ObjectSchema original, final ObjectSchema update
  ) {
    try (Context.PathScope pathScope = ctx.enterPath("properties")) {
      Set<String> propertyKeys = new HashSet<>(original.getPropertySchemas().keySet());
      propertyKeys.addAll(update.getPropertySchemas().keySet());

      for (String propertyKey : propertyKeys) {
        try (Context.PathScope pathScope2 = ctx.enterPath(propertyKey)) {
          Schema originalSchema = original.getPropertySchemas().get(propertyKey);
          Schema updateSchema = update.getPropertySchemas().get(propertyKey);
          if (updateSchema == null) {
            // We only consider the content model of the update
            // since we can only remove properties if the update is an open content model
            if (isOpenContentModel(update)) {
              // compatible
              ctx.addDifference(PROPERTY_REMOVED_FROM_OPEN_CONTENT_MODEL);
            } else {
              Schema schemaFromPartial = schemaFromPartiallyOpenContentModel(update, propertyKey);
              if (schemaFromPartial != null) {
                final Context subctx = ctx.getSubcontext();
                SchemaDiff.compare(subctx, originalSchema, schemaFromPartial);
                ctx.addDifferences(subctx.getDifferences());
                if (subctx.isCompatible()) {
                  // compatible
                  ctx.addDifference(PROPERTY_REMOVED_IS_COVERED_BY_PARTIALLY_OPEN_CONTENT_MODEL);
                } else {
                  // incompatible
                  ctx.addDifference(PROPERTY_REMOVED_NOT_COVERED_BY_PARTIALLY_OPEN_CONTENT_MODEL);
                }
              } else {
                if (originalSchema instanceof FalseSchema) {
                  // compatible
                  ctx.addDifference(PROPERTY_WITH_FALSE_REMOVED_FROM_CLOSED_CONTENT_MODEL);
                } else {
                  // incompatible
                  ctx.addDifference(PROPERTY_REMOVED_FROM_CLOSED_CONTENT_MODEL);
                }
              }
            }
          } else if (originalSchema == null) {
            // We only consider the content model of the original
            // since we can only add properties if the original is a closed content model
            if (isOpenContentModel(original)) {
              if (updateSchema instanceof EmptySchema) {
                // compatible
                ctx.addDifference(PROPERTY_WITH_EMPTY_SCHEMA_ADDED_TO_OPEN_CONTENT_MODEL);
              } else {
                // incompatible
                ctx.addDifference(PROPERTY_ADDED_TO_OPEN_CONTENT_MODEL);
              }
            } else {
              Schema schemaFromPartial = schemaFromPartiallyOpenContentModel(original, propertyKey);
              if (schemaFromPartial != null) {
                final Context subctx = ctx.getSubcontext();
                SchemaDiff.compare(subctx, schemaFromPartial, updateSchema);
                ctx.addDifferences(subctx.getDifferences());
                if (subctx.isCompatible()) {
                  // compatible
                  ctx.addDifference(PROPERTY_ADDED_IS_COVERED_BY_PARTIALLY_OPEN_CONTENT_MODEL);
                } else {
                  // incompatible
                  ctx.addDifference(PROPERTY_ADDED_NOT_COVERED_BY_PARTIALLY_OPEN_CONTENT_MODEL);
                }
              }
              // The following checks are for both closed and partially open content models.
              if (update.getRequiredProperties().contains(propertyKey)) {
                if (update.getPropertySchemas().get(propertyKey).hasDefaultValue()) {
                  // compatible
                  ctx.addDifference(REQUIRED_PROPERTY_WITH_DEFAULT_ADDED_TO_UNOPEN_CONTENT_MODEL);
                } else {
                  // incompatible
                  ctx.addDifference(REQUIRED_PROPERTY_ADDED_TO_UNOPEN_CONTENT_MODEL);
                }
              } else {
                // compatible
                ctx.addDifference(OPTIONAL_PROPERTY_ADDED_TO_UNOPEN_CONTENT_MODEL);
              }
            }
          } else {
            SchemaDiff.compare(ctx, originalSchema, updateSchema);
          }
        }
      }
    }
  }

  private static void compareRequired(
      final Context ctx, final ObjectSchema original, final ObjectSchema update
  ) {
    try (Context.PathScope pathScope = ctx.enterPath("required")) {
      for (String propertyKey : original.getPropertySchemas().keySet()) {
        if (update.getPropertySchemas().containsKey(propertyKey)) {
          try (Context.PathScope pathScope2 = ctx.enterPath(propertyKey)) {
            boolean originalRequired = original.getRequiredProperties().contains(propertyKey);
            boolean updateRequired = update.getRequiredProperties().contains(propertyKey);
            if (originalRequired && !updateRequired) {
              ctx.addDifference(REQUIRED_ATTRIBUTE_REMOVED);
            } else if (!originalRequired && updateRequired) {
              if (update.getPropertySchemas().get(propertyKey).hasDefaultValue()) {
                ctx.addDifference(REQUIRED_ATTRIBUTE_WITH_DEFAULT_ADDED);
              } else {
                ctx.addDifference(REQUIRED_ATTRIBUTE_ADDED);
              }
            }
          }
        }
      }
    }
  }

  private static boolean isOpenContentModel(final ObjectSchema schema) {
    return schema.getPatternProperties().size() == 0
        && schema.getSchemaOfAdditionalProperties() == null
        && schema.permitsAdditionalProperties();
  }

  private static Schema schemaFromPartiallyOpenContentModel(
      final ObjectSchema schema, final String propertyKey) {
    for (Map.Entry<Pattern, Schema> entry : schema.getPatternProperties().entrySet()) {
      Pattern pattern = entry.getKey();
      if (pattern.matcher(propertyKey).find()) {
        return entry.getValue();
      }
    }
    return schema.getSchemaOfAdditionalProperties();
  }
}
