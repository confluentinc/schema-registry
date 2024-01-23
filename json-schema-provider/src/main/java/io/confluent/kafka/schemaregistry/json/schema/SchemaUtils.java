/*
 * Copyright 2023 Confluent Inc.
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

package io.confluent.kafka.schemaregistry.json.schema;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.Map;
import java.util.Set;
import java.util.regex.Pattern;
import org.everit.json.schema.ArraySchema;
import org.everit.json.schema.BooleanSchema;
import org.everit.json.schema.CombinedSchema;
import org.everit.json.schema.ConditionalSchema;
import org.everit.json.schema.ConstSchema;
import org.everit.json.schema.ConstSchema.ConstSchemaBuilder;
import org.everit.json.schema.EmptySchema;
import org.everit.json.schema.EnumSchema;
import org.everit.json.schema.FalseSchema;
import org.everit.json.schema.NotSchema;
import org.everit.json.schema.NullSchema;
import org.everit.json.schema.NumberSchema;
import org.everit.json.schema.ObjectSchema;
import org.everit.json.schema.ReferenceSchema;
import org.everit.json.schema.Schema;
import org.everit.json.schema.StringSchema;
import org.everit.json.schema.TrueSchema;

public class SchemaUtils {

  private static volatile Method isSyntheticMethod;

  public static Schema.Builder<?> schemaToBuilder(Schema s) {
    // TrueSchema extends EmptySchema
    if (s instanceof TrueSchema) {
      return toBuilder((TrueSchema) s);
    }
    if (s instanceof ArraySchema) {
      return toBuilder((ArraySchema) s);
    }
    if (s instanceof BooleanSchema) {
      return toBuilder((BooleanSchema) s);
    }
    if (s instanceof CombinedSchema) {
      return toBuilder((CombinedSchema) s);
    }
    if (s instanceof ConditionalSchema) {
      return toBuilder((ConditionalSchema) s);
    }
    if (s instanceof ConstSchema) {
      return toBuilder((ConstSchema) s);
    }
    if (s instanceof EmptySchema) {
      return toBuilder((EmptySchema) s);
    }
    if (s instanceof EnumSchema) {
      return toBuilder((EnumSchema) s);
    }
    if (s instanceof FalseSchema) {
      return toBuilder((FalseSchema) s);
    }
    if (s instanceof NotSchema) {
      return toBuilder((NotSchema) s);
    }
    if (s instanceof NullSchema) {
      return toBuilder((NullSchema) s);
    }
    if (s instanceof NumberSchema) {
      return toBuilder((NumberSchema) s);
    }
    if (s instanceof ObjectSchema) {
      return toBuilder((ObjectSchema) s);
    }
    if (s instanceof ReferenceSchema) {
      return toBuilder((ReferenceSchema) s);
    }
    if (s instanceof StringSchema) {
      return toBuilder((StringSchema) s);
    }
    throw new IllegalArgumentException();
  }

  public static ArraySchema.Builder toBuilder(ArraySchema s) {
    return merge(ArraySchema.builder().requiresArray(false), s);
  }

  public static BooleanSchema.Builder toBuilder(BooleanSchema s) {
    return merge(BooleanSchema.builder(), s);
  }

  public static CombinedSchema.Builder toBuilder(CombinedSchema s) {
    return merge(CombinedSchema.builder(), s);
  }

  public static ConditionalSchema.Builder toBuilder(ConditionalSchema s) {
    return merge(ConditionalSchema.builder(), s);
  }

  public static ConstSchemaBuilder toBuilder(ConstSchema s) {
    return merge(ConstSchema.builder(), s);
  }

  public static EmptySchema.Builder toBuilder(EmptySchema s) {
    return merge(EmptySchema.builder(), s);
  }

  public static EnumSchema.Builder toBuilder(EnumSchema s) {
    return merge(EnumSchema.builder(), s);
  }

  public static FalseSchema.Builder toBuilder(FalseSchema s) {
    return merge(FalseSchema.builder(), s);
  }

  public static NotSchema.Builder toBuilder(NotSchema s) {
    return merge(NotSchema.builder(), s);
  }

  public static NullSchema.Builder toBuilder(NullSchema s) {
    return merge(NullSchema.builder(), s);
  }

  public static NumberSchema.Builder toBuilder(NumberSchema s) {
    return merge(NumberSchema.builder().requiresNumber(false), s);
  }

  public static ObjectSchema.Builder toBuilder(ObjectSchema s) {
    return merge(ObjectSchema.builder().requiresObject(false), s);
  }

  public static ReferenceSchema.Builder toBuilder(ReferenceSchema s) {
    return merge(ReferenceSchema.builder(), s);
  }

  public static StringSchema.Builder toBuilder(StringSchema s) {
    return merge(StringSchema.builder().requiresString(false), s);
  }

  public static TrueSchema.Builder toBuilder(TrueSchema s) {
    return merge(TrueSchema.builder(), s);
  }

  public static ArraySchema.Builder merge(ArraySchema.Builder builder, ArraySchema s) {
    copyGenericAttrs(builder, s);
    if (s.requiresArray()) {
      builder.requiresArray(true);
    }
    if (s.getMinItems() != null) {
      builder.minItems(s.getMinItems());
    }
    if (s.getMaxItems() != null) {
      builder.maxItems(s.getMaxItems());
    }
    if (s.needsUniqueItems()) {
      builder.uniqueItems(true);
    }
    if (s.getAllItemSchema() != null) {
      builder.allItemSchema(s.getAllItemSchema());
    }
    if (s.getItemSchemas() != null) {
      for (Schema i : s.getItemSchemas()) {
        builder.addItemSchema(i);
      }
    }
    if (!s.permitsAdditionalItems()) {
      builder.additionalItems(false);
    }
    if (s.getSchemaOfAdditionalItems() != null) {
      builder.schemaOfAdditionalItems(s.getSchemaOfAdditionalItems());
    }
    if (s.getContainedItemSchema() != null) {
      builder.containsItemSchema(s.getContainedItemSchema());
    }
    return builder;
  }

  public static BooleanSchema.Builder merge(BooleanSchema.Builder builder, BooleanSchema s) {
    copyGenericAttrs(builder, s);
    return builder;
  }

  public static CombinedSchema.Builder merge(CombinedSchema.Builder builder, CombinedSchema s) {
    copyGenericAttrs(builder, s);
    builder.isSynthetic(isSynthetic(s));
    builder.criterion(s.getCriterion());
    builder.subschemas(s.getSubschemas());
    return builder;
  }

  public static ConditionalSchema.Builder merge(
      ConditionalSchema.Builder builder, ConditionalSchema s) {
    copyGenericAttrs(builder, s);
    if (s.getIfSchema().isPresent()) {
      builder.ifSchema(s.getIfSchema().get());
    }
    if (s.getThenSchema().isPresent()) {
      builder.thenSchema(s.getThenSchema().get());
    }
    if (s.getElseSchema().isPresent()) {
      builder.elseSchema(s.getElseSchema().get());
    }
    return builder;
  }

  public static ConstSchemaBuilder merge(ConstSchemaBuilder builder, ConstSchema s) {
    copyGenericAttrs(builder, s);
    if (s.getPermittedValue() != null) {
      builder.permittedValue(s.getPermittedValue());
    }
    return builder;
  }

  public static EmptySchema.Builder merge(EmptySchema.Builder builder, EmptySchema s) {
    copyGenericAttrs(builder, s);
    return builder;
  }

  public static EnumSchema.Builder merge(EnumSchema.Builder builder, EnumSchema s) {
    copyGenericAttrs(builder, s);
    if (s.getPossibleValuesAsList() != null) {
      builder.possibleValues(s.getPossibleValuesAsList());
    }
    return builder;
  }

  public static FalseSchema.Builder merge(FalseSchema.Builder builder, FalseSchema s) {
    copyGenericAttrs(builder, s);
    return builder;
  }

  public static NotSchema.Builder merge(NotSchema.Builder builder, NotSchema s) {
    copyGenericAttrs(builder, s);
    if (s.getMustNotMatch() != null) {
      builder.mustNotMatch(s.getMustNotMatch());
    }
    return builder;
  }

  public static NullSchema.Builder merge(NullSchema.Builder builder, NullSchema s) {
    copyGenericAttrs(builder, s);
    return builder;
  }

  public static NumberSchema.Builder merge(NumberSchema.Builder builder, NumberSchema s) {
    copyGenericAttrs(builder, s);
    if (s.getMinimum() != null) {
      builder.minimum(s.getMinimum());
    }
    if (s.getMaximum() != null) {
      builder.maximum(s.getMaximum());
    }
    if (s.getExclusiveMinimumLimit() != null) {
      builder.exclusiveMinimum(s.getExclusiveMinimumLimit());
    }
    if (s.getExclusiveMaximumLimit() != null) {
      builder.exclusiveMaximum(s.getExclusiveMaximumLimit());
    }
    if (s.getMultipleOf() != null) {
      builder.multipleOf(s.getMultipleOf());
    }
    if (s.isRequiresNumber()) {
      builder.requiresNumber(true);
    }
    if (s.requiresInteger()) {
      builder.requiresInteger(true);
    }
    return builder;
  }

  public static ObjectSchema.Builder merge(ObjectSchema.Builder builder, ObjectSchema s) {
    copyGenericAttrs(builder, s);
    if (s.getPatternProperties() != null) {
      for (Map.Entry<Pattern, Schema> entry : s.getPatternProperties().entrySet()) {
        builder.patternProperty(entry.getKey(), entry.getValue());
      }
    }
    if (s.requiresObject()) {
      builder.requiresObject(true);
    }
    if (s.getPropertySchemas() != null) {
      for (Map.Entry<String, Schema> entry : s.getPropertySchemas().entrySet()) {
        builder.addPropertySchema(entry.getKey(), entry.getValue());
      }
    }
    if (!s.permitsAdditionalProperties()) {
      builder.additionalProperties(false);
    }
    if (s.getSchemaOfAdditionalProperties() != null) {
      builder.schemaOfAdditionalProperties(s.getSchemaOfAdditionalProperties());
    }
    if (s.getRequiredProperties() != null) {
      for (String p : s.getRequiredProperties()) {
        builder.addRequiredProperty(p);
      }
    }
    if (s.getMinProperties() != null) {
      builder.minProperties(s.getMinProperties());
    }
    if (s.getMaxProperties() != null) {
      builder.maxProperties(s.getMaxProperties());
    }
    if (s.getPropertyDependencies() != null) {
      for (Map.Entry<String, Set<String>> entry : s.getPropertyDependencies().entrySet()) {
        for (String p : entry.getValue()) {
          builder.propertyDependency(entry.getKey(), p);
        }
      }
    }
    if (s.getSchemaDependencies() != null) {
      for (Map.Entry<String, Schema> entry : s.getSchemaDependencies().entrySet()) {
        builder.schemaDependency(entry.getKey(), entry.getValue());
      }
    }
    if (s.getPropertyNameSchema() != null) {
      builder.propertyNameSchema(s.getPropertyNameSchema());
    }
    return builder;
  }

  public static ReferenceSchema.Builder merge(ReferenceSchema.Builder builder, ReferenceSchema s) {
    copyGenericAttrs(builder, s);
    if (s.getReferenceValue() != null) {
      builder.refValue(s.getReferenceValue());
    }
    return builder;
  }

  public static StringSchema.Builder merge(StringSchema.Builder builder, StringSchema s) {
    copyGenericAttrs(builder, s);
    if (s.getMinLength() != null) {
      builder.minLength(s.getMinLength());
    }
    if (s.getMaxLength() != null) {
      builder.maxLength(s.getMaxLength());
    }
    if (s.getPattern() != null) {
      builder.pattern(s.getPattern().pattern());
    }
    if (s.requireString()) {
      builder.requiresString(true);
    }
    if (s.getFormatValidator() != null) {
      builder.formatValidator(s.getFormatValidator());
    }
    return builder;
  }

  public static TrueSchema.Builder merge(TrueSchema.Builder builder, TrueSchema s) {
    copyGenericAttrs(builder, s);
    return builder;
  }

  private static void copyGenericAttrs(Schema.Builder<?> builder, Schema s) {
    if (s.getTitle() != null) {
      builder.title(s.getTitle());
    }
    if (s.getDescription() != null) {
      builder.description(s.getDescription());
    }
    if (s.getId() != null) {
      builder.id(s.getId());
    }
    if (s.getDefaultValue() != null) {
      builder.defaultValue(s.getDefaultValue());
    }
    if (s.isNullable() != null) {
      builder.nullable(s.isNullable());
    }
    if (s.isReadOnly() != null) {
      builder.readOnly(s.isReadOnly());
    }
    if (s.isWriteOnly() != null) {
      builder.writeOnly(s.isWriteOnly());
    }
    if (s.getUnprocessedProperties() != null) {
      builder.unprocessedProperties(s.getUnprocessedProperties());
    }
  }

  protected static boolean isSynthetic(CombinedSchema schema) {
    // We use reflection to access isSynthetic
    try {
      if (isSyntheticMethod == null) {
        synchronized (SchemaTranslator.class) {
          if (isSyntheticMethod == null) {
            isSyntheticMethod = CombinedSchema.class.getDeclaredMethod("isSynthetic");
          }
        }
        isSyntheticMethod.setAccessible(true);
      }
      return (Boolean) isSyntheticMethod.invoke(schema);
    } catch (NoSuchMethodException | IllegalAccessException | InvocationTargetException e) {
      throw new RuntimeException(e);
    }
  }
}
