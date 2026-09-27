/*
 * Copyright 2026 Confluent Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.confluent.kafka.schemaregistry.type.logical.json;

import io.confluent.kafka.schemaregistry.type.logical.ValidationException;

import org.everit.json.schema.ArraySchema;
import org.everit.json.schema.BooleanSchema;
import org.everit.json.schema.CombinedSchema;
import org.everit.json.schema.ConditionalSchema;
import org.everit.json.schema.ConstSchema;
import org.everit.json.schema.EnumSchema;
import org.everit.json.schema.NotSchema;
import org.everit.json.schema.NullSchema;
import org.everit.json.schema.NumberSchema;
import org.everit.json.schema.ObjectSchema;
import org.everit.json.schema.ObjectSchema.Builder;
import org.everit.json.schema.ReferenceSchema;
import org.everit.json.schema.Schema;
import org.everit.json.schema.StringSchema;
import org.json.JSONObject;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.IdentityHashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.Set;

/**
 * Utility class for handling {@link CombinedSchema} allOf simplification.
 */
public class CombinedSchemaUtils {

  static final String NULL_ONLY_VALUES_MESSAGE =
      "JSON Schema enum/const must permit at least one non-null value: null alone has no "
          + "column type";

  public static Schema simplifyAllOfSchema(CombinedSchema combinedSchema) {
    ConstSchema constSchema = null;
    EnumSchema enumSchema = null;
    BooleanSchema booleanSchema = null;
    NumberSchema numberSchema = null;
    StringSchema stringSchema = null;
    CombinedSchema combinedSubschema = null;
    Map<String, Schema> properties = new LinkedHashMap<>();
    Map<String, Boolean> required = new HashMap<>();
    Collection<Schema> subschemas = combinedSchema.getSubschemas();
    for (Schema subSchema : subschemas) {
      if (subSchema instanceof ConstSchema) {
        constSchema = (ConstSchema) subSchema;
      } else if (subSchema instanceof EnumSchema) {
        enumSchema = (EnumSchema) subSchema;
      } else if (subSchema instanceof BooleanSchema) {
        booleanSchema = (BooleanSchema) subSchema;
      } else if (subSchema instanceof NumberSchema) {
        numberSchema = (NumberSchema) subSchema;
      } else if (subSchema instanceof StringSchema) {
        stringSchema = (StringSchema) subSchema;
      } else if (subSchema instanceof CombinedSchema) {
        combinedSubschema = (CombinedSchema) subSchema;
      } else if (subSchema instanceof ConditionalSchema || subSchema instanceof NotSchema) {
        throw new ValidationException(
            "JSON Schema if/then/else and `not` are not supported: a property declared only under "
                + "a condition has no column to be read into, so its values would be silently "
                + "dropped");
      }
      collectPropertySchemas(subSchema, properties, required,
          Collections.newSetFromMap(new IdentityHashMap<>()));
    }
    if (!properties.isEmpty()) {
      final Builder builder = ObjectSchema.builder();
      properties.forEach(builder::addPropertySchema);
      required.entrySet().stream()
          .filter(Entry::getValue)
          .forEach(e -> builder.addRequiredProperty(e.getKey()));
      return builder.build();
    }
    Schema stringTypedValues = simplifyStringTypedValues(combinedSchema);
    if (stringTypedValues != null) {
      return stringTypedValues;
    } else if (combinedSubschema != null) {
      return combinedSubschema;
    } else if (constSchema != null) {
      if (stringSchema != null) {
        return stringSchema;
      } else if (numberSchema != null) {
        return numberSchema;
      } else if (booleanSchema != null) {
        return booleanSchema;
      }
    } else if (enumSchema != null) {
      if (stringSchema != null) {
        return stringSchema;
      } else if (numberSchema != null) {
        return numberSchema;
      } else if (booleanSchema != null) {
        return booleanSchema;
      }
    } else if (stringSchema != null && stringSchema.getFormatValidator() != null) {
      if (numberSchema != null) {
        return numberSchema;
      }
    }
    if (subschemas.size() == 2) {
      Iterator<Schema> it = subschemas.iterator();
      Schema first = it.next();
      Schema second = it.next();
      Optional<IgnoredAdditionalPropertiesSchema> ignoredAdditionalPropertiesSchema =
          isExactlyOneSchemaOfTypeObject(first, second);
      if (ignoredAdditionalPropertiesSchema.isPresent()) {
        final IgnoredAdditionalPropertiesSchema schemaWithIgnoredAdditionalProperties =
            ignoredAdditionalPropertiesSchema.get();
        if (schemaWithIgnoredAdditionalProperties.isSuperfluousAdditionalProperties()) {
          return schemaWithIgnoredAdditionalProperties.schema;
        }
      }
    }
    throw new ValidationException(
        "Unsupported criterion " + combinedSchema.getCriterion() + " for " + combinedSchema);
  }

  /**
   * For an allOf of exactly one {@code const}/{@code enum} and a {@code "string"} or
   * {@code ["string", "null"]} type, returns an enum of the non-null values (nullable only when
   * the type is), so it reads like a bare enum; returns the bare type when it has a length limit,
   * which only VARCHAR(n)/CHAR(n) can carry. Returns null for any other allOf.
   */
  private static Schema simplifyStringTypedValues(CombinedSchema allOf) {
    Schema valueSchema = null;
    Schema typeSchema = null;
    for (Schema subSchema : allOf.getSubschemas()) {
      if ((subSchema instanceof ConstSchema || subSchema instanceof EnumSchema)
          && valueSchema == null) {
        valueSchema = subSchema;
      } else if (stringMemberOf(subSchema) != null && typeSchema == null) {
        typeSchema = subSchema;
      } else {
        return null;
      }
    }
    if (valueSchema == null || typeSchema == null) {
      return null;
    }
    StringSchema stringSchema = stringMemberOf(typeSchema);
    if (stringSchema.getMinLength() != null || stringSchema.getMaxLength() != null) {
      return typeSchema;
    }
    List<Object> values = valueSchema instanceof ConstSchema
        ? Collections.singletonList(((ConstSchema) valueSchema).getPermittedValue())
        : ((EnumSchema) valueSchema).getPossibleValuesAsList();
    // everit hangs title/description/unprocessed keywords off the synthetic allOf, not its
    // members. confluent:enum is positional, so each null value's entry is dropped with it.
    Map<String, Object> unprocessed = new LinkedHashMap<>(allOf.getUnprocessedProperties());
    Object rawMeta = unprocessed.get("confluent:enum");
    List<?> meta = rawMeta instanceof List ? (List<?>) rawMeta : null;
    List<Object> nonNullValues = new ArrayList<>();
    List<Object> nonNullMeta = new ArrayList<>();
    for (int i = 0; i < values.size(); i++) {
      if (JSONObject.NULL.equals(values.get(i))) {
        continue;
      }
      nonNullValues.add(values.get(i));
      if (meta != null && i < meta.size()) {
        nonNullMeta.add(meta.get(i));
      }
    }
    if (nonNullValues.isEmpty()) {
      throw new ValidationException(NULL_ONLY_VALUES_MESSAGE);
    }
    if (meta != null) {
      unprocessed.put("confluent:enum", nonNullMeta);
    }
    Schema enumSchema = EnumSchema.builder()
        .possibleValues(nonNullValues)
        .title(allOf.getTitle())
        .description(allOf.getDescription())
        .unprocessedProperties(unprocessed)
        .build();
    return typeSchema instanceof StringSchema
        ? enumSchema
        : CombinedSchema.anyOf(Arrays.asList(enumSchema, NullSchema.INSTANCE)).build();
  }

  // The string member of a `"string"` type or a `["string", "null"]` type list, else null.
  private static StringSchema stringMemberOf(Schema typeSchema) {
    if (typeSchema instanceof StringSchema) {
      return (StringSchema) typeSchema;
    }
    if (!(typeSchema instanceof CombinedSchema)
        || ((CombinedSchema) typeSchema).getCriterion() == CombinedSchema.ALL_CRITERION) {
      return null;
    }
    StringSchema stringSchema = null;
    boolean hasNull = false;
    for (Schema member : ((CombinedSchema) typeSchema).getSubschemas()) {
      if (member instanceof StringSchema && stringSchema == null) {
        stringSchema = (StringSchema) member;
      } else if (member instanceof NullSchema && !hasNull) {
        hasNull = true;
      } else {
        return null;
      }
    }
    return hasNull ? stringSchema : null;
  }

  private static Optional<IgnoredAdditionalPropertiesSchema> isExactlyOneSchemaOfTypeObject(
      Schema first, Schema second) {
    if (first instanceof ObjectSchema && !(second instanceof ObjectSchema)) {
      return Optional.of(new IgnoredAdditionalPropertiesSchema((ObjectSchema) first, second));
    } else if (!(first instanceof ObjectSchema) && second instanceof ObjectSchema) {
      return Optional.of(new IgnoredAdditionalPropertiesSchema((ObjectSchema) second, first));
    }
    return Optional.empty();
  }

  private static class IgnoredAdditionalPropertiesSchema {
    final ObjectSchema objectSchema;
    final Schema schema;

    IgnoredAdditionalPropertiesSchema(ObjectSchema objectSchema, Schema schema) {
      this.objectSchema = objectSchema;
      this.schema = schema;
    }

    private boolean isSuperfluousAdditionalProperties() {
      if (!objectSchema.requiresObject()) {
        return true;
      }
      return objectSchema.getRequiredProperties().isEmpty() && schema instanceof ArraySchema;
    }
  }

  private static void collectPropertySchemas(
      Schema schema,
      Map<String, Schema> properties,
      Map<String, Boolean> required,
      Set<Schema> visited) {
    // Identity-based cycle guard: a recursive $ref resolves back to the same
    // Schema instance, so this breaks the recursion without serializing the
    // subschema (add() returns false when already present).
    if (!visited.add(schema)) {
      return;
    }
    if (schema instanceof CombinedSchema) {
      CombinedSchema combinedSchema = (CombinedSchema) schema;
      if (combinedSchema.getCriterion() == CombinedSchema.ALL_CRITERION) {
        for (Schema subSchema : combinedSchema.getSubschemas()) {
          collectPropertySchemas(subSchema, properties, required, visited);
        }
      }
    } else if (schema instanceof ObjectSchema) {
      ObjectSchema objectSchema = (ObjectSchema) schema;
      for (Map.Entry<String, Schema> entry : objectSchema.getPropertySchemas().entrySet()) {
        String fieldName = entry.getKey();
        properties.put(fieldName, entry.getValue());
        required.put(fieldName, objectSchema.getRequiredProperties().contains(fieldName));
      }
    } else if (schema instanceof ReferenceSchema) {
      ReferenceSchema refSchema = (ReferenceSchema) schema;
      collectPropertySchemas(refSchema.getReferredSchema(), properties, required, visited);
    }
  }

  private CombinedSchemaUtils() {}
}
