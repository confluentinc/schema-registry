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
import org.everit.json.schema.CombinedSchema;
import org.everit.json.schema.ConstSchema;
import org.everit.json.schema.EnumSchema;
import org.everit.json.schema.NumberSchema;
import org.everit.json.schema.ObjectSchema;
import org.everit.json.schema.ObjectSchema.Builder;
import org.everit.json.schema.ReferenceSchema;
import org.everit.json.schema.Schema;
import org.everit.json.schema.StringSchema;

import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.Set;

/**
 * Utility class for handling {@link CombinedSchema} allOf simplification.
 */
public class CombinedSchemaUtils {

  public static Schema simplifyAllOfSchema(CombinedSchema combinedSchema) {
    ConstSchema constSchema = null;
    EnumSchema enumSchema = null;
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
      } else if (subSchema instanceof NumberSchema) {
        numberSchema = (NumberSchema) subSchema;
      } else if (subSchema instanceof StringSchema) {
        stringSchema = (StringSchema) subSchema;
      } else if (subSchema instanceof CombinedSchema) {
        combinedSubschema = (CombinedSchema) subSchema;
      }
      collectPropertySchemas(subSchema, properties, required, new HashSet<>());
    }
    if (!properties.isEmpty()) {
      final Builder builder = ObjectSchema.builder();
      properties.forEach(builder::addPropertySchema);
      required.entrySet().stream()
          .filter(Entry::getValue)
          .forEach(e -> builder.addRequiredProperty(e.getKey()));
      return builder.build();
    } else if (combinedSubschema != null) {
      return combinedSubschema;
    } else if (constSchema != null) {
      if (stringSchema != null) {
        return stringSchema;
      } else if (numberSchema != null) {
        return numberSchema;
      }
    } else if (enumSchema != null) {
      if (stringSchema != null) {
        return stringSchema;
      } else if (numberSchema != null) {
        return numberSchema;
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
      Set<String> visited) {
    if (visited.contains(schema.toString())) {
      return;
    } else {
      visited.add(schema.toString());
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
