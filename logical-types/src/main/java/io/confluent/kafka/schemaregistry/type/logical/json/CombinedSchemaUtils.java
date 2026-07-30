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
import org.everit.json.schema.ConditionalSchema;
import org.everit.json.schema.ConstSchema;
import org.everit.json.schema.EnumSchema;
import org.everit.json.schema.NotSchema;
import org.everit.json.schema.NumberSchema;
import org.everit.json.schema.ObjectSchema;
import org.everit.json.schema.ObjectSchema.Builder;
import org.everit.json.schema.ReferenceSchema;
import org.everit.json.schema.Schema;
import org.everit.json.schema.StringSchema;

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.IdentityHashMap;
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
      } else if (subSchema instanceof ConditionalSchema || subSchema instanceof NotSchema) {
        // Rejected rather than dropped. A JSON Schema writing `if`/`then`/`else` alongside its
        // properties is parsed by everit as an allOf of [ObjectSchema, ConditionalSchema], and the
        // branches of the conditional declare properties that the ObjectSchema does not. Falling
        // through this chain would discard the ConditionalSchema and return a struct built from the
        // ObjectSchema alone -- so a conditionally-required property gets no column at all, and
        // every record carrying it silently loses that value.
        //
        // `not` is in scope for a reason that reverses an earlier decision. On its own it costs no
        // column, only a constraint. But JSON Schema has no implication operator -- `if S then T
        // else E` is written `(not S or T) and (S or E)` -- so every encoding of conditional
        // semantics needs a negation, and rejecting only ConditionalSchema blocks just the sugar.
        //
        // COVERAGE IS PARTIAL, deliberately. This chain inspects only the *immediate* subschemas of
        // the allOf, so nesting the negation one level deeper still converts and still loses the
        // column. Naming more subschema types will not fix that: the root cause is that this method
        // discards any subschema it cannot merge, which is also why an `anyOf` of object shapes
        // nested in an allOf loses its properties. Closing it means refusing to drop a subschema,
        // which reaches ordinary allOf composition and carries far more blast radius.
        //
        // Also note this rejects schemas that previously converted -- unlike the ConstSchema and
        // tuple rejections, which turned away input that was never convertible.
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
