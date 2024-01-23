/*
 * Copyright 2018 Confluent Inc.
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

package io.confluent.kafka.schemaregistry;

import java.util.Collections;
import java.util.List;
import java.util.Objects;
import io.confluent.kafka.schemaregistry.client.rest.entities.SchemaReference;

/**
 * A parsed schema.
 *
 * <p>Implementations of this interface are instantiated by a corresponding
 * {@link io.confluent.kafka.schemaregistry.SchemaProvider}.
 */
public interface ParsedSchema {

  /**
   * Returns the schema type.
   *
   * @return the schema type
   */
  String schemaType();

  /**
   * Returns a name for the schema.
   *
   * @return the name, or null
   */
  String name();

  /**
   * Returns a canonical string representation of the schema.
   *
   * @return the canonical representation
   */
  String canonicalString();

  /**
   * Returns a formatted string according to a type-specific format.
   *
   * @return the formatted string
   */
  default String formattedString(String format) {
    if (format == null || format.trim().isEmpty()) {
      return canonicalString();
    }
    throw new IllegalArgumentException("Format not supported: " + format);
  }

  /**
   * Returns a list of schema references.
   *
   * @return the schema references
   */
  List<SchemaReference> references();

  /**
   * Returns a normalized copy of this schema.
   * Normalization generally ignores ordering when it is not significant.
   *
   * @return the normalized representation
   */
  default ParsedSchema normalize() {
    return this;
  }

  /**
   * Validates the schema and ensures all references are resolved properly.
   * Throws an exception if the schema is not valid.
   */
  default void validate() {
  }

  /**
   * Checks the backward compatibility between this schema and the specified schema.
   * <p/>
   * Custom providers may choose to modify this schema during this check,
   * to ensure that it is compatible with the specified schema.
   *
   * @param previousSchema previous schema
   * @return an empty list if this schema is backward compatible with the previous schema,
   *         otherwise the list of error messages
   */
  List<String> isBackwardCompatible(ParsedSchema previousSchema);

  /**
   * Checks the compatibility between this schema and the specified schemas.
   * <p/>
   * Custom providers may choose to modify this schema during this check,
   * to ensure that it is compatible with the specified schemas.
   *
   * @param level the compatibility level
   * @param previousSchemas full schema history in chronological order
   * @return an empty list if this schema is backward compatible with the previous schema, otherwise
   *         the list of error messages
   */
  default List<String> isCompatible(
      CompatibilityLevel level, List<? extends ParsedSchema> previousSchemas) {
    if (level != CompatibilityLevel.NONE) {
      for (ParsedSchema previousSchema : previousSchemas) {
        if (!schemaType().equals(previousSchema.schemaType())) {
          return Collections.singletonList("Incompatible because of different schema type");
        }
      }
    }
    return CompatibilityChecker.checker(level).isCompatible(this, previousSchemas);
  }

  /**
   * Returns the underlying raw representation of the schema.
   *
   * @return the raw schema
   */
  Object rawSchema();

  /**
   * Returns whether the underlying raw representations are equal.
   *
   * @return whether the underlying raw representations are equal
   */
  default boolean deepEquals(ParsedSchema schema) {
    return Objects.equals(rawSchema(), schema.rawSchema());
  }
}
