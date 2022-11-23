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

import com.google.common.collect.ImmutableMap;
import java.util.Map;
import java.util.Objects;

public class Difference {
  public enum Type {
    ID_CHANGED, DESCRIPTION_CHANGED, TITLE_CHANGED, DEFAULT_CHANGED,
    SCHEMA_ADDED, SCHEMA_REMOVED, TYPE_EXTENDED, TYPE_NARROWED, TYPE_CHANGED,

    MAX_LENGTH_ADDED, MAX_LENGTH_REMOVED, MAX_LENGTH_INCREASED, MAX_LENGTH_DECREASED,
    MIN_LENGTH_ADDED, MIN_LENGTH_REMOVED, MIN_LENGTH_INCREASED, MIN_LENGTH_DECREASED,
    PATTERN_ADDED, PATTERN_REMOVED, PATTERN_CHANGED,

    MAXIMUM_ADDED, MAXIMUM_REMOVED, MAXIMUM_INCREASED, MAXIMUM_DECREASED, MINIMUM_ADDED,
    MINIMUM_REMOVED, MINIMUM_INCREASED, MINIMUM_DECREASED, EXCLUSIVE_MAXIMUM_ADDED,
    EXCLUSIVE_MAXIMUM_REMOVED, EXCLUSIVE_MAXIMUM_INCREASED, EXCLUSIVE_MAXIMUM_DECREASED,
    EXCLUSIVE_MINIMUM_ADDED, EXCLUSIVE_MINIMUM_REMOVED, EXCLUSIVE_MINIMUM_INCREASED,
    EXCLUSIVE_MINIMUM_DECREASED, MULTIPLE_OF_ADDED, MULTIPLE_OF_REMOVED, MULTIPLE_OF_EXPANDED,
    MULTIPLE_OF_REDUCED, MULTIPLE_OF_CHANGED,

    REQUIRED_ATTRIBUTE_ADDED, REQUIRED_ATTRIBUTE_WITH_DEFAULT_ADDED, REQUIRED_ATTRIBUTE_REMOVED,
    MAX_PROPERTIES_ADDED, MAX_PROPERTIES_REMOVED, MAX_PROPERTIES_INCREASED,
    MAX_PROPERTIES_DECREASED, MIN_PROPERTIES_ADDED, MIN_PROPERTIES_REMOVED,
    MIN_PROPERTIES_INCREASED, MIN_PROPERTIES_DECREASED, ADDITIONAL_PROPERTIES_ADDED,
    ADDITIONAL_PROPERTIES_REMOVED, ADDITIONAL_PROPERTIES_EXTENDED, ADDITIONAL_PROPERTIES_NARROWED,
    DEPENDENCY_ARRAY_ADDED, DEPENDENCY_ARRAY_REMOVED, DEPENDENCY_ARRAY_EXTENDED,
    DEPENDENCY_ARRAY_NARROWED, DEPENDENCY_ARRAY_CHANGED, DEPENDENCY_SCHEMA_ADDED,
    DEPENDENCY_SCHEMA_REMOVED, PROPERTY_ADDED_TO_OPEN_CONTENT_MODEL,
    PROPERTY_WITH_EMPTY_SCHEMA_ADDED_TO_OPEN_CONTENT_MODEL,
    REQUIRED_PROPERTY_ADDED_TO_UNOPEN_CONTENT_MODEL,
    REQUIRED_PROPERTY_WITH_DEFAULT_ADDED_TO_UNOPEN_CONTENT_MODEL,
    OPTIONAL_PROPERTY_ADDED_TO_UNOPEN_CONTENT_MODEL, PROPERTY_REMOVED_FROM_OPEN_CONTENT_MODEL,
    PROPERTY_WITH_FALSE_REMOVED_FROM_CLOSED_CONTENT_MODEL,
    PROPERTY_REMOVED_FROM_CLOSED_CONTENT_MODEL,
    PROPERTY_REMOVED_IS_COVERED_BY_PARTIALLY_OPEN_CONTENT_MODEL,
    PROPERTY_REMOVED_NOT_COVERED_BY_PARTIALLY_OPEN_CONTENT_MODEL,
    PROPERTY_ADDED_IS_COVERED_BY_PARTIALLY_OPEN_CONTENT_MODEL,
    PROPERTY_ADDED_NOT_COVERED_BY_PARTIALLY_OPEN_CONTENT_MODEL,

    MAX_ITEMS_ADDED, MAX_ITEMS_REMOVED, MAX_ITEMS_INCREASED, MAX_ITEMS_DECREASED, MIN_ITEMS_ADDED,
    MIN_ITEMS_REMOVED, MIN_ITEMS_INCREASED, MIN_ITEMS_DECREASED, UNIQUE_ITEMS_ADDED,
    UNIQUE_ITEMS_REMOVED, ADDITIONAL_ITEMS_ADDED, ADDITIONAL_ITEMS_REMOVED,
    ADDITIONAL_ITEMS_EXTENDED, ADDITIONAL_ITEMS_NARROWED, ITEM_ADDED_TO_OPEN_CONTENT_MODEL,
    ITEM_WITH_EMPTY_SCHEMA_ADDED_TO_OPEN_CONTENT_MODEL,
    ITEM_ADDED_TO_CLOSED_CONTENT_MODEL, ITEM_REMOVED_FROM_OPEN_CONTENT_MODEL,
    ITEM_WITH_FALSE_REMOVED_FROM_CLOSED_CONTENT_MODEL,
    ITEM_REMOVED_FROM_CLOSED_CONTENT_MODEL,
    ITEM_REMOVED_IS_COVERED_BY_PARTIALLY_OPEN_CONTENT_MODEL,
    ITEM_REMOVED_NOT_COVERED_BY_PARTIALLY_OPEN_CONTENT_MODEL,
    ITEM_ADDED_IS_COVERED_BY_PARTIALLY_OPEN_CONTENT_MODEL,
    ITEM_ADDED_NOT_COVERED_BY_PARTIALLY_OPEN_CONTENT_MODEL,

    ENUM_ARRAY_EXTENDED, ENUM_ARRAY_NARROWED, ENUM_ARRAY_CHANGED,

    COMBINED_TYPE_EXTENDED, COMBINED_TYPE_CHANGED,
    PRODUCT_TYPE_EXTENDED, PRODUCT_TYPE_NARROWED, SUM_TYPE_EXTENDED,
    SUM_TYPE_NARROWED, COMBINED_TYPE_SUBSCHEMAS_CHANGED,
    NOT_TYPE_EXTENDED, NOT_TYPE_NARROWED

  }

  private final String jsonPath;
  private final Type type;
  private final Map<Type, String> errorDescription = ImmutableMap.<Type, String>builder()
      .put(Type.SCHEMA_ADDED, "A new reader schema (path: '%s') was added")
      .put(Type.TYPE_NARROWED, "The list of types at path: '%s' in the reader schema was narrowed")
      .put(Type.TYPE_CHANGED, "The type of a field at path '%s' in the reader schema has changed")
      .put(Type.MAX_LENGTH_ADDED,
        "The 'maxLength' keyword (path: '%s') was added to the reader schema")
      .put(Type.MAX_LENGTH_DECREASED,
        "The value of 'maxLength' at path: '%s' was decreased in the reader schema")
      .put(Type.MIN_LENGTH_ADDED,
        "The 'minLength' keyword (path: '%s') was added to the reader schema")
      .put(Type.MIN_LENGTH_INCREASED,
        "The value of 'minLength' at path: '%s' was increased in the reader schema")
      .put(Type.PATTERN_ADDED,
        "The 'pattern' keyword (path: '%s') was added to the reader schema")
      .put(Type.PATTERN_CHANGED,
        "The value of 'pattern' at path: '%s' was changed in the reader schema")
      .put(Type.MAXIMUM_ADDED,
        "The 'maximum' keyword (path: '%s') was added to the reader schema")
      .put(Type.MAXIMUM_DECREASED,
        "The value of 'maximum' at path: '%s' was decreased in the reader schema")
      .put(Type.MINIMUM_ADDED,
        "The 'minimum' keyword (path: '%s') was added to the reader schema")
      .put(Type.MINIMUM_INCREASED,
        "The value of 'minimum' at path: '%s' was increased in the reader schema")
      .put(Type.EXCLUSIVE_MAXIMUM_ADDED,
        "The 'exclusiveMaximum' keyword (path: '%s') was added to the reader schema")
      .put(Type.EXCLUSIVE_MAXIMUM_DECREASED,
        "The value of 'exclusiveMaximum' at path: '%s' was decreased in the reader schema")
      .put(Type.EXCLUSIVE_MINIMUM_ADDED,
        "The 'exclusiveMinimum' keyword (path: '%s') was added to the reader schema")
      .put(Type.EXCLUSIVE_MINIMUM_INCREASED,
        "The value of 'exclusiveMinimum' at path: '%s' was increased in the reader schema")
      .put(Type.MULTIPLE_OF_ADDED,
        "The 'multipleOf' keyword was added at path: '%s' in the reader schema")
      .put(Type.MULTIPLE_OF_EXPANDED,
        "The value of 'multipleOf' at path: '%s' in the reader schema was expanded")
      .put(Type.MULTIPLE_OF_CHANGED,
        "The value of 'multipleOf' at path: '%s' in the reader schema was changed")
      .put(Type.REQUIRED_ATTRIBUTE_ADDED,
        "The 'required' keyword was added to an object type at path: '%s' in the reader schema")
      .put(Type.MAX_PROPERTIES_ADDED,
        "The 'maxProperties' keyword was added to object type path: '%s' in the reader schema")
      .put(Type.MAX_PROPERTIES_DECREASED,
        "The value of 'maxProperties' at path: '%s' was decreased in the "
          + "reader schema")
      .put(Type.MIN_PROPERTIES_ADDED,
        "The 'minProperties' keyword was added to an object type at path: '%s' in the "
          + "reader schema")
      .put(Type.MIN_PROPERTIES_INCREASED,
        "The value of 'maxProperties' at path: '%s' was increased in the reader schema")
      .put(Type.ADDITIONAL_PROPERTIES_REMOVED,
        "The 'additionalProperties' keyword at path '%s' was removed from the reader schema")
      .put(Type.ADDITIONAL_PROPERTIES_NARROWED,
        "The value of 'additionalProperties' at path '%s' was narrowed in the reader schema")
      .put(Type.DEPENDENCY_ARRAY_ADDED,
        "The 'dependentRequired' array was added at path '%s' in the reader schema")
      .put(Type.DEPENDENCY_ARRAY_EXTENDED,
        "The 'dependentRequired' array at path '%s' was extended in the reader schema")
      .put(Type.DEPENDENCY_ARRAY_CHANGED,
        "The 'dependentRequired' array at path '%s' was changed in the reader schema")
      .put(Type.DEPENDENCY_SCHEMA_ADDED,
        "The 'dependentSchemas' keyword was added at path '%s' in the reader schema")
      .put(Type.PROPERTY_ADDED_TO_OPEN_CONTENT_MODEL,
        "A property (path '%s') was added  to the reader schema which has an open content model ")
      .put(Type.REQUIRED_PROPERTY_ADDED_TO_UNOPEN_CONTENT_MODEL,
        "A required property (path '%s') was added to the reader schema which has an "
          + "unopen content model ")
      .put(Type.PROPERTY_REMOVED_FROM_CLOSED_CONTENT_MODEL,
        "A property (path '%s') was removed from the reader schema which has a closed content "
          + "model")
      .put(Type.PROPERTY_REMOVED_NOT_COVERED_BY_PARTIALLY_OPEN_CONTENT_MODEL,
      "A property (path '%s') was removed at  from the reader schema and is not covered by "
        + "its partially open content model")
      .put(Type.PROPERTY_ADDED_NOT_COVERED_BY_PARTIALLY_OPEN_CONTENT_MODEL,
        "A property (path '%s') that as added to the reader schema is not covered by its partially "
          + "open content model")
      .put(Type.MAX_ITEMS_ADDED,
        "The 'maxItems' keyword (path '%s') was added to the reader schema")
      .put(Type.MAX_ITEMS_DECREASED,
        "The value of 'maxItems' at path '%s' was decreased in the reader schema")
      .put(Type.MIN_ITEMS_ADDED,
        "The 'minItems' keyword (path '%s') was added in the reader schema")
      .put(Type.MIN_ITEMS_INCREASED,
        "The value of 'minItems' of an array at path '%s' was increased in the reader schema")
      .put(Type.UNIQUE_ITEMS_ADDED,
        "The 'uniqueItems' keyword was added to an array at path '%s' in the reader schema")
      .put(Type.ADDITIONAL_ITEMS_REMOVED,
        "The 'additionalItems' keyword at path '%s' was removed from the reader schema")
      .put(Type.ADDITIONAL_ITEMS_NARROWED,
        "The list of 'additionalItems' at path '%s' was narrowed in the reader schema")
      .put(Type.ITEM_ADDED_TO_OPEN_CONTENT_MODEL,
        "An item at path '%s' was added to the reader schema which has an open content model")
      .put(Type.ITEM_REMOVED_FROM_CLOSED_CONTENT_MODEL,
        "An item at path '%s' was removed from the reader schema which"
          + "has a closed content model")
      .put(Type.ITEM_REMOVED_NOT_COVERED_BY_PARTIALLY_OPEN_CONTENT_MODEL,
        "An item was removed at path '%s' from the reader schema and is not covered by its "
          + "partially closed content model")
      .put(Type.ITEM_ADDED_NOT_COVERED_BY_PARTIALLY_OPEN_CONTENT_MODEL,
        "An item was added at path '%s' to the reader schema and is not covered by its partially"
          + "open content model")
      .put(Type.ENUM_ARRAY_NARROWED,
        "The enum array at path '%s' in the reader schema was narrowed")
      .put(Type.ENUM_ARRAY_CHANGED, "The enum array at path '%s' in the reader schema was changed")
      .put(Type.COMBINED_TYPE_CHANGED,
        "A combined type at path '%s' in the reader schema was changed")
      .put(Type.PRODUCT_TYPE_EXTENDED,
        "A product type (allOf) at path '%s' in the reader schema was changed")
      .put(Type.SUM_TYPE_NARROWED,
        "A sum type (anyOf) at path '%s' in the reader schema was changed")
      .put(Type.COMBINED_TYPE_SUBSCHEMAS_CHANGED,
        "Subschemas of a combined type at path '%s' in the reader schema changed")
      .put(Type.NOT_TYPE_EXTENDED, "A 'NOT' type at path '%s' in the reader schema was extended")
      .build();

  public Difference(final Type type, final String jsonPath) {
    this.jsonPath = jsonPath;
    this.type = type;
  }

  public String getJsonPath() {
    return jsonPath;
  }

  public Type getType() {
    return type;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    Difference that = (Difference) o;
    return Objects.equals(jsonPath, that.jsonPath) && type == that.type;
  }

  @Override
  public int hashCode() {
    return Objects.hash(jsonPath, type);
  }

  @Override
  public String toString() {
    return "{ errorType:\"" + type + "\""
             + ", errorMessage:\""
             + String.format(errorDescription.getOrDefault(type, "%s"), jsonPath) + "'}";
  }
}
