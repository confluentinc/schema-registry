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

import java.util.Objects;
import java.util.Set;
import java.util.HashSet;
import java.util.Arrays;

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
    RESERVED_PROPERTY_REMOVED, RESERVED_PROPERTY_CONFLICTS_WITH_PROPERTY,

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
  Set<Type> keywordAddedOrRemoved = new HashSet<>(Arrays.asList(Type.MAXIMUM_ADDED,
      Type.MINIMUM_ADDED,
      Type.EXCLUSIVE_MAXIMUM_ADDED, Type.EXCLUSIVE_MINIMUM_ADDED, Type.MULTIPLE_OF_ADDED,
      Type.MAX_LENGTH_ADDED, Type.MIN_LENGTH_ADDED, Type.PATTERN_ADDED,
      Type.REQUIRED_ATTRIBUTE_ADDED, Type.MAX_PROPERTIES_ADDED, Type.MIN_PROPERTIES_ADDED,
      Type.DEPENDENCY_ARRAY_ADDED, Type.DEPENDENCY_SCHEMA_ADDED, Type.MAX_ITEMS_ADDED,
      Type.MIN_ITEMS_ADDED, Type.UNIQUE_ITEMS_ADDED, Type.ADDITIONAL_ITEMS_REMOVED,
      Type.ADDITIONAL_PROPERTIES_REMOVED));
  Set<Type> valueIncreased = new HashSet<>(Arrays.asList(Type.MIN_LENGTH_INCREASED,
      Type.MINIMUM_INCREASED, Type.EXCLUSIVE_MINIMUM_INCREASED, Type.MIN_PROPERTIES_INCREASED,
      Type.MULTIPLE_OF_EXPANDED, Type.MIN_ITEMS_INCREASED));
  Set<Type> valueDecreased = new HashSet<>(Arrays.asList(Type.MAX_LENGTH_DECREASED,
      Type.MAXIMUM_DECREASED, Type.MAX_ITEMS_DECREASED,
      Type.EXCLUSIVE_MAXIMUM_DECREASED, Type.MAX_PROPERTIES_DECREASED));
  Set<Type> typeNarrowed = new HashSet<>(Arrays.asList(
      Type.ADDITIONAL_ITEMS_NARROWED, Type.ENUM_ARRAY_NARROWED, Type.SUM_TYPE_NARROWED,
      Type.ADDITIONAL_PROPERTIES_NARROWED));
  Set<Type> valueChanged = new HashSet<>(Arrays.asList(Type.PATTERN_CHANGED,
      Type.MULTIPLE_OF_CHANGED, Type.DEPENDENCY_ARRAY_CHANGED));
  Set<Type> typeChanged = new HashSet<>(Arrays.asList(Type.TYPE_CHANGED, Type.TYPE_NARROWED,
      Type.COMBINED_TYPE_CHANGED, Type.COMBINED_TYPE_SUBSCHEMAS_CHANGED, Type.ENUM_ARRAY_CHANGED));
  Set<Type> typeExtended = new HashSet<>(Arrays.asList(Type.DEPENDENCY_ARRAY_EXTENDED,
      Type.PRODUCT_TYPE_EXTENDED, Type.SUM_TYPE_EXTENDED, Type.NOT_TYPE_EXTENDED));

  private String error() {
    String message = "";
    if (keywordAddedOrRemoved.contains(type)) {
      message = "The keyword at path '" + jsonPath + "' in the %s schema is not present in "
                  + "the %s schema";
    } else if (valueIncreased.contains(type)) {
      message = "The value at path '" + jsonPath + "' in the %s schema is "
                  + "more than its value in the %s schema";
    } else if (valueDecreased.contains(type)) {
      message =  "The value at path '" + jsonPath + "' in the %s schema is "
                   + "less than its value in the %s schema";
    } else if (valueChanged.contains(type)) {
      message = "The value at path '" + jsonPath + "' is different between the "
                  + "%s and %s schema";
    } else if (typeNarrowed.contains(type)) {
      message = "An array or combined type at path '" + jsonPath + "' has fewer elements in the "
                  + "%s schema than the %s schema";
    } else if (typeExtended.contains(type)) {
      message = "An array or combined type at path '" + jsonPath + "' has more elements in the "
                  + "%s schema than the %s schema";
    } else if (typeChanged.contains(type)) {
      message = "A type at path '" + jsonPath + "' is different between the "
                  + "%s schema and the %s schema";
    } else {
      message = propertyOrItemError();
    }
    return message;
  }

  @SuppressWarnings("CyclomaticComplexity")
  private String propertyOrItemError() {
    if (type == Type.PROPERTY_ADDED_TO_OPEN_CONTENT_MODEL
          || type == Type.ITEM_ADDED_TO_OPEN_CONTENT_MODEL) {
      return "The %s schema has an open content model and has a property or item at "
               + "path '" + jsonPath + "' which is missing in the %s schema";
    } else if (type == Type.REQUIRED_PROPERTY_ADDED_TO_UNOPEN_CONTENT_MODEL) {
      return "The %s schema has an unopen content model and has a required property "
               + "at path '" + jsonPath + "' which is missing in the %s schema";
    } else if (type == Type.PROPERTY_REMOVED_FROM_CLOSED_CONTENT_MODEL
          || type == Type.ITEM_REMOVED_FROM_CLOSED_CONTENT_MODEL) {
      return "The %s has a closed content model and is missing a property or item present at "
               + "path '" + jsonPath + "' in the %s schema";
    } else if (type == Type.PROPERTY_REMOVED_NOT_COVERED_BY_PARTIALLY_OPEN_CONTENT_MODEL
          || type == Type.ITEM_REMOVED_NOT_COVERED_BY_PARTIALLY_OPEN_CONTENT_MODEL) {
      return "A property or item is missing in the %s schema but present at path '"
               + jsonPath + "' in the %s schema and is not covered by its partially "
               + "open content model";
    } else if (type == Type.PROPERTY_ADDED_NOT_COVERED_BY_PARTIALLY_OPEN_CONTENT_MODEL
          || type == Type.ITEM_ADDED_NOT_COVERED_BY_PARTIALLY_OPEN_CONTENT_MODEL) {
      return "The %s schema has a property or item at path '" + jsonPath + "' which is "
               + "missing in the %s schema and is not covered by its partially open content model";
    } else if (type == Type.RESERVED_PROPERTY_REMOVED) {
      return "The %s schema has reserved property '" + jsonPath + "' removed from its metadata "
              + "which is present in the %s schema.";
    } else if (type == Type.RESERVED_PROPERTY_CONFLICTS_WITH_PROPERTY) {
      return "The %s schema has property at path '" + jsonPath + "' that conflicts with the "
              + "reserved properties which is missing in the %s schema.";
    }
    return "";
  }

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
    return "{errorType:\"" + type + "\"" + ", description:\"" + error() + "'}";
  }
}
