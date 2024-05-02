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
 *
 */

package io.confluent.kafka.schemaregistry.protobuf.diff;

import java.util.Objects;

public class Difference {
  public enum Type {
    PACKAGE_CHANGED, MESSAGE_ADDED, MESSAGE_REMOVED, MESSAGE_MOVED,
    ENUM_ADDED, ENUM_REMOVED, ENUM_CONST_ADDED, ENUM_CONST_CHANGED,
    ENUM_CONST_REMOVED, FIELD_ADDED, FIELD_REMOVED, FIELD_NAME_CHANGED, FIELD_KIND_CHANGED,
    FIELD_SCALAR_KIND_CHANGED, FIELD_NAMED_TYPE_CHANGED,
    FIELD_NUMERIC_LABEL_CHANGED, FIELD_STRING_OR_BYTES_LABEL_CHANGED,
    REQUIRED_FIELD_ADDED, REQUIRED_FIELD_REMOVED,
    ONEOF_ADDED, ONEOF_REMOVED,
    ONEOF_FIELD_ADDED, ONEOF_FIELD_REMOVED,
    MULTIPLE_FIELDS_MOVED_TO_ONEOF, FIELD_MOVED_TO_EXISTING_ONEOF
  }

  private final String fullPath;
  private final Type type;

  public Difference(final Type type, final String fullPath) {
    this.fullPath = fullPath;
    this.type = type;
  }

  public String getFullPath() {
    return fullPath;
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
    return Objects.equals(fullPath, that.fullPath) && type == that.type;
  }

  @Override
  public int hashCode() {
    return Objects.hash(fullPath, type);
  }

  private String error() {
    String errorDescription = "";
    switch (type) {
      case PACKAGE_CHANGED:
        errorDescription = "The package at '" + fullPath + "' in the %s schema does "
                             + "not match the package in the %s schema";
        break;
      case MESSAGE_REMOVED:
        errorDescription = "The %s schema is missing a MESSAGE type at path '"
                             + fullPath + "' in the %s schema";
        break;
      case FIELD_KIND_CHANGED:
        errorDescription = "The type of a field at path '" + fullPath + "' in the %s schema does "
                        + "not match the %s schema";
        break;
      case FIELD_SCALAR_KIND_CHANGED:
        errorDescription = "The kind of a SCALAR field at path '" + fullPath
                             + "' in the %s schema does not match its kind in the %s schema";
        break;
      case FIELD_NAMED_TYPE_CHANGED:
        errorDescription = "The type of a MESSAGE field at path '" + fullPath
                             + "' in the %s schema does not match its type in the %s schema";
        break;
      case FIELD_NUMERIC_LABEL_CHANGED:
        errorDescription = "The label for a NUMERIC field at path '" + fullPath
                             + "' in the %s schema does not match its label in the %s schema";
        break;
      case REQUIRED_FIELD_ADDED:
        errorDescription = "A required field  at path '" + fullPath + "' in the %s schema "
                             + "is missing in the %s schema";
        break;
      case REQUIRED_FIELD_REMOVED:
        errorDescription = "The %s schema is missing a required field at path: '" + fullPath
                             + "' in the %s schema";
        break;
      case ONEOF_FIELD_REMOVED:
        errorDescription = "The %s schema is missing a oneof field at path '" + fullPath
                             + "' in the %s schema";
        break;
      case MULTIPLE_FIELDS_MOVED_TO_ONEOF:
        errorDescription = "Multiple fields in the oneof at path '" + fullPath
                             + "' in the %s schema are outside a oneof in the %s schema";
        break;
      case FIELD_MOVED_TO_EXISTING_ONEOF:
        errorDescription = "A field in the oneof at path '" + fullPath
                             + "' in the %s schema is outside an existing oneof in the %s schema";
        break;
      default:
        errorDescription = "";
        break;
    }
    return errorDescription;
  }

  @Override
  public String toString() {
    return "{errorType:\"" + type + '"' + ", description:\"" + error() + "\"}";
  }
}
