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

package io.confluent.kafka.schemaregistry.avro;

import org.apache.avro.SchemaCompatibility;
import org.apache.avro.SchemaCompatibility.Incompatibility;
import org.apache.avro.SchemaCompatibility.SchemaIncompatibilityType;

public class Difference {
  private final Incompatibility incompatibility;

  private final Type type;

  private final String path;

  public enum Type {
    RESERVED_FIELD_REMOVED, FIELD_CONFLICTS_WITH_RESERVED_FIELD
  }

  public Difference(final SchemaCompatibility.Incompatibility incompatibility) {
    this.incompatibility = incompatibility;
    this.type = null;
    this.path = null;
  }

  public Difference(final Type type, final String path) {
    this.incompatibility = null;
    this.type = type;
    this.path = path;
  }

  @SuppressWarnings("CyclomaticComplexity")
  public String error() {
    String errorDescription;
    if (type != null && path != null) {
      switch (type) {
        case RESERVED_FIELD_REMOVED:
          errorDescription = "The %s schema has reserved field " + path + " removed from its "
                  + "metadata which is present in the %s schema's metadata.";
          break;
        case FIELD_CONFLICTS_WITH_RESERVED_FIELD:
          errorDescription = "The %s schema has field that conflicts with the reserved field "
                  + path + " which is missing in the %s schema.";
          break;
        default:
          errorDescription = "";
          break;
      }
      return "{errorType:'" + type + '\''
              + ", description:'" + errorDescription + "'}";
    }
    SchemaIncompatibilityType errorType = incompatibility.getType();
    String path = incompatibility.getLocation();

    switch (errorType) {
      case FIXED_SIZE_MISMATCH:
        errorDescription = "The size of FIXED type field at path '" + path + "' in the %s "
                                           + "schema does not match with the %s schema";
        break;
      case TYPE_MISMATCH:
        errorDescription = "The type (path '" + path + "') of a field in the %s schema does "
                                           + "not match with the %s schema";
        break;
      case NAME_MISMATCH:
        errorDescription = "The name of the schema has changed (path '" + path + "')";
        break;
      case MISSING_ENUM_SYMBOLS:
        errorDescription =
          "The %s schema is missing enum symbols '" + incompatibility.getMessage() + "' at path '"
            + path + "' in the %s schema";
        break;
      case MISSING_UNION_BRANCH:
        errorDescription = "The %s schema is missing a type inside a union field at path '"
                             + path + "' in the %s schema";
        break;
      case READER_FIELD_MISSING_DEFAULT_VALUE:
        errorDescription =
          "The field '" + incompatibility.getMessage() + "' at path '"
             + path + "' in the %s schema has no default value and is missing in the %s schema";
        break;
      default:
        errorDescription = "";
    }

    return "{errorType:'" + errorType + '\''
             + ", description:'" + errorDescription + '\''
             + ", additionalInfo:'" + incompatibility.getMessage() + "'}";
  }

  public String toString() {
    return error();
  }

}
