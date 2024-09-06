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

  public Difference(final SchemaCompatibility.Incompatibility incompatibility) {
    this.incompatibility = incompatibility;
  }

  @SuppressWarnings("CyclomaticComplexity")
  public String error() {
    String errorDescription;
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
