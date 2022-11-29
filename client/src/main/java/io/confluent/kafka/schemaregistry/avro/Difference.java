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

import java.util.HashMap;
import java.util.Map;
import org.apache.avro.SchemaCompatibility;
import org.apache.avro.SchemaCompatibility.Incompatibility;
import org.apache.avro.SchemaCompatibility.SchemaIncompatibilityType;

public class Difference {
  private final Incompatibility incompatibility;
  private final Map<SchemaIncompatibilityType, String> errorDescription;

  public Difference(final SchemaCompatibility.Incompatibility incompatibility) {
    this.incompatibility = incompatibility;
    String path = incompatibility.getLocation();

    errorDescription = new HashMap<>();
    errorDescription.put(SchemaIncompatibilityType.FIXED_SIZE_MISMATCH,
        String.format("The size of FIXED type field at path: '%s' in the reader schema has "
                        + "changed", path));
    errorDescription.put(SchemaIncompatibilityType.TYPE_MISMATCH,
        String.format("The type (path: '%s') of a field in the reader schema has changed. ",
          path));
    errorDescription.put(SchemaIncompatibilityType.NAME_MISMATCH,
        String.format("The name of the schema has changed (path: '%s').", path));
    errorDescription.put(SchemaIncompatibilityType.MISSING_ENUM_SYMBOLS,
        String.format("Enum symbols '%s' at path: '%s' in the writer schema are missing in the "
                        + "reader schema", incompatibility.getMessage(), path));
    errorDescription.put(SchemaIncompatibilityType.MISSING_UNION_BRANCH,
        String.format("A type inside a union field at path: '%s' in the writer schema is "
                        + "missing in the reader schema", path));
    errorDescription.put(SchemaIncompatibilityType.READER_FIELD_MISSING_DEFAULT_VALUE,
        String.format("The reader schema has an additional field '%s' at path: '%s' without a "
                        + "default value", incompatibility.getMessage(), path));
  }

  public String error() {
    SchemaIncompatibilityType errorType = incompatibility.getType();
    return "errorType:'" + errorType.toString() + '\''
             + ", description:'" + errorDescription.getOrDefault(errorType, "") + '\''
             + ", additionalInfo:'" + incompatibility.getMessage() + "'";
  }

  public String toString() {
    return "{" + error() + "}";
  }

  public String toStringVerbose() {
    return "{" + error()
             + ", readerFragment:'" + incompatibility.getReaderFragment() + '\''
             + ", writerFragment:'" + incompatibility.getWriterFragment() + "'}";
  }
}
