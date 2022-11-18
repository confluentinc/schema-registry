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
    errorDescription = new HashMap<>();
    errorDescription.put(SchemaIncompatibilityType.FIXED_SIZE_MISMATCH,
        String.format("The size of a FIXED type field has changed"));
    errorDescription.put(SchemaIncompatibilityType.TYPE_MISMATCH,
        String.format("The type of a field has changed"));
    errorDescription.put(SchemaIncompatibilityType.NAME_MISMATCH,
        String.format("The name of the schema does not match a previous version of the schema"));
    errorDescription.put(SchemaIncompatibilityType.MISSING_ENUM_SYMBOLS,
        String.format("The reader schema has missing enum symbols"));
    errorDescription.put(SchemaIncompatibilityType.MISSING_UNION_BRANCH,
        String.format("A type inside a union field is missing in the reader schema"));
    errorDescription.put(SchemaIncompatibilityType.READER_FIELD_MISSING_DEFAULT_VALUE,
        String.format("The reader schema has an additional field without a default value"));
  }

  public String errorMessage() {
    SchemaIncompatibilityType errorType = incompatibility.getType();
    return  "jsonPath='" + incompatibility.getLocation() + '\''
              + ", errorType='" + errorType.toString() + '\''
              + ", errorMsg='" + errorDescription.getOrDefault(errorType, "") + '\''
              + ", additionalInfo='" + incompatibility.getMessage() + "'";
  }

  public String toString() {
    return "{" + errorMessage() + "}";
  }

  public String toStringVerbose() {
    return "{" + errorMessage()
             + ", readerSchema:'" + incompatibility.getReaderFragment() + '\''
             + "writerSchema:'" + incompatibility.getWriterFragment() + "'}";
  }
}
