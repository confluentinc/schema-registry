/*
 * Copyright 2014-2025 Confluent Inc.
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

import com.google.common.annotations.VisibleForTesting;
import org.apache.avro.SchemaParseException;

public class AvroUtils {

  /**
   * Convert a schema string into a schema object and a canonical schema string.
   *
   * @return A schema object and a canonical representation of the schema string. Return null if
   *     there is any parsing error.
   */
  @VisibleForTesting
  public static AvroSchema parseSchema(String schemaString) {
    try {
      return new AvroSchema(schemaString);
    } catch (SchemaParseException e) {
      return null;
    }
  }
}
