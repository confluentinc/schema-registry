/*
 * Copyright 2018 Confluent Inc.
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

import org.apache.avro.Schema;
import org.apache.avro.SchemaParseException;

public class AvroUtils {

  /**
   * Convert a schema string into a schema object and a canonical schema string.
   *
   * @return A schema object and a canonical representation of the schema string. Return null if
   *     there is any parsing error.
   */
  public static AvroSchema parseSchema(String schemaString) {
    try {
      Schema.Parser parser1 = new Schema.Parser();
      parser1.setValidateDefaults(false);
      Schema schema = parser1.parse(schemaString);
      //TODO: schema.toString() is not canonical (issue-28)
      return new AvroSchema(schema, schema.toString());
    } catch (SchemaParseException e) {
      return null;
    }
  }
}
