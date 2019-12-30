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

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import net.thisptr.jackson.jq.BuiltinFunctionLoader;
import net.thisptr.jackson.jq.JsonQuery;
import net.thisptr.jackson.jq.Scope;
import net.thisptr.jackson.jq.Versions;
import org.apache.avro.AvroTypeException;
import org.apache.avro.Schema;
import org.apache.avro.SchemaParseException;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class AvroUtils {

  private static final ObjectMapper MAPPER = new ObjectMapper();

  /**
   * Convert a schema string into a schema object and a canonical schema string.
   *
   * @return A schema object and a canonical representation of the schema string. Return null if
   *     there is any parsing error.
   */
  public static AvroSchema parseSchema(String schemaString) {
    Schema schema;
    try {
      try {
        Schema.Parser parser1 = new Schema.Parser();
        schema = parser1.parse(schemaString);
      } catch (AvroTypeException e) {
        // Must clear the parser
        Schema.Parser parser1 = new Schema.Parser();
        // In case of AvroTypeException,, attempt to replace quoted null with null for defaults
        schema = parser1.parse(replaceQuotedNullWithNullForDefaults(schemaString));
      }
    } catch (SchemaParseException e) {
      return null;
    }
    return new AvroSchema(schema, schema.toString());
  }

  private static String replaceQuotedNullWithNullForDefaults(String schemaString) {
    try {
      Scope rootScope = Scope.newEmptyScope();
      // Load built-in jq functions such as "select"
      BuiltinFunctionLoader.getInstance().loadFunctions(Versions.JQ_1_5, rootScope);
      Scope childScope = Scope.newChildScope(rootScope);

      // This jq expression recursively replaces all default properties that have
      // a quoted null with a plain null
      JsonQuery jq = JsonQuery.compile(
          "(.. | .default? | select(. == \"null\")) = null", Versions.JQ_1_5);

      JsonNode in = MAPPER.readTree(schemaString);
      final List<JsonNode> out = new ArrayList<>();
      // Perform the transformation using jq
      jq.apply(childScope, in, out::add);
      if (out.isEmpty()) {
        throw new SchemaParseException("Invalid schema after jq processing");
      }
      return MAPPER.writeValueAsString(out.get(0));
    } catch (IOException ex) {
      throw new SchemaParseException(ex);
    }
  }
}
