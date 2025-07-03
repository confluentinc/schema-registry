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

package io.confluent.connect.json;

import org.apache.kafka.connect.data.Schema;
import org.junit.Test;

import io.confluent.kafka.schemaregistry.json.JsonSchema;

import static org.junit.Assert.assertEquals;

public class AdditionalJsonSchemaDataTest {

  private JsonSchemaData jsonSchemaData = new JsonSchemaData();

  private static String optionalStringWithTitleForNullSchema = "{\n"
      + "  \"oneOf\": [\n"
      + "    { \n"
      + "      \"title\": \"Not included\",\n"
      + "      \"type\": \"null\" \n"
      + "    },\n"
      + "    { \"type\": \"string\" }\n"
      + "  ]\n"
      + "}";

  public AdditionalJsonSchemaDataTest() {
  }

  @Test
  public void testToConnectOptionalStringWithTitleForNullSchema() throws Exception {
    org.everit.json.schema.Schema schema =
        new JsonSchema(optionalStringWithTitleForNullSchema).rawSchema();
    Schema connectSchema = jsonSchemaData.toConnectSchema(schema);
    Schema expectedSchema = Schema.OPTIONAL_STRING_SCHEMA;
    assertEquals(expectedSchema, connectSchema);
  }
}
