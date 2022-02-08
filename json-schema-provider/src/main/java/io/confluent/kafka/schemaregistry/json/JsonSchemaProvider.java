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

package io.confluent.kafka.schemaregistry.json;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

import io.confluent.kafka.schemaregistry.AbstractSchemaProvider;
import io.confluent.kafka.schemaregistry.ParsedSchema;
import io.confluent.kafka.schemaregistry.client.rest.entities.SchemaReference;

public class JsonSchemaProvider extends AbstractSchemaProvider {

  private static final Logger log = LoggerFactory.getLogger(JsonSchemaProvider.class);

  @Override
  public String schemaType() {
    return JsonSchema.TYPE;
  }

  @Override
  public ParsedSchema parseSchemaOrElseThrow(String schemaString,
                                             List<SchemaReference> references,
                                             boolean isNew) {
    try {
      return new JsonSchema(
              schemaString,
              references,
              resolveReferences(references),
              null
      );
    } catch (Exception e) {
      log.error("Could not parse JSON schema", e);
      throw e;
    }
  }
}
