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

import java.util.List;
import java.util.Optional;

import io.confluent.kafka.schemaregistry.AbstractSchemaProvider;
import io.confluent.kafka.schemaregistry.ParsedSchema;
import io.confluent.kafka.schemaregistry.client.rest.entities.SchemaReference;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class AvroSchemaProvider extends AbstractSchemaProvider {

  private static final Logger log = LoggerFactory.getLogger(AvroSchemaProvider.class);

  @Override
  public String schemaType() {
    return AvroSchema.TYPE;
  }

  @Override
  public Optional<ParsedSchema> parseSchema(String schemaString,
                                            List<SchemaReference> references) {
    try {
      return Optional.of(
          new AvroSchema(schemaString, references, resolveReferences(references), null));
    } catch (Exception e) {
      log.warn("Could not parse schema", e);
      return Optional.empty();
    }
  }
}
