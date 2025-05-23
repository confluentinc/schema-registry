/*
 * Copyright 2025 Confluent Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.confluent.kafka.schemaregistry.builtin;

import io.confluent.kafka.schemaregistry.AbstractSchemaProvider;
import io.confluent.kafka.schemaregistry.ParsedSchema;
import io.confluent.kafka.schemaregistry.client.rest.entities.Schema;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class NativeSchemaProvider extends AbstractSchemaProvider {

  private static final Logger log = LoggerFactory.getLogger(NativeSchemaProvider.class);

  @Override
  public void configure(Map<String, ?> configs) {
    super.configure(configs);
  }

  @Override
  public String schemaType() {
    return NativeSchema.TYPE;
  }

  @Override
  public ParsedSchema parseSchemaOrElseThrow(Schema schema, boolean isNew, boolean normalize) {
    try {
      return new NativeSchema(
          schema.getSchema(),
          schema.getReferences(),
          resolveReferences(schema),
          schema.getMetadata(),
          schema.getRuleSet(),
          null
      );
    } catch (Exception e) {
      log.error("Could not parse native schema", e);
      throw e;
    }
  }
}
