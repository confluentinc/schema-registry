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

import io.confluent.kafka.schemaregistry.client.rest.entities.Schema;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.confluent.kafka.schemaregistry.AbstractSchemaProvider;
import io.confluent.kafka.schemaregistry.ParsedSchema;

public class JsonSchemaProvider extends AbstractSchemaProvider {

  private static final Logger log = LoggerFactory.getLogger(JsonSchemaProvider.class);

  /**
   * Config key (within the {@code schema.providers.} provider config map) controlling whether
   * remote HTTP(S) schema references may be fetched. The typed value is injected by the schema
   * registry from {@code schema.providers.json.fetch.remote.schemas} (defaults to true).
   */
  public static final String FETCH_REMOTE_REFS = "fetchRemoteRefs";

  private boolean fetchRemoteRefs = true;

  @Override
  public void configure(Map<String, ?> configs) {
    super.configure(configs);
    Object value = configs.get(FETCH_REMOTE_REFS);
    if (value instanceof Boolean) {
      fetchRemoteRefs = (Boolean) value;
    } else if (value != null) {
      fetchRemoteRefs = Boolean.parseBoolean(value.toString());
    }
  }

  @Override
  public String schemaType() {
    return JsonSchema.TYPE;
  }

  @Override
  public ParsedSchema parseSchemaOrElseThrow(
      Schema schema, boolean validateAsNew, boolean normalize) {
    try {
      // Whether this is a brand-new schema registration, derived from the schema id
      // (negative/unset id == not yet registered).
      boolean schemaIsNew = schema.getId() == null || schema.getId() < 0;
      return new JsonSchema(
              schema.getSchema(),
              schema.getReferences(),
              resolveReferences(schema, validateAsNew),
              schema.getMetadata(),
              schema.getRuleSet(),
              null,
              fetchRemoteRefs,
              schemaIsNew
      );
    } catch (Exception e) {
      log.error("Could not parse JSON schema", e);
      throw new IllegalArgumentException("Invalid schema of type " + schema.getSchemaType()
          + ", details: " + e.getMessage(), e);
    }
  }
}
