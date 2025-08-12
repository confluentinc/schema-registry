/*
 * Copyright 2022 Confluent Inc.
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

package io.confluent.kafka.schemaregistry.utils;

import io.confluent.kafka.schemaregistry.client.rest.entities.SchemaRegistryType;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Props {
  public static final String PROPERTY_SCHEMA_REGISTRY_TYPE = "schema.registry.metadata.type";
  private static final Logger log = LoggerFactory.getLogger(Props.class);

  public static SchemaRegistryType getSchemaRegistryType(Map<String, Object> props) {
    Object srType = props.getOrDefault(PROPERTY_SCHEMA_REGISTRY_TYPE, null);
    if (srType == null) {
      log.warn("Schema registry type not specified, defaulting to 'opensource'");
      return new SchemaRegistryType();
    } else if (srType instanceof String) {
      String srTypeString = ((String) srType).trim().toLowerCase();
      return new SchemaRegistryType(srTypeString);
    } else {
      log.error("Schema registry type not specified, defaulting to 'opensource'");
      throw new IllegalArgumentException("Invalid schema registry type: " + srType);
    }
  }
}
