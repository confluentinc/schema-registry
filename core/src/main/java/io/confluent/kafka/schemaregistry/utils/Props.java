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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Props {
  public static final String PROPERTY_SCHEMA_REGISTRY_TYPE_ATTRIBUTES = "schema.registry.metadata.type.attributes";
  private static final Logger log = LoggerFactory.getLogger(Props.class);

  public static SchemaRegistryType getSchemaRegistryType(Map<String, Object> props) {
    Object srType = props.getOrDefault(PROPERTY_SCHEMA_REGISTRY_TYPE_ATTRIBUTES,
        Arrays.asList(SchemaRegistryType.DEFAULT_ATTRIBUTE));
    if (srType == null) {
      log.warn("Schema registry type not specified, defaulting to 'opensource'");
      return new SchemaRegistryType();
    } else if (srType instanceof List) {
      List<?> srTypeList = (List<?>) srType;
      // Validate and process each element
      List<String> processedList = new ArrayList<>();
      for (Object item : srTypeList) {
        if (item instanceof String) {
          String type = (String) item;
          if (type != null) {
            processedList.add(type.trim().toLowerCase());
          }
        } else if (item != null) {
          throw new IllegalArgumentException("Invalid schema registry type attribute: " + item +
              ". Expected List<String> but got " + item.getClass().getSimpleName());
        }
      }
      return new SchemaRegistryType(processedList);
    } else {
      log.error("Schema registry type unexpected, defaulting to 'opensource'");
      throw new IllegalArgumentException("Invalid schema registry type: " + srType);
    }
  }
}
