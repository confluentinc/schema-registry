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

package io.confluent.kafka.schemaregistry.utils;

import io.confluent.kafka.schemaregistry.client.rest.entities.SchemaRegistryDeployment;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Props {
  public static final String PROPERTY_SCHEMA_REGISTRY_DEPLOYMENT_ATTRIBUTES
      = "schema.registry.metadata.deployment.attributes";
  private static final Logger log = LoggerFactory.getLogger(Props.class);

  public static SchemaRegistryDeployment getSchemaRegistryDeployment(Map<String, Object> props) {
    Object srDeployment = props.getOrDefault(PROPERTY_SCHEMA_REGISTRY_DEPLOYMENT_ATTRIBUTES,
        null);
    if (srDeployment == null) {
      return null;
    } else if (srDeployment instanceof List) {
      List<?> srDeploymentList = (List<?>) srDeployment;
      // Validate and process each element
      List<String> processedList = new ArrayList<>(srDeploymentList.size());
      srDeploymentList.stream().map(
          item -> item.toString().trim().toLowerCase()
      ).forEach(processedList::add);
      return new SchemaRegistryDeployment(processedList);
    } else {
      log.error("Schema registry deployment unexpected, defaulting to 'opensource'");
      throw new IllegalArgumentException("Invalid schema registry deployment: " + srDeployment);
    }
  }
}
