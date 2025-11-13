/*
 * Copyright 2025 Confluent Inc.
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

package io.confluent.kafka.schemaregistry;

import java.util.Properties;

/**
 * Interface defining common operations needed by schema registry integration tests.
 * This allows tests to be written against non-Kafka-based implementations.
 */
public interface SchemaRegistryTestHarness {
  
  /**
   * Gets the broker list for Kafka connections.
   * @return broker list string, or null if not applicable
   */
  String getBrokerList();
  
  /**
   * Gets the REST application instance for making schema registry API calls.
   * @return RestApp instance
   */
  RestApp getRestApp();
  
  /**
   * Gets the port on which the Schema Registry is listening.
   * @return schema registry port number
   */
  Integer getSchemaRegistryPort();
  
  /**
   * Chooses an available port for schema registry or other services.
   * @return available port number
   */
  int choosePort();

  /**
   * Gets the schema registry protocol (http or https).
   * @return protocol string
   */
  String getSchemaRegistryProtocol();
  
  /**
   * Inject properties into broker configuration.
   * @param props properties to inject
   */
  void injectProperties(Properties props);
}

