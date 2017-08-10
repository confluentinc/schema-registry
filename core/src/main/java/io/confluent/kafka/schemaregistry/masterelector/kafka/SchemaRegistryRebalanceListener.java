/**
 * Copyright 2017 Confluent Inc.
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
 **/

package io.confluent.kafka.schemaregistry.masterelector.kafka;

/**
 * Listener for rebalance events in the Kafka group.
 */
interface SchemaRegistryRebalanceListener {
  /**
   * Invoked when a new assignment is created by joining the schema registry group. This is
   * invoked for both successful and unsuccessful assignments.
   */
  void onAssigned(SchemaRegistryProtocol.Assignment assignment, int generation);

  /**
   * Invoked when a rebalance operation starts, revoking leadership
   */
  void onRevoked();
}
