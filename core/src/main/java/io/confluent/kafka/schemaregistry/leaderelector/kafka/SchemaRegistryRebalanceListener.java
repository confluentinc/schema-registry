/*
 * Copyright 2014-2020 Confluent Inc.
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

package io.confluent.kafka.schemaregistry.leaderelector.kafka;

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
