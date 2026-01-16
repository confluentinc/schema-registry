/*
 * Copyright 2026 Confluent Inc.
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

package io.confluent.kafka.schemaregistry.storage;

public enum ExecutionEnvironment {
  ALL,
  CLIENT,
  GATEWAY,
  SERVER,
  NONE;

  public static ExecutionEnvironment fromEntity(
      io.confluent.kafka.schemaregistry.client.rest.entities.ExecutionEnvironment e) {
    switch (e) {
      case ALL:
        return ExecutionEnvironment.ALL;
      case CLIENT:
        return ExecutionEnvironment.CLIENT;
      case GATEWAY:
        return ExecutionEnvironment.GATEWAY;
      case SERVER:
        return ExecutionEnvironment.SERVER;
      case NONE:
        return ExecutionEnvironment.NONE;
      default:
        throw new IllegalArgumentException();
    }
  }

  public io.confluent.kafka.schemaregistry.client.rest.entities.ExecutionEnvironment toEntity() {
    switch (this) {
      case ALL:
        return io.confluent.kafka.schemaregistry.client.rest.entities.ExecutionEnvironment.ALL;
      case CLIENT:
        return io.confluent.kafka.schemaregistry.client.rest.entities.ExecutionEnvironment.CLIENT;
      case GATEWAY:
        return io.confluent.kafka.schemaregistry.client.rest.entities.ExecutionEnvironment.GATEWAY;
      case SERVER:
        return io.confluent.kafka.schemaregistry.client.rest.entities.ExecutionEnvironment.SERVER;
      case NONE:
        return io.confluent.kafka.schemaregistry.client.rest.entities.ExecutionEnvironment.NONE;
      default:
        throw new IllegalArgumentException();
    }
  }
}
