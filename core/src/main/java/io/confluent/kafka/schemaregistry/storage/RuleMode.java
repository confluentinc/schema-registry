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

package io.confluent.kafka.schemaregistry.storage;

public enum RuleMode {
  UPGRADE,
  DOWNGRADE,
  UPDOWN,
  WRITE,
  READ,
  WRITEREAD;

  public static RuleMode fromEntity(
      io.confluent.kafka.schemaregistry.client.rest.entities.RuleMode m) {
    switch (m) {
      case UPGRADE:
        return RuleMode.UPGRADE;
      case DOWNGRADE:
        return RuleMode.DOWNGRADE;
      case UPDOWN:
        return RuleMode.UPDOWN;
      case WRITE:
        return RuleMode.WRITE;
      case READ:
        return RuleMode.READ;
      case WRITEREAD:
        return RuleMode.WRITEREAD;
      default:
        throw new IllegalArgumentException();
    }
  }

  public io.confluent.kafka.schemaregistry.client.rest.entities.RuleMode toEntity() {
    switch (this) {
      case UPGRADE:
        return io.confluent.kafka.schemaregistry.client.rest.entities.RuleMode.UPGRADE;
      case DOWNGRADE:
        return io.confluent.kafka.schemaregistry.client.rest.entities.RuleMode.DOWNGRADE;
      case UPDOWN:
        return io.confluent.kafka.schemaregistry.client.rest.entities.RuleMode.UPDOWN;
      case WRITE:
        return io.confluent.kafka.schemaregistry.client.rest.entities.RuleMode.WRITE;
      case READ:
        return io.confluent.kafka.schemaregistry.client.rest.entities.RuleMode.READ;
      case WRITEREAD:
        return io.confluent.kafka.schemaregistry.client.rest.entities.RuleMode.WRITEREAD;
      default:
        throw new IllegalArgumentException();
    }
  }
}
