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

public enum RuleKind {
  TRANSFORM,
  CONDITION;

  public static RuleKind fromEntity(
      io.confluent.kafka.schemaregistry.client.rest.entities.RuleKind r) {
    switch (r) {
      case TRANSFORM:
        return RuleKind.TRANSFORM;
      case CONDITION:
        return RuleKind.CONDITION;
      default:
        throw new IllegalArgumentException();
    }
  }

  public io.confluent.kafka.schemaregistry.client.rest.entities.RuleKind toEntity() {
    switch (this) {
      case TRANSFORM:
        return io.confluent.kafka.schemaregistry.client.rest.entities.RuleKind.TRANSFORM;
      case CONDITION:
        return io.confluent.kafka.schemaregistry.client.rest.entities.RuleKind.CONDITION;
      default:
        throw new IllegalArgumentException();
    }
  }
}
