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

package io.confluent.kafka.schemaregistry.client.rest.entities;

public enum RuleMode {
  UPGRADE,
  DOWNGRADE,
  UPDOWN,
  WRITE,
  READ,
  WRITEREAD;

  public boolean isMigrationRule() {
    return this == UPGRADE || this == DOWNGRADE || this == UPDOWN;
  }

  public boolean isDomainRule() {
    return this == WRITE || this == READ || this == WRITEREAD;
  }
}
