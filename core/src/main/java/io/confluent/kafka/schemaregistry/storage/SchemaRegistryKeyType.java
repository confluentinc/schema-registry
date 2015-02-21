/*
 * Copyright 2014 Confluent Inc.
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

public enum SchemaRegistryKeyType {
  CONFIG("CONFIG"),
  SCHEMA("SCHEMA"),
  NOOP("NOOP");

  public final String keyType;

  private SchemaRegistryKeyType(String keyType) {
    this.keyType = keyType;
  }

  public static SchemaRegistryKeyType forName(String keyType) {
    if (CONFIG.keyType.equals(keyType)) {
      return CONFIG;
    } else if (SCHEMA.keyType.equals(keyType)) {
      return SCHEMA;
    } else if (NOOP.keyType.equals(keyType)) {
      return NOOP;
    } else {
      throw new IllegalArgumentException("Unknown schema registry key type : " + keyType
                                         + " Valid key types are {config, schema}");
    }
  }
}

