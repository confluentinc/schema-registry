/*
 * Copyright 2018 Confluent Inc.
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

public enum SchemaRegistryKeyType {
  CONFIG("CONFIG"),
  SCHEMA("SCHEMA"),
  MODE("MODE"),
  NOOP("NOOP"),
  DELETE_SUBJECT("DELETE_SUBJECT"),
  CLEAR_SUBJECT("CLEAR_SUBJECT"),
  CONTEXT("CONTEXT");

  public final String keyType;

  private SchemaRegistryKeyType(String keyType) {
    this.keyType = keyType;
  }

  public static SchemaRegistryKeyType forName(String keyType) {
    for (SchemaRegistryKeyType type : values()) {
      if (type.keyType.equals(keyType)) {
        return type;
      }
    }
    throw new IllegalArgumentException("Unknown schema registry key type : " + keyType);
  }
}

