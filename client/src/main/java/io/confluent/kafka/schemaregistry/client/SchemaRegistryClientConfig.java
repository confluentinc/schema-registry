/*
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
 */

package io.confluent.kafka.schemaregistry.client;

public class SchemaRegistryClientConfig {
  public static final String BASIC_AUTH_CREDENTIALS_SOURCE = "basic.auth.credentials.source";
  public static final String SCHEMA_REGISTRY_USER_INFO_CONFIG =
      "schema.registry.basic.auth.user.info";
  public static final String SCHEMA_REGISTRY_USERNAME_CONFIG =
      "schema.registry.basic.auth.username";
  public static final String SCHEMA_REGISTRY_PASSWORD_CONFIG =
      "schema.registry.basic.auth.password";
}
