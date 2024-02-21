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


package io.confluent.kafka.schemaregistry.client.security.bearerauth.oauth.exceptions;

/**
 * <code>SchemaRegistryOauthTokenRetrieverException</code> can be used to differentiate OAuth
 * error/exception of schema registry from that of kafka.
 */
public class SchemaRegistryOauthTokenRetrieverException extends RuntimeException {

  public SchemaRegistryOauthTokenRetrieverException(String message, Throwable cause) {
    super("Error while fetching Oauth Token for Schema Registry: " + message, cause);
  }
}
