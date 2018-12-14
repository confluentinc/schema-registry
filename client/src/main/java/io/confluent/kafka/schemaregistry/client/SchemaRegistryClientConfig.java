/*
 * Copyright 2018 Confluent Inc.
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
  @Deprecated
  public static final String SCHEMA_REGISTRY_USER_INFO_CONFIG =
      "schema.registry.basic.auth.user.info";
  public static final String USER_INFO_CONFIG = "basic.auth.user.info";

  public static final String BEARER_AUTH_CREDENTIALS_SOURCE = "bearer.auth.credentials.source";
  public static final String BEARER_AUTH_TOKEN_CONFIG = "bearer.auth.token";

  public static final String SSL_KEYSTORE_LOCATION_CONFIG = "ssl.keystore.location";
  public static final String SSL_KEYSTORE_PASSWORD_CONFIG = "ssl.keystore.password";
  public static final String SSL_KEY_PASSWORD_CONFIG = "ssl.key.password";
  public static final String SSL_KEYSTORE_TYPE_CONFIG = "ssl.keystore.type";
  public static final String SSL_KEYMANAGER_ALGORITHM_CONFIG = "ssl.keymanager.algorithm";
  public static final String SSL_TRUSTSTORE_LOCATION_CONFIG = "ssl.truststore.location";
  public static final String SSL_TRUSTSTORE_PASSWORD_CONFIG = "ssl.truststore.password";
  public static final String SSL_TRUSTSTORE_TYPE_CONFIG = "ssl.truststore.type";
  public static final String SSL_TRUSTMANAGER_ALGORITHM_CONFIG = "ssl.trustmanager.algorithm";
  public static final String SSL_PROTOCOL_CONFIG = "ssl.protocol";
  public static final String SSL_PROVIDER_CONFIG = "ssl.provider";
  public static final String SSL_KEYSTORE_LOCATION_DOC =
      "Location of the keystore file to use for SSL. This is required for HTTPS.";
  public static final String SSL_KEYSTORE_LOCATION_DEFAULT = "";
  public static final String SSL_KEYSTORE_PASSWORD_DOC =
      "The store password for the keystore file.";
  public static final String SSL_KEYSTORE_PASSWORD_DEFAULT = "";
  public static final String SSL_KEY_PASSWORD_DOC =
      "The password of the private key in the keystore file.";
  public static final String SSL_KEY_PASSWORD_DEFAULT = "";
  public static final String SSL_KEYSTORE_TYPE_DOC =
      "The type of keystore file.";
  public static final String SSL_KEYSTORE_TYPE_DEFAULT = "JKS";
  public static final String SSL_KEYMANAGER_ALGORITHM_DOC =
      "The algorithm used by the key manager factory for SSL connections. "
          + "Leave blank to use Jetty's default.";
  public static final String SSL_KEYMANAGER_ALGORITHM_DEFAULT = "";
  public static final String SSL_TRUSTSTORE_LOCATION_DOC =
      "Location of the trust store. Required only to authenticate HTTPS clients.";
  public static final String SSL_TRUSTSTORE_LOCATION_DEFAULT = "";
  public static final String SSL_TRUSTSTORE_PASSWORD_DOC =
      "The store password for the trust store file.";
  public static final String SSL_TRUSTSTORE_PASSWORD_DEFAULT = "";
  public static final String SSL_TRUSTSTORE_TYPE_DOC =
      "The type of trust store file.";
  public static final String SSL_TRUSTSTORE_TYPE_DEFAULT = "JKS";
  public static final String SSL_TRUSTMANAGER_ALGORITHM_DOC =
      "The algorithm used by the trust manager factory for SSL connections. "
          + "Leave blank to use Jetty's default.";
  public static final String SSL_TRUSTMANAGER_ALGORITHM_DEFAULT = "";
  public static final String SSL_PROTOCOL_DOC =
      "The SSL protocol used to generate the SslContextFactory.";
  public static final String SSL_PROTOCOL_DEFAULT = "TLS";
  public static final String SSL_PROVIDER_DOC =
      "The SSL security provider name. Leave blank to use Java's default.";
  public static final String SSL_PROVIDER_DEFAULT = "";
}
