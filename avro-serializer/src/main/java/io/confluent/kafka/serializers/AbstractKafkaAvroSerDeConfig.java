/*
 * Copyright 2015-2025 Confluent Inc.
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

package io.confluent.kafka.serializers;

import java.util.Map;

import org.apache.kafka.common.config.ConfigDef;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClientConfig;
import io.confluent.kafka.serializers.subject.TopicNameStrategy;

/**
 * Use {@link io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig} instead
 */
@Deprecated
public class AbstractKafkaAvroSerDeConfig extends AbstractKafkaSchemaSerDeConfig {

  /**
   * Configurations beginning with this prefix can be used to specify headers to include in requests
   * made to Schema Registry. For example, to include an {@code Authorization} header with a value
   * of {@code Bearer NjksNDIw}, use the following configuration:
   *
   * <p>{@code request.header.Authorization=Bearer NjksNDIw}
   */
  public static final String REQUEST_HEADER_PREFIX = "request.header.";

  public static final String SCHEMA_REGISTRY_URL_CONFIG = "schema.registry.url";
  public static final String SCHEMA_REGISTRY_URL_DOC =
      "Comma-separated list of URLs for schema registry instances that can be used to register "
      + "or look up schemas. "
      + "If you wish to get a connection to a mocked schema registry for testing, "
      + "you can specify a scope using the 'mock://' pseudo-protocol. For example, "
      + "'mock://my-scope-name' corresponds to "
      + "'MockSchemaRegistry.getClientForScope(\"my-scope-name\")'.";

  public static final String MAX_SCHEMAS_PER_SUBJECT_CONFIG = "max.schemas.per.subject";
  public static final int MAX_SCHEMAS_PER_SUBJECT_DEFAULT = 1000;
  public static final String MAX_SCHEMAS_PER_SUBJECT_DOC =
      "Maximum number of schemas to create or cache locally.";

  public static final String AUTO_REGISTER_SCHEMAS = "auto.register.schemas";
  public static final boolean AUTO_REGISTER_SCHEMAS_DEFAULT = true;
  public static final String AUTO_REGISTER_SCHEMAS_DOC =
      "Specify if the Serializer should attempt to register the Schema with Schema Registry";

  public static final String BASIC_AUTH_CREDENTIALS_SOURCE = SchemaRegistryClientConfig
      .BASIC_AUTH_CREDENTIALS_SOURCE;
  public static final String BASIC_AUTH_CREDENTIALS_SOURCE_DEFAULT = "URL";
  public static final String BASIC_AUTH_CREDENTIALS_SOURCE_DOC =
      "Specify how to pick the credentials for Basic Auth header. "
      + "The supported values are URL, USER_INFO and SASL_INHERIT";

  public static final String BEARER_AUTH_CREDENTIALS_SOURCE = SchemaRegistryClientConfig
          .BEARER_AUTH_CREDENTIALS_SOURCE;
  public static final String BEARER_AUTH_CREDENTIALS_SOURCE_DEFAULT = "STATIC_TOKEN";
  public static final String BEARER_AUTH_CREDENTIALS_SOURCE_DOC =
          "Specify how to pick the credentials for Bearer Auth header. ";

  @Deprecated
  public static final String SCHEMA_REGISTRY_USER_INFO_CONFIG =
      SchemaRegistryClientConfig.SCHEMA_REGISTRY_USER_INFO_CONFIG;
  public static final String SCHEMA_REGISTRY_USER_INFO_DEFAULT = "";
  public static final String SCHEMA_REGISTRY_USER_INFO_DOC =
      "Specify the user info for Basic Auth in the form of {username}:{password}";

  public static final String USER_INFO_CONFIG =
      SchemaRegistryClientConfig.USER_INFO_CONFIG;
  public static final String USER_INFO_DEFAULT = "";

  public static final String BEARER_AUTH_TOKEN_CONFIG = SchemaRegistryClientConfig
          .BEARER_AUTH_TOKEN_CONFIG;
  public static final String BEARER_AUTH_TOKEN_DEFAULT = "";
  public static final String BEARER_AUTH_TOKEN_DOC =
          "Specify the Bearer token to be used for authentication";

  public static final String KEY_SUBJECT_NAME_STRATEGY = "key.subject.name.strategy";
  public static final String KEY_SUBJECT_NAME_STRATEGY_DEFAULT =
      TopicNameStrategy.class.getName();
  public static final String KEY_SUBJECT_NAME_STRATEGY_DOC =
      "Determines how to construct the subject name under which the key schema is registered "
      + "with the schema registry. By default, <topic>-key is used as subject.";

  public static final String VALUE_SUBJECT_NAME_STRATEGY = "value.subject.name.strategy";
  public static final String VALUE_SUBJECT_NAME_STRATEGY_DEFAULT =
      TopicNameStrategy.class.getName();
  public static final String VALUE_SUBJECT_NAME_STRATEGY_DOC =
      "Determines how to construct the subject name under which the value schema is registered "
      + "with the schema registry. By default, <topic>-value is used as subject.";

  public static final String SCHEMA_REFLECTION_CONFIG = "schema.reflection";
  public static final boolean SCHEMA_REFLECTION_DEFAULT = false;
  public static final String SCHEMA_REFLECTION_DOC =
          "If true, uses the Avro reflection API when serializing/deserializing ";

  public static final String PROXY_HOST = SchemaRegistryClientConfig.PROXY_HOST;
  public static final String PROXY_HOST_DEFAULT = "";
  public static final String PROXY_HOST_DOC =
      "The hostname, or address, of the proxy server that will be used to connect to the schema "
          + "registry instances.";

  public static final String PROXY_PORT = SchemaRegistryClientConfig.PROXY_PORT;
  public static final int PROXY_PORT_DEFAULT = -1;
  public static final String PROXY_PORT_DOC =
      "The port number of the proxy server that will be used to connect to the schema registry "
          + "instances.";

  public AbstractKafkaAvroSerDeConfig(ConfigDef config, Map<?, ?> props) {
    super(config, props);
  }
}
