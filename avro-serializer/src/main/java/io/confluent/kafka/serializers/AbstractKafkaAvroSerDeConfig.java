/**
 * Copyright 2015 Confluent Inc.
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
 **/

package io.confluent.kafka.serializers;

import java.util.List;
import java.util.Map;

import io.confluent.common.config.AbstractConfig;
import io.confluent.common.config.ConfigDef;
import io.confluent.common.config.ConfigDef.Importance;
import io.confluent.common.config.ConfigDef.Type;

/**
 * Base class for configs for serializers and deserializers, defining a few common configs and
 * defaults.
 */
public class AbstractKafkaAvroSerDeConfig extends AbstractConfig {

  public static final String SCHEMA_REGISTRY_URL_CONFIG = "schema.registry.url";
  public static final String
      SCHEMA_REGISTRY_URL_DOC =
      "Comma-separated list of URLs for schema registry instances that can be used to register "
      + "or look up schemas.";

  public static final String MAX_SCHEMAS_PER_SUBJECT_CONFIG = "max.schemas.per.subject";
  public static final int MAX_SCHEMAS_PER_SUBJECT_DEFAULT = 1000;
  public static final String MAX_SCHEMAS_PER_SUBJECT_DOC =
      "Maximum number of schemas to create or cache locally.";

  public static final String AUTO_REGISTER_SCHEMAS = "auto.register.schemas";
  public static final boolean AUTO_REGISTER_SCHEMAS_DEFAULT = true;
  public static final String AUTO_REGISTER_SCHEMAS_DOC =
      "Specify if the Serializer should attempt to register the Schema with Schema Registry";

  public static final String BASIC_AUTH_CREDENTIALS_SOURCE = "basic.auth.credentials.source";
  public static final String BASIC_AUTH_CREDENTIALS_SOURCE_DEFAULT = "URL";
  public static final String BASIC_AUTH_CREDENTIALS_SOURCE_DOC =
      "Specify how to pick the credentials for Basic uth header. "
      + "The supported values are URL and SASL_INHERIT";

  public static final String SCHEMA_REGISTRY_USER_INFO = "schema.registry.url.user.info";

  public static ConfigDef baseConfigDef() {
    return new ConfigDef()
        .define(SCHEMA_REGISTRY_URL_CONFIG, Type.LIST,
                Importance.HIGH, SCHEMA_REGISTRY_URL_DOC)
        .define(MAX_SCHEMAS_PER_SUBJECT_CONFIG, Type.INT, MAX_SCHEMAS_PER_SUBJECT_DEFAULT,
                Importance.LOW, MAX_SCHEMAS_PER_SUBJECT_DOC)
        .define(AUTO_REGISTER_SCHEMAS, Type.BOOLEAN, AUTO_REGISTER_SCHEMAS_DEFAULT,
                Importance.MEDIUM, AUTO_REGISTER_SCHEMAS_DOC)
        .define(BASIC_AUTH_CREDENTIALS_SOURCE, Type.STRING, BASIC_AUTH_CREDENTIALS_SOURCE_DEFAULT,
            Importance.MEDIUM, BASIC_AUTH_CREDENTIALS_SOURCE_DOC);
  }

  public AbstractKafkaAvroSerDeConfig(ConfigDef config, Map<?, ?> props) {
    super(config, props);
  }

  public int getMaxSchemasPerSubject() {
    return this.getInt(MAX_SCHEMAS_PER_SUBJECT_CONFIG);
  }

  public List<String> getSchemaRegistryUrls() {
    return this.getList(SCHEMA_REGISTRY_URL_CONFIG);
  }

  public boolean autoRegisterSchema() {
    return this.getBoolean(AUTO_REGISTER_SCHEMAS);
  }

}
