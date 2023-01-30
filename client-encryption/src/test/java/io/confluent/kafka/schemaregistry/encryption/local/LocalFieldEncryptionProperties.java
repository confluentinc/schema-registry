/*
 * Copyright 2023 Confluent Inc.
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
package io.confluent.kafka.schemaregistry.encryption.local;

import static io.confluent.kafka.schemaregistry.encryption.local.LocalFieldEncryptionExecutor.LOCAL_OLD_SECRETS;
import static io.confluent.kafka.schemaregistry.encryption.local.LocalFieldEncryptionExecutor.LOCAL_SECRET;

import io.confluent.kafka.schemaregistry.encryption.FieldEncryptionProperties;
import io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig;
import java.util.HashMap;
import java.util.Map;

public class LocalFieldEncryptionProperties implements FieldEncryptionProperties {

  @Override
  public String getKeyId() {
    return "";
  }

  @Override
  public Map<String, Object> getClientPropertiesWithoutKey() throws Exception {
    Map<String, Object> props = new HashMap<>();
    props.put(AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, "bogus");
    props.put(AbstractKafkaSchemaSerDeConfig.AUTO_REGISTER_SCHEMAS, "false");
    props.put(AbstractKafkaSchemaSerDeConfig.USE_LATEST_VERSION, "true");
    props.put(AbstractKafkaSchemaSerDeConfig.LATEST_CACHE_TTL, "60");
    props.put(AbstractKafkaSchemaSerDeConfig.RULE_EXECUTORS, "exec");
    props.put(AbstractKafkaSchemaSerDeConfig.RULE_EXECUTORS + ".exec.class",
        LocalFieldEncryptionExecutor.class.getName());
    props.put(AbstractKafkaSchemaSerDeConfig.RULE_EXECUTORS + ".exec.param."
        + LOCAL_SECRET, "mysecret");
    props.put(AbstractKafkaSchemaSerDeConfig.RULE_EXECUTORS + ".exec.param."
        + LOCAL_OLD_SECRETS, "old1, old2");
    return props;
  }
}

