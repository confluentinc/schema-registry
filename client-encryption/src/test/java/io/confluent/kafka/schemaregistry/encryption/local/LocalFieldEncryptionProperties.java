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

import static io.confluent.kafka.schemaregistry.encryption.local.LocalKmsDriver.OLD_SECRETS;
import static io.confluent.kafka.schemaregistry.encryption.local.LocalKmsDriver.SECRET;

import io.confluent.kafka.schemaregistry.encryption.FieldEncryptionProperties;
import io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class LocalFieldEncryptionProperties extends FieldEncryptionProperties {

  public LocalFieldEncryptionProperties(List<String> ruleNames) {
    super(ruleNames);
  }

  public LocalFieldEncryptionProperties(List<String> ruleNames, Class<?> ruleExecutor) {
    super(ruleNames, ruleExecutor);
  }

  @Override
  public String getKmsType() {
    return "local-kms";
  }

  @Override
  public String getKmsKeyId() {
    return "mykey";
  }

  @Override
  public Map<String, String> getKmsProps() {
    return Collections.singletonMap(SECRET, "mysecret");
  }

  @Override
  public Map<String, Object> getClientProperties(String baseUrls) {
    List<String> ruleNames = getRuleNames();
    Map<String, Object> props = new HashMap<>();
    props.put(AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, baseUrls);
    props.put(AbstractKafkaSchemaSerDeConfig.AUTO_REGISTER_SCHEMAS, "false");
    props.put(AbstractKafkaSchemaSerDeConfig.USE_LATEST_VERSION, "true");
    props.put(AbstractKafkaSchemaSerDeConfig.LATEST_CACHE_TTL, "60");
    props.put(AbstractKafkaSchemaSerDeConfig.RULE_EXECUTORS, String.join(",", ruleNames));
    for (String ruleName : ruleNames) {
      props.put(AbstractKafkaSchemaSerDeConfig.RULE_EXECUTORS + "." + ruleName + ".class",
          getRuleExecutor().getName());
      props.put(AbstractKafkaSchemaSerDeConfig.RULE_EXECUTORS + "." + ruleName
          + ".param." + SECRET, "mysecret");
      props.put(AbstractKafkaSchemaSerDeConfig.RULE_EXECUTORS + "." + ruleName
          + ".param." + OLD_SECRETS, "old1, old2");
    }
    return props;
  }

  @Override
  public Object getTestClient() throws Exception {
    return null;
  }
}

