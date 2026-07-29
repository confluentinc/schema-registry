/*
 * Copyright 2026 Confluent Inc.
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

package io.confluent.kafka.schemaregistry.encryption.alicloud;

import static io.confluent.kafka.schemaregistry.encryption.tink.KmsDriver.TEST_CLIENT;

import io.confluent.kafka.schemaregistry.encryption.EncryptionProperties;
import io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class AliCloudEncryptionProperties extends EncryptionProperties {

  public AliCloudEncryptionProperties(List<String> ruleNames, Class<?> ruleExecutor) {
    super(ruleNames, ruleExecutor);
  }

  @Override
  public String getKmsType() {
    return AliCloudKmsDriver.KMS_TYPE;
  }

  @Override
  public String getKmsKeyId() {
    return "cn-chengdu/alias%2Fconfluent-csfle";
  }

  @Override
  public Map<String, Object> getClientProperties(String baseUrls) throws Exception {
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
              + ".param." + TEST_CLIENT,
          getTestClient());
    }
    return props;
  }

  @Override
  public Object getTestClient() throws Exception {
    AliCloudKmsKeyUri keyUri =
        AliCloudKmsKeyUri.parse(AliCloudKmsClient.PREFIX + getKmsKeyId());
    return new FakeAliCloudKmsOperations(Collections.singletonList(keyUri.keyId()));
  }
}
