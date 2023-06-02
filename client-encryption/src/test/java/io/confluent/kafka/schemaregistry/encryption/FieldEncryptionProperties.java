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
package io.confluent.kafka.schemaregistry.encryption;

import static io.confluent.kafka.schemaregistry.encryption.FieldEncryptionExecutor.DEFAULT_KMS_KEY_ID;

import io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig;
import java.util.List;
import java.util.Map;

public abstract class FieldEncryptionProperties {

  private List<String> ruleNames;

  public FieldEncryptionProperties(List<String> ruleNames) {
    this.ruleNames = ruleNames;
  }

  public List<String> getRuleNames() {
    return ruleNames;
  }

  public abstract String getKeyId();

  public Map<String, Object> getClientProperties() throws Exception {
    List<String> ruleNames = getRuleNames();
    Map<String, Object> props = getClientPropertiesWithoutKey();
    for (String ruleName : ruleNames) {
      props.put(AbstractKafkaSchemaSerDeConfig.RULE_EXECUTORS + "." + ruleName
              + ".param." + DEFAULT_KMS_KEY_ID,
          getKeyId());
    }
    return props;
  }

  public abstract Map<String, Object> getClientPropertiesWithoutKey() throws Exception;
}

