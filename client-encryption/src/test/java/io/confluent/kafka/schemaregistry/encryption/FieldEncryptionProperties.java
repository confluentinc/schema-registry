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

  public abstract String getKmsType();

  public abstract String getKmsKeyId();

  public Map<String, String> getKmsProps() {
    return null;
  }

  public Map<String, Object> getClientProperties(String baseUrls) throws Exception {
    List<String> ruleNames = getRuleNames();
    Map<String, Object> props = getClientPropertiesWithoutKey(baseUrls);
    // TODO remove
    // Put local key in rule
    /*
    for (String ruleName : ruleNames) {
      props.put(AbstractKafkaSchemaSerDeConfig.RULE_EXECUTORS + "." + ruleName
              + ".param." + DEFAULT_KMS_KEY_ID,
          getKeyId());
    }
     */
    return props;
  }

  public abstract Map<String, Object> getClientPropertiesWithoutKey(String baseUrls)
      throws Exception;

  public abstract Object getTestClient() throws Exception;
}

