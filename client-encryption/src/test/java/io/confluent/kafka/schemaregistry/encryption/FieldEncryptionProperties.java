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

public interface FieldEncryptionProperties {

  String getKeyId();

  default Map<String, Object> getClientProperties(List<String> ruleNames) throws Exception {
    Map<String, Object> props = getClientPropertiesWithoutKey(ruleNames);
    for (String ruleName : ruleNames) {
      props.put(AbstractKafkaSchemaSerDeConfig.RULE_EXECUTORS + "." + ruleName
              + ".param." + DEFAULT_KMS_KEY_ID,
          getKeyId());
    }
    return props;
  }

  Map<String, Object> getClientPropertiesWithoutKey(List<String> ruleNames) throws Exception;
}

