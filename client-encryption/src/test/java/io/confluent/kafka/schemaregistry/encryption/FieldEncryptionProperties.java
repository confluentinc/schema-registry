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
  private Class<?> ruleExecutor;

  public FieldEncryptionProperties(List<String> ruleNames) {
    this.ruleNames = ruleNames;
    this.ruleExecutor = FieldEncryptionExecutor.class;
  }

  public FieldEncryptionProperties(List<String> ruleNames, Class<?> ruleExecutor) {
    this.ruleNames = ruleNames;
    this.ruleExecutor = ruleExecutor;
  }

  public List<String> getRuleNames() {
    return ruleNames;
  }

  public Class<?> getRuleExecutor() {
    return ruleExecutor;
  }

  public abstract String getKmsType();

  public abstract String getKmsKeyId();

  public Map<String, String> getKmsProps() {
    return null;
  }

  public abstract Map<String, Object> getClientProperties(String baseUrls) throws Exception;

  public abstract Object getTestClient() throws Exception;
}

