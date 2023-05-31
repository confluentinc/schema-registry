/*
 * Copyright 2022 Confluent Inc.
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

package io.confluent.kafka.schemaregistry.encryption.multikms;

import com.google.crypto.tink.KmsClient;
import io.confluent.kafka.schemaregistry.encryption.FieldEncryptionExecutor;
import io.confluent.kafka.schemaregistry.rules.RuleContext;
import java.security.GeneralSecurityException;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.ServiceLoader;

public class MultiKmsFieldEncryptionExecutor extends FieldEncryptionExecutor {

  public static final String ENCRYPT_KMS_TYPE = "encrypt.kms.type";

  public static final String KMS_TYPE_SUFFIX = "-kms://";

  private Map<String, FieldEncryptionExecutor> executors;
  private Map<String, Boolean> configured;
  private Map<String, ?> configs;

  public MultiKmsFieldEncryptionExecutor() {
    Class<FieldEncryptionExecutor> cls = FieldEncryptionExecutor.class;
    ServiceLoader<? extends FieldEncryptionExecutor> serviceLoader =
        ServiceLoader.load(cls, cls.getClassLoader());

    this.executors = new HashMap<>();
    this.configured = new HashMap<>();
    for (FieldEncryptionExecutor executor : serviceLoader) {
      this.executors.put(executor.getKeyUrlPrefix(null), executor);
    }
  }

  @Override
  public String getKeyUrlPrefix(RuleContext ctx) {
    String kmsType = ctx.getParameter(ENCRYPT_KMS_TYPE);
    if (kmsType == null || kmsType.isEmpty()) {
      throw new IllegalArgumentException("Rule parameter '" + ENCRYPT_KMS_TYPE + "' is required");
    }
    return kmsType + KMS_TYPE_SUFFIX;
  }

  @Override
  @SuppressWarnings("unchecked")
  public void configure(Map<String, ?> configs) {
    this.configs = configs;
    super.configure(configs);
  }

  @Override
  public KmsClient registerKmsClient(Optional<String> kekId) throws GeneralSecurityException {
    if (kekId.isPresent()) {
      for (Map.Entry<String, FieldEncryptionExecutor> entry : executors.entrySet()) {
        String keyUrlPrefix = entry.getKey();
        FieldEncryptionExecutor executor = entry.getValue();
        if (executor.doesSupport(kekId.get())) {
          Boolean previous = configured.putIfAbsent(keyUrlPrefix, true);
          if (previous == null || !previous) {
            executor.configure(configs);
          }
          return executor.registerKmsClient(kekId);
        }
      }
      throw new IllegalArgumentException("Unrecognized key url " + kekId.get());
    }
    throw new IllegalArgumentException("Missing key url");
  }
}

