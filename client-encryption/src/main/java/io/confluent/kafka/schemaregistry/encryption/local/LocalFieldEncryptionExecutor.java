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

package io.confluent.kafka.schemaregistry.encryption.local;

import static io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig.RULE_EXECUTORS;

import io.confluent.kafka.schemaregistry.encryption.FieldEncryptionExecutor;
import java.security.GeneralSecurityException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;

public class LocalFieldEncryptionExecutor extends FieldEncryptionExecutor {

  public static final String LOCAL_SECRET = "secret";
  public static final String LOCAL_OLD_SECRETS = "old.secrets";

  public LocalFieldEncryptionExecutor() {
  }

  public void configure(Map<String, ?> configs) {
    try {
      super.configure(configs);
      String secret = (String) configs.get(LOCAL_SECRET);
      if (secret == null) {
        throw new IllegalArgumentException("Missing property "
            + RULE_EXECUTORS + ".<name>.param." + LOCAL_SECRET);
      }
      String oldSecretsStr = (String) configs.get(LOCAL_OLD_SECRETS);
      List<String> oldSecrets;
      if (oldSecretsStr != null) {
        oldSecrets = Arrays.asList(oldSecretsStr.split(","));
      } else {
        oldSecrets = Collections.emptyList();
      }
      LocalKmsClient.register(Optional.of(LocalKmsClient.PREFIX), secret, oldSecrets);

      setDefaultKekId(LocalKmsClient.PREFIX);
    } catch (GeneralSecurityException e) {
      throw new IllegalArgumentException(e);
    }
  }
}

