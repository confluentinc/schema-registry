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

import com.google.crypto.tink.KmsClient;
import io.confluent.kafka.schemaregistry.encryption.tink.KmsDriver;
import java.security.GeneralSecurityException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;

public class LocalKmsDriver implements KmsDriver {

  public static final String SECRET = "secret";
  public static final String OLD_SECRETS = "old.secrets";

  public static final String LOCAL_SECRET = "LOCAL_SECRET";
  public static final String LOCAL_OLD_SECRETS = "LOCAL_OLD_SECRETS";

  public LocalKmsDriver() {
  }

  @Override
  public String getKeyUrlPrefix() {
    return LocalKmsClient.PREFIX;
  }

  private String getSecret(Map<String, ?> configs) throws GeneralSecurityException {
    String secret = (String) configs.get(SECRET);
    if (secret == null) {
      secret = System.getenv(LOCAL_SECRET);
    }
    if (secret == null) {
      throw new GeneralSecurityException("cannot load secret");
    }
    return secret;
  }

  private List<String> getOldSecrets(Map<String, ?> configs) {
    String oldSecretsStr = (String) configs.get(OLD_SECRETS);
    if (oldSecretsStr == null) {
      oldSecretsStr = System.getenv(LOCAL_OLD_SECRETS);
    }
    if (oldSecretsStr != null) {
      return Arrays.asList(oldSecretsStr.split(","));
    } else {
      return Collections.emptyList();
    }
  }

  @Override
  public KmsClient newKmsClient(Map<String, ?> configs, Optional<String> kekUrl)
      throws GeneralSecurityException {
    return new LocalKmsClient(
        kekUrl.orElse(LocalKmsClient.PREFIX), getSecret(configs), getOldSecrets(configs));
  }
}

