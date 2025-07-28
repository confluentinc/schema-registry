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
import java.util.Map;
import java.util.Optional;

public class LocalKmsDriver implements KmsDriver {

  public static final String SECRET = "secret";

  public static final String LOCAL_SECRET = "LOCAL_SECRET";

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

  @Override
  public KmsClient newKmsClient(Map<String, ?> configs, Optional<String> kekUrl)
      throws GeneralSecurityException {
    return new LocalKmsClient(kekUrl.orElse(LocalKmsClient.PREFIX), getSecret(configs));
  }
}

