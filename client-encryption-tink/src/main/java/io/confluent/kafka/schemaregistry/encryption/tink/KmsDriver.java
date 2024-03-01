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

package io.confluent.kafka.schemaregistry.encryption.tink;

import com.google.crypto.tink.KmsClient;
import com.google.crypto.tink.KmsClients;
import java.security.GeneralSecurityException;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;

public interface KmsDriver {

  String KMS_TYPE_SUFFIX = "://";
  String TEST_CLIENT = "test.client";

  String getKeyUrlPrefix();

  KmsClient newKmsClient(Map<String, ?> configs, Optional<String> kekUrl)
      throws GeneralSecurityException;

  /**
   * @return true if this client does support {@code keyUri}
   */
  default boolean doesSupport(String keyUri) {
    return keyUri.toLowerCase(Locale.US).startsWith(getKeyUrlPrefix());
  }

  default KmsClient getKmsClient(String kekUrl) throws GeneralSecurityException {
    return KmsClients.get(kekUrl);
  }

  default KmsClient registerKmsClient(Map<String, ?> configs, Optional<String> kekUrl)
      throws GeneralSecurityException {
    KmsClient client = newKmsClient(configs, kekUrl);
    KmsClients.add(client);
    return client;
  }

  default Object getTestClient(Map<String, ?> configs) {
    return configs.get(TEST_CLIENT);
  }
}

