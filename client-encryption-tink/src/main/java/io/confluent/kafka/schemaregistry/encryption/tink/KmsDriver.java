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

  /**
   * Returns true if the given throwable, or any of its causes, indicates that the KMS rejected the
   * request because the caller is not authenticated or not authorized (i.e. a 401 or 403), so that
   * such failures can be surfaced as a 4xx rather than a 5xx. Walks the cause chain and delegates
   * to {@link #isAccessDeniedException(Throwable)}, which drivers override to inspect their own
   * provider-specific exception types.
   */
  default boolean isAccessDenied(Throwable t) {
    for (; t != null; t = t.getCause()) {
      if (isAccessDeniedException(t)) {
        return true;
      }
    }
    return false;
  }

  /**
   * Returns true if this single throwable (not its causes) is a provider-specific exception that
   * represents an authentication or authorization failure. Defaults to false.
   */
  default boolean isAccessDeniedException(Throwable t) {
    return false;
  }

  /**
   * Returns true if the given HTTP status code denotes an authentication/authorization failure
   * (401 or 403). Single source of truth for the access-denied status set shared by drivers.
   */
  default boolean isAccessDeniedStatus(int statusCode) {
    return statusCode == 401 || statusCode == 403;
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

