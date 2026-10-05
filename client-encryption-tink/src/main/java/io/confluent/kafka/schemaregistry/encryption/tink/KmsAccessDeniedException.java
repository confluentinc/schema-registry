/*
 * Copyright 2025 Confluent Inc.
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

import java.security.GeneralSecurityException;

/**
 * Indicates that a KMS operation failed because the caller is not authenticated or lacks the
 * permissions required to use the configured key (i.e. the KMS returned a 401 or 403). This is a
 * caller/configuration problem rather than a server error, so it should surface as a 4xx response.
 */
public class KmsAccessDeniedException extends GeneralSecurityException {

  public KmsAccessDeniedException(String message) {
    super(message);
  }

  public KmsAccessDeniedException(String message, Throwable cause) {
    super(message, cause);
  }
}
