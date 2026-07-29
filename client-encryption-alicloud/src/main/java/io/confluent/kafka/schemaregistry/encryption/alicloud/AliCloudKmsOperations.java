/*
 * Copyright 2026 Confluent Inc.
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

package io.confluent.kafka.schemaregistry.encryption.alicloud;

import java.util.Map;

interface AliCloudKmsOperations {

  String encrypt(String keyId, String plaintextBase64, Map<String, ?> encryptionContext)
      throws Exception;

  DecryptResult decrypt(String ciphertextBlob, Map<String, ?> encryptionContext) throws Exception;

  final class DecryptResult {
    private final String plaintextBase64;
    private final String keyId;

    DecryptResult(String plaintextBase64, String keyId) {
      this.plaintextBase64 = plaintextBase64;
      this.keyId = keyId;
    }

    String plaintextBase64() {
      return plaintextBase64;
    }

    String keyId() {
      return keyId;
    }
  }
}
