/*
 * Copyright 2023 Confluent Inc.
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

package io.confluent.kafka.schemaregistry.encryption.noop;

import com.google.crypto.tink.KmsClient;
import io.confluent.kafka.schemaregistry.encryption.FieldEncryptionExecutor;
import java.security.GeneralSecurityException;
import java.util.Map;
import java.util.Optional;

public class NoopFieldEncryptionExecutor extends FieldEncryptionExecutor {

  public NoopFieldEncryptionExecutor() {
  }

  @Override
  public String getKeyUrlPrefix() {
    return NoopKmsClient.PREFIX;
  }

  @Override
  @SuppressWarnings("unchecked")
  public void configure(Map<String, ?> configs) {
    // set the default kms id as empty
    ((Map<String, Object>) configs).put(FieldEncryptionExecutor.DEFAULT_KMS_KEY_ID, "");
    super.configure(configs);
  }

  @Override
  public KmsClient registerKmsClient(Optional<String> kekId) throws GeneralSecurityException {
    return NoopKmsClient.register(Optional.of(NoopKmsClient.PREFIX));
  }
}

