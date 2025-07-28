/*
 * Copyright 2024 Confluent Inc.
 * Copyright 2017 Google LLC
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

package io.confluent.kafka.schemaregistry.encryption.aws;

import com.google.crypto.tink.Aead;
import com.google.crypto.tink.KeyTemplates;
import com.google.crypto.tink.KeysetHandle;
import java.nio.charset.Charset;
import java.security.GeneralSecurityException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import software.amazon.awssdk.awscore.exception.AwsServiceException;
import software.amazon.awssdk.core.SdkBytes;
import software.amazon.awssdk.services.kms.KmsClient;
import software.amazon.awssdk.services.kms.model.DecryptRequest;
import software.amazon.awssdk.services.kms.model.DecryptResponse;
import software.amazon.awssdk.services.kms.model.EncryptRequest;
import software.amazon.awssdk.services.kms.model.EncryptResponse;

/**
 * A partial, fake implementation of KmsClient that only supports encrypt and decrypt.
 *
 * <p>It creates a new AEAD for every valid key ID. It can encrypt message for these valid key IDs,
 * but fails for all other key IDs. On decrypt, it tries out all its AEADs and returns the plaintext
 * and the key ID of the AEAD that can successfully decrypt it.
 */
public class FakeAwsKms implements KmsClient {
  private static final Charset UTF_8 = Charset.forName("UTF-8");
  private final Map<String, Aead> aeads = new HashMap<>();

  private static byte[] serializeContext(Map<String, String> encryptionContext) {
    TreeMap<String, String> ordered = new TreeMap<>(encryptionContext);
    return ordered.toString().getBytes(UTF_8);
  }

  public FakeAwsKms(List<String> validKeyIds) throws GeneralSecurityException {
    for (String keyId : validKeyIds) {
      Aead aead = KeysetHandle.generateNew(KeyTemplates.get("AES256_GCM")).getPrimitive(Aead.class);
      aeads.put(keyId, aead);
    }
  }

  @Override
  public String serviceName() {
    return KmsClient.SERVICE_NAME;
  }

  @Override
  public EncryptResponse encrypt(EncryptRequest request) {
    if (!aeads.containsKey(request.keyId())) {
      throw AwsServiceException.builder()
          .message("Unknown key ID : " + request.keyId() + " is not in " + aeads.keySet())
          .build();
    }
    try {
      Aead aead = aeads.get(request.keyId());
      byte[] ciphertext =
          aead.encrypt(
              request.plaintext().asByteArray(), serializeContext(request.encryptionContext()));
      return EncryptResponse.builder()
          .keyId(request.keyId())
          .ciphertextBlob(SdkBytes.fromByteArray(ciphertext))
          .build();
    } catch (GeneralSecurityException e) {
      throw AwsServiceException.builder().message(e.getMessage()).build();
    }
  }

  @Override
  public DecryptResponse decrypt(DecryptRequest request) {
    for (Map.Entry<String, Aead> entry : aeads.entrySet()) {
      try {
        byte[] plaintext =
            entry
                .getValue()
                .decrypt(
                    request.ciphertextBlob().asByteArray(),
                    serializeContext(request.encryptionContext()));
        return DecryptResponse.builder()
            .keyId(entry.getKey())
            .plaintext(SdkBytes.fromByteArray(plaintext))
            .build();
      } catch (GeneralSecurityException e) {
        // try next key
      }
    }
    throw AwsServiceException.builder().message("unable to decrypt").build();
  }

  @Override
  public void close() {
  }
}
