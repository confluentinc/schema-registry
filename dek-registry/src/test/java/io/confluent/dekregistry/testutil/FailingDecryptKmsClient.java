/*
 * Copyright 2026 Confluent Inc.
 *
 * Licensed under the Confluent Community License (the "License"); you may not use
 * this file except in compliance with the License.  You may obtain a copy of the
 * License at
 *
 * http://www.confluent.io/confluent-community-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OF ANY KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations under the License.
 */

package io.confluent.dekregistry.testutil;

import com.google.crypto.tink.Aead;
import com.google.crypto.tink.KmsClient;
import com.google.crypto.tink.Registry;
import com.google.crypto.tink.proto.AesGcmKey;
import com.google.crypto.tink.subtle.Hkdf;
import com.google.errorprone.annotations.CanIgnoreReturnValue;
import com.google.protobuf.ByteString;
import java.nio.charset.StandardCharsets;
import java.security.GeneralSecurityException;
import java.util.Locale;

/**
 * Test {@link KmsClient} whose Aead succeeds at encrypt but always fails at decrypt.
 *
 * <p>If the key uri contains {@link #ACCESS_DENIED_MARKER} the simulated decrypt failure carries
 * that marker so that {@link FailingDecryptKmsDriver#isAccessDeniedException(Throwable)} classifies
 * it as an authentication/authorization failure (HTTP 403); otherwise it is a generic failure
 * (HTTP 500).
 */
public final class FailingDecryptKmsClient implements KmsClient {

  public static final String PREFIX = "test-fail-decrypt-kms://";

  /** When present in the key uri, the simulated decrypt failure is treated as access-denied. */
  public static final String ACCESS_DENIED_MARKER = "access-denied";

  private static final String AES_GCM_KEY = "type.googleapis.com/google.crypto.tink.AesGcmKey";

  private final Aead realAead;

  public FailingDecryptKmsClient(String secret) throws GeneralSecurityException {
    byte[] keyBytes = Hkdf.computeHkdf(
        "HmacSha256", secret.getBytes(StandardCharsets.UTF_8), null, null, 16);
    AesGcmKey key = AesGcmKey.newBuilder()
        .setVersion(0)
        .setKeyValue(ByteString.copyFrom(keyBytes))
        .build();
    this.realAead = Registry.getPrimitive(AES_GCM_KEY, key.toByteString(), Aead.class);
  }

  @Override
  public boolean doesSupport(String uri) {
    return uri.toLowerCase(Locale.US).startsWith(PREFIX);
  }

  @Override
  @CanIgnoreReturnValue
  public KmsClient withCredentials(String credentialPath) {
    return this;
  }

  @Override
  @CanIgnoreReturnValue
  public KmsClient withDefaultCredentials() {
    return this;
  }

  @Override
  public Aead getAead(String uri) {
    boolean accessDenied = uri != null && uri.contains(ACCESS_DENIED_MARKER);
    String failureMessage = accessDenied
        ? "simulated decrypt failure: " + ACCESS_DENIED_MARKER
        : "simulated decrypt failure";
    return new Aead() {
      @Override
      public byte[] encrypt(byte[] plaintext, byte[] associatedData) throws GeneralSecurityException {
        return realAead.encrypt(plaintext, associatedData);
      }

      @Override
      public byte[] decrypt(byte[] ciphertext, byte[] associatedData) throws GeneralSecurityException {
        throw new GeneralSecurityException(failureMessage);
      }
    };
  }
}
