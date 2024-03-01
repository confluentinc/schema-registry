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

package io.confluent.dekregistry.testutil;

import com.google.crypto.tink.Aead;
import com.google.crypto.tink.KmsClient;
import com.google.crypto.tink.PrimitiveSet;
import com.google.crypto.tink.Registry;
import com.google.crypto.tink.proto.AesGcmKey;
import com.google.crypto.tink.proto.KeyStatusType;
import com.google.crypto.tink.proto.Keyset.Key;
import com.google.crypto.tink.proto.OutputPrefixType;
import com.google.crypto.tink.subtle.Hkdf;
import com.google.errorprone.annotations.CanIgnoreReturnValue;
import com.google.protobuf.ByteString;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.security.GeneralSecurityException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Locale;
import javax.annotation.Nullable;

/**
 * An implementation of {@link KmsClient} for testing.
 */
public final class TestKmsClient implements KmsClient {

  public static final String PREFIX = "test-kms://";

  private static final String AES_GCM_KEY = "type.googleapis.com/google.crypto.tink.AesGcmKey";

  @Nullable
  private String keyUri;
  private Aead aead;

  private TestKmsClient() {
  }

  public TestKmsClient(String uri, String secret)
      throws GeneralSecurityException {
    if (!uri.toLowerCase(Locale.US).startsWith(PREFIX)) {
      throw new IllegalArgumentException("key URI must start with " + PREFIX);
    }
    this.keyUri = uri;

    PrimitiveSet.Builder<Aead> builder = PrimitiveSet.newBuilder(Aead.class);
    builder.addPrimaryPrimitive(getPrimitive(secret), getKey(secret));
    this.aead = Registry.wrap(builder.build());
  }

  private Aead getPrimitive(String secret)
      throws GeneralSecurityException {
    byte[] keyBytes = Hkdf.computeHkdf(
        "HmacSha256", secret.getBytes(StandardCharsets.UTF_8), null, null, 16);
    AesGcmKey key = AesGcmKey.newBuilder()
        .setVersion(0)
        .setKeyValue(ByteString.copyFrom(keyBytes))
        .build();
    return Registry.getPrimitive(AES_GCM_KEY, key.toByteString(), Aead.class);
  }

  private Key getKey(String secret) throws GeneralSecurityException {
    return Key.newBuilder()
        .setKeyId(getId(secret))
        .setStatus(KeyStatusType.ENABLED)
        .setOutputPrefixType(OutputPrefixType.TINK)
        .build();
  }

  private int getId(String secret) throws GeneralSecurityException {
    try {
      MessageDigest md = MessageDigest.getInstance("MD5");
      md.update(secret.getBytes(StandardCharsets.UTF_8));
      return ByteBuffer.wrap(md.digest()).getInt();
    } catch (NoSuchAlgorithmException e) {
      throw new GeneralSecurityException(e);
    }
  }

  /**
   * @return true either if this client is a generic one and uri starts with
   *     {@link TestKmsClient#PREFIX}, or the client is a specific one that is bound to the key
   *     identified by {@code uri}.
   */
  @Override
  public boolean doesSupport(String uri) {
    if (this.keyUri != null && this.keyUri.equals(uri)) {
      return true;
    }
    return this.keyUri == null && uri.toLowerCase(Locale.US).startsWith(PREFIX);
  }

  /**
   * Loads credentials from a properties file.
   *
   * @throws GeneralSecurityException if the client initialization fails
   */
  @Override
  @CanIgnoreReturnValue
  public KmsClient withCredentials(String credentialPath) throws GeneralSecurityException {
    return this;
  }

  /**
   * Loads default credentials.
   *
   * @throws GeneralSecurityException if the client initialization fails
   */
  @Override
  @CanIgnoreReturnValue
  public KmsClient withDefaultCredentials() throws GeneralSecurityException {
    return this;
  }

  @Override
  public Aead getAead(String uri) throws GeneralSecurityException {
    if (this.keyUri != null && !this.keyUri.equals(uri)) {
      throw new GeneralSecurityException(
          String.format(
              "this client is bound to %s, cannot load keys bound to %s", this.keyUri, uri));
    }

    return aead;
  }
}