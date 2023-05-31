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

import com.google.crypto.tink.Aead;
import com.google.crypto.tink.KmsClient;
import com.google.crypto.tink.KmsClients;
import com.google.errorprone.annotations.CanIgnoreReturnValue;
import java.security.GeneralSecurityException;
import java.util.Locale;
import java.util.Optional;
import javax.annotation.Nullable;

/**
 * An implementation of noop {@link KmsClient} for testing.
 */
public final class NoopKmsClient implements KmsClient {

  /**
   * The prefix of all keys stored in Noop KMS.
   */
  public static final String PREFIX = "noop-kms://";

  @Nullable
  private String keyUri;
  private Aead aead;

  private NoopKmsClient() {
  }

  private NoopKmsClient(String uri) throws GeneralSecurityException {
    if (!uri.toLowerCase(Locale.US).startsWith(PREFIX)) {
      throw new IllegalArgumentException("key URI must start with " + PREFIX);
    }
    this.keyUri = uri;
    this.aead = new NoopAead();
  }

  /**
   * @return true either if this client is a generic one and uri starts with
   *     {@link NoopKmsClient#PREFIX}, or the client is a specific one that is bound to the key
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

  /**
   * Creates and registers a {@link #NoopKmsClient} with the Tink runtime.
   *
   * <p>If {@code keyUri} is present, it is the only key that the new client will support.
   *
   * <p>If {@code credentialPath} is present, load the credentials from that. Otherwise use the
   * default credentials.
   */
  public static KmsClient register(Optional<String> keyUri)
      throws GeneralSecurityException {
    KmsClient client = new NoopKmsClient(keyUri.orElse(PREFIX));
    KmsClients.add(client);
    return client;
  }
}
