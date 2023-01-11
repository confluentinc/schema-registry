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

package io.confluent.kafka.schemaregistry.encryption.gcp;

import com.google.api.client.googleapis.auth.oauth2.GoogleCredential;
import com.google.api.client.googleapis.javanet.GoogleNetHttpTransport;
import com.google.api.client.http.javanet.NetHttpTransport;
import com.google.api.client.json.gson.GsonFactory;
import com.google.api.services.cloudkms.v1.CloudKMS;
import com.google.api.services.cloudkms.v1.CloudKMSScopes;
import com.google.auth.http.HttpCredentialsAdapter;
import com.google.auth.oauth2.GoogleCredentials;
import com.google.auto.service.AutoService;
import com.google.crypto.tink.Aead;
import com.google.crypto.tink.KmsClient;
import com.google.crypto.tink.KmsClients;
import com.google.crypto.tink.Version;
import com.google.crypto.tink.integration.gcpkms.GcpKmsAead;
import com.google.crypto.tink.subtle.Validators;
import com.google.errorprone.annotations.CanIgnoreReturnValue;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.security.GeneralSecurityException;
import java.util.Locale;
import java.util.Optional;
import javax.annotation.Nullable;

/**
 * An implementation of {@code KmsClient} for <a href="https://cloud.google.com/kms/">Google Cloud
 * KMS</a>.
 *
 * @since 1.0.0
 */
@AutoService(KmsClient.class)
public final class GcpKmsClient implements KmsClient {

  /**
   * The prefix of all keys stored in Google Cloud KMS.
   */
  public static final String PREFIX = "gcp-kms://";

  private static final String APPLICATION_NAME =
      "Tink/" + Version.TINK_VERSION + " Java/" + System.getProperty("java.version");

  @Nullable
  private CloudKMS cloudKms;
  @Nullable
  private String keyUri;

  /**
   * Constructs a generic GcpKmsClient that is not bound to any specific key.
   *
   * @deprecated use {@link #register}
   */
  @Deprecated
  public GcpKmsClient() {
  }

  /**
   * Constructs a specific GcpKmsClient that is bound to a single key identified by {@code uri}.
   *
   * @deprecated use {@link #register}
   */
  @Deprecated
  public GcpKmsClient(String uri) {
    if (!uri.toLowerCase(Locale.US).startsWith(PREFIX)) {
      throw new IllegalArgumentException("key URI must starts with " + PREFIX);
    }
    this.keyUri = uri;
  }

  /**
   * Returns true either if this client is a generic one and uri starts with
   * {@link GcpKmsClient#PREFIX}, or the client is a specific one that is bound to the key
   * identified by {@code uri}
   */
  @Override
  public boolean doesSupport(String uri) {
    if (this.keyUri != null && this.keyUri.equals(uri)) {
      return true;
    }
    return this.keyUri == null && uri.toLowerCase(Locale.US).startsWith(PREFIX);
  }

  /**
   * Loads credentials from a service account JSON file {@code credentialPath}.
   *
   * <p>If {@code credentialPath} is null, loads default Google Cloud credentials.
   */
  @CanIgnoreReturnValue
  @Override
  public KmsClient withCredentials(String credentialPath) throws GeneralSecurityException {
    if (credentialPath == null) {
      return withDefaultCredentials();
    }
    try {
      GoogleCredentials credentials =
          GoogleCredentials.fromStream(new FileInputStream(new File(credentialPath)));
      return withCredentials(credentials);
    } catch (IOException e) {
      throw new GeneralSecurityException("cannot load credentials", e);
    }
  }

  /**
   * Loads the provided credential with {@code GoogleCredential}.
   */
  @CanIgnoreReturnValue
  public KmsClient withCredentials(GoogleCredential credential) {
    if (credential.createScopedRequired()) {
      credential = credential.createScoped(CloudKMSScopes.all());
    }
    this.cloudKms =
        new CloudKMS.Builder(new NetHttpTransport(), new GsonFactory(), credential)
            .setApplicationName(APPLICATION_NAME)
            .build();
    return this;
  }

  /**
   * Loads the provided credentials with {@code GoogleCredentials}.
   */
  @CanIgnoreReturnValue
  public KmsClient withCredentials(GoogleCredentials credentials) throws GeneralSecurityException {
    if (credentials.createScopedRequired()) {
      credentials = credentials.createScoped(CloudKMSScopes.all());
    }
    try {
      this.cloudKms =
          new CloudKMS.Builder(
              GoogleNetHttpTransport.newTrustedTransport(),
              new GsonFactory(),
              new HttpCredentialsAdapter(credentials))
              .setApplicationName(APPLICATION_NAME)
              .build();
    } catch (IOException e) {
      throw new GeneralSecurityException("cannot build GCP KMS client", e);
    }
    return this;
  }

  /**
   * Loads default Google Cloud credentials.
   */
  @CanIgnoreReturnValue
  @Override
  public KmsClient withDefaultCredentials() throws GeneralSecurityException {
    try {
      GoogleCredentials credentials = GoogleCredentials.getApplicationDefault();
      return withCredentials(credentials);
    } catch (IOException e) {
      throw new GeneralSecurityException("cannot load default credentials", e);
    }
  }

  /**
   * Specifies the {@link CloudKMS} object to be used. Only used for testing.
   */
  @CanIgnoreReturnValue
  KmsClient withCloudKms(CloudKMS cloudKms) {
    this.cloudKms = cloudKms;
    return this;
  }

  @Override
  public Aead getAead(String uri) throws GeneralSecurityException {
    if (this.keyUri != null && !this.keyUri.equals(uri)) {
      throw new GeneralSecurityException(
          String.format("this client is bound to %s, cannot load keys bound to %s",
              this.keyUri, uri));
    }
    return new GcpKmsAead(cloudKms, Validators.validateKmsKeyUriAndRemovePrefix(PREFIX, uri));
  }

  /**
   * Creates and registers a {@link #GcpKmsClient} with the Tink runtime.
   *
   * <p>If {@code keyUri} is present, it is the only key that the new client will support.
   * Otherwise
   * the new client supports all GCP KMS keys.
   *
   * <p>If {@code credentialPath} is present, load the credentials from that. Otherwise use the
   * default credentials.
   */
  public static void register(Optional<String> keyUri, Optional<String> credentialPath)
      throws GeneralSecurityException {
    GcpKmsClient client;
    if (keyUri.isPresent()) {
      client = new GcpKmsClient(keyUri.get());
    } else {
      client = new GcpKmsClient();
    }
    if (credentialPath.isPresent()) {
      client.withCredentials(credentialPath.get());
    } else {
      client.withDefaultCredentials();
    }
    KmsClients.add(client);
  }
}
