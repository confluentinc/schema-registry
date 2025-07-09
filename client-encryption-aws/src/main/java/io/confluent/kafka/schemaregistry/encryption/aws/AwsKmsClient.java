/*
 * Copyright 2024 Confluent Inc.
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

import com.google.common.base.Splitter;
import com.google.crypto.tink.Aead;
import com.google.crypto.tink.KmsClient;
import com.google.crypto.tink.KmsClients;
import com.google.errorprone.annotations.CanIgnoreReturnValue;
import java.nio.file.Paths;
import java.security.GeneralSecurityException;
import java.util.List;
import java.util.Locale;
import java.util.Optional;
import javax.annotation.Nullable;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider;
import software.amazon.awssdk.auth.credentials.ProfileCredentialsProvider;
import software.amazon.awssdk.awscore.exception.AwsServiceException;
import software.amazon.awssdk.profiles.ProfileFile;
import software.amazon.awssdk.regions.Region;

/**
 * An implementation of {@link KmsClient} for <a href="https://aws.amazon.com/kms/">AWS KMS</a>.
 *
 * @since 1.0.0
 */
public final class AwsKmsClient implements KmsClient {
  /** The prefix of all keys stored in AWS KMS. */
  public static final String PREFIX = "aws-kms://";

  @Nullable private software.amazon.awssdk.services.kms.KmsClient awsKms;
  @Nullable private String keyUri;
  @Nullable private AwsCredentialsProvider provider;

  /**
   * Constructs a generic AwsKmsClient that is not bound to any specific key.
   * This constructor should not be used. We recommend to register the client instead.
   */
  public AwsKmsClient() {}

  /**
   * Constructs a specific AwsKmsClient that is bound to a single key identified by {@code uri}.
   * This constructor should not be used. We recommend to register the client instead.
   */
  public AwsKmsClient(String uri) {
    if (!uri.toLowerCase(Locale.US).startsWith(PREFIX)) {
      throw new IllegalArgumentException("key URI must starts with " + PREFIX);
    }
    this.keyUri = uri;
  }

  /**
   * @return true either if this client is a generic one and uri starts with {@link
   *     AwsKmsClient#PREFIX}, or the client is a specific one that is bound to the key identified
   *     by {@code uri}.
   */
  @Override
  public boolean doesSupport(String uri) {
    if (this.keyUri != null && this.keyUri.equals(uri)) {
      return true;
    }
    return this.keyUri == null && uri.toLowerCase(Locale.US).startsWith(PREFIX);
  }

  /**
   * Loads AWS credentials from a properties file.
   *
   * <p>The AWS access key ID is expected to be in the <code>accessKey</code> property and the AWS
   * secret key is expected to be in the <code>secretKey</code> property.
   *
   * @throws GeneralSecurityException if the client initialization fails
   */
  @Override
  @CanIgnoreReturnValue
  public KmsClient withCredentials(String credentialPath) throws GeneralSecurityException {
    try {
      if (credentialPath == null) {
        return withDefaultCredentials();
      }
      return withCredentialsProvider(ProfileCredentialsProvider.builder()
          .profileFile(ProfileFile.builder().content(Paths.get(credentialPath)).build()).build());
    } catch (AwsServiceException e) {
      throw new GeneralSecurityException("cannot load credentials", e);
    }
  }

  /**
   * Loads default AWS credentials.
   *
   * <p>AWS credentials provider chain that looks for credentials in this order:
   *
   * <ul>
   *   <li>Environment Variables - AWS_ACCESS_KEY_ID and AWS_SECRET_KEY
   *   <li>Java System Properties - aws.accessKeyId and aws.secretKey
   *   <li>Credential profiles file at the default location (~/.aws/credentials)
   *   <li>Instance profile credentials delivered through the Amazon EC2 metadata service
   * </ul>
   *
   * @throws GeneralSecurityException if the client initialization fails
   */
  @Override
  @CanIgnoreReturnValue
  public KmsClient withDefaultCredentials() throws GeneralSecurityException {
    try {
      return withCredentialsProvider(DefaultCredentialsProvider.create());
    } catch (AwsServiceException e) {
      throw new GeneralSecurityException("cannot load default credentials", e);
    }
  }

  /** Loads AWS credentials from a provider. */
  @CanIgnoreReturnValue
  public KmsClient withCredentialsProvider(AwsCredentialsProvider provider)
      throws GeneralSecurityException {
    this.provider = provider;
    return this;
  }

  /**
   * Specifies the {@link software.amazon.awssdk.services.kms.KmsClient} object to be used.
   * Only used for testing.
   */
  @CanIgnoreReturnValue
  KmsClient withAwsKms(@Nullable software.amazon.awssdk.services.kms.KmsClient awsKms) {
    this.awsKms = awsKms;
    return this;
  }

  protected static String removePrefix(String expectedPrefix, String kmsKeyUri) {
    if (!kmsKeyUri.toLowerCase(Locale.US).startsWith(expectedPrefix)) {
      throw new IllegalArgumentException(
          String.format("key URI must start with %s", expectedPrefix));
    }
    return kmsKeyUri.substring(expectedPrefix.length());
  }

  @Override
  public Aead getAead(String uri) throws GeneralSecurityException {
    if (this.keyUri != null && !this.keyUri.equals(uri)) {
      throw new GeneralSecurityException(
          String.format(
              "this client is bound to %s, cannot load keys bound to %s", this.keyUri, uri));
    }

    try {
      String keyId = removePrefix(PREFIX, uri);
      software.amazon.awssdk.services.kms.KmsClient client = awsKms;
      List<String> tokens = Splitter.on(':').splitToList(keyId);
      if (tokens.size() < 4) {
        throw new IllegalArgumentException("invalid key URI");
      }
      String regionName = tokens.get(3);
      if (client == null) {
        client =
            software.amazon.awssdk.services.kms.KmsClient.builder()
                .credentialsProvider(provider)
                .region(Region.of(regionName))
                .build();
      }
      return new AwsKmsAead(client, keyId);
    } catch (AwsServiceException e) {
      throw new GeneralSecurityException("cannot load credentials from provider", e);
    }
  }

  /**
   * Creates and registers a {@link #AwsKmsClient} with the Tink runtime.
   *
   * <p>If {@code keyUri} is present, it is the only key that the new client will support. Otherwise
   * the new client supports all AWS KMS keys.
   *
   * <p>If {@code credentialPath} is present, load the credentials from that. Otherwise use the
   * default credentials.
   */
  public static void register(Optional<String> keyUri, Optional<String> credentialPath)
      throws GeneralSecurityException {
    registerWithAwsKms(keyUri, credentialPath, null);
  }

  /**
   * Does the same as {@link #register}, but with an additional {@code awsKms} argument. Only used
   * for testing.
   */
  static void registerWithAwsKms(
      Optional<String> keyUri, Optional<String> credentialPath,
      @Nullable software.amazon.awssdk.services.kms.KmsClient awsKms)
      throws GeneralSecurityException {
    AwsKmsClient client;
    if (keyUri.isPresent()) {
      client = new AwsKmsClient(keyUri.get());
    } else {
      client = new AwsKmsClient();
    }
    if (credentialPath.isPresent()) {
      client.withCredentials(credentialPath.get());
    } else {
      client.withDefaultCredentials();
    }
    client.withAwsKms(awsKms);
    KmsClients.add(client);
  }
}
