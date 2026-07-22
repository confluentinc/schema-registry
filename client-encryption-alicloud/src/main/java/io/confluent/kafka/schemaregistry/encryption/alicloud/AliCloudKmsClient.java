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

import com.google.crypto.tink.Aead;
import com.google.crypto.tink.KmsClient;
import com.google.crypto.tink.KmsClients;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.GeneralSecurityException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;

public final class AliCloudKmsClient implements KmsClient {

  /** The prefix of all keys stored in Alibaba Cloud KMS. */
  public static final String PREFIX = "alicloud-kms://";

  private final Map<String, ?> configs;
  private final Optional<String> keyUri;
  private final AliCloudKmsOperationsFactory operationsFactory;
  private final AliCloudKmsConfig.Env env;
  private final ConcurrentHashMap<AliCloudKmsConfig, AliCloudKmsOperations> operationsCache =
      new ConcurrentHashMap<>();

  /**
   * Constructs a generic AliCloudKmsClient that is not bound to any specific key.
   */
  public AliCloudKmsClient() {
    this(Collections.emptyMap(), Optional.empty(), new AliCloudKmsSdkOperations.Factory(),
        System::getenv);
  }

  /**
   * Constructs an AliCloudKmsClient that is bound to a single key identified by {@code uri}.
   */
  public AliCloudKmsClient(String uri) {
    this(Collections.emptyMap(), Optional.of(uri), new AliCloudKmsSdkOperations.Factory(),
        System::getenv);
  }

  public AliCloudKmsClient(Map<String, ?> configs, Optional<String> kekUrl)
      throws GeneralSecurityException {
    this(configs, kekUrl, new AliCloudKmsSdkOperations.Factory(), System::getenv);
  }

  AliCloudKmsClient(
      Map<String, ?> configs,
      Optional<String> kekUrl,
      AliCloudKmsOperationsFactory operationsFactory,
      AliCloudKmsConfig.Env env) {
    this.configs = copyConfigs(configs);
    this.keyUri = normalize(kekUrl);
    this.operationsFactory = operationsFactory;
    this.env = env;
    if (this.keyUri.isPresent()) {
      try {
        AliCloudKmsKeyUri.parse(this.keyUri.get());
      } catch (GeneralSecurityException e) {
        throw new IllegalArgumentException("invalid Alibaba Cloud KMS key URI", e);
      }
    }
  }

  @Override
  public boolean doesSupport(String uri) {
    if (uri == null) {
      return false;
    }
    if (!hasAliCloudKmsPrefix(uri)) {
      return false;
    }
    if (keyUri.isPresent()) {
      return sameKeyUri(keyUri.get(), uri);
    }
    return true;
  }

  @Override
  public KmsClient withCredentials(String credentialPath) throws GeneralSecurityException {
    if (credentialPath == null) {
      return withDefaultCredentials();
    }
    Properties properties = new Properties();
    try (InputStream input = Files.newInputStream(Path.of(credentialPath))) {
      properties.load(input);
    } catch (IOException e) {
      throw new GeneralSecurityException("failed to load Alibaba Cloud KMS credentials", e);
    }

    Map<String, Object> merged = new HashMap<>(configs);
    for (String name : properties.stringPropertyNames()) {
      merged.put(name, properties.getProperty(name));
    }
    return new AliCloudKmsClient(merged, keyUri, operationsFactory, env);
  }

  @Override
  public KmsClient withDefaultCredentials() {
    return this;
  }

  @Override
  public Aead getAead(String uri) throws GeneralSecurityException {
    if (!doesSupport(uri)) {
      throw new GeneralSecurityException("Alibaba Cloud KMS client does not support " + uri);
    }

    AliCloudKmsKeyUri key = AliCloudKmsKeyUri.parse(uri);
    AliCloudKmsConfig config = AliCloudKmsConfig.from(configs, key.regionId(), env);
    AliCloudKmsOperations operations = cachedOperations(config);
    return new AliCloudKmsAead(key, operations);
  }

  static boolean hasAliCloudKmsPrefix(String uri) {
    return uri != null && uri.length() >= PREFIX.length()
        && uri.substring(0, PREFIX.length()).toLowerCase(Locale.US).equals(PREFIX);
  }

  static boolean sameKeyUri(String a, String b) {
    if (a == null || b == null) {
      return false;
    }
    if (!hasAliCloudKmsPrefix(a) || !hasAliCloudKmsPrefix(b)) {
      return false;
    }
    return a.substring(PREFIX.length()).equals(b.substring(PREFIX.length()));
  }

  private AliCloudKmsOperations cachedOperations(AliCloudKmsConfig config)
      throws GeneralSecurityException {
    try {
      return operationsCache.computeIfAbsent(config, c -> {
        try {
          return operationsFactory.create(c);
        } catch (GeneralSecurityException e) {
          throw new OperationsCreationException(e);
        }
      });
    } catch (OperationsCreationException e) {
      throw (GeneralSecurityException) e.getCause();
    }
  }

  private static final class OperationsCreationException extends RuntimeException {
    OperationsCreationException(GeneralSecurityException cause) {
      super(cause);
    }
  }

  private static Map<String, ?> copyConfigs(Map<String, ?> configs) {
    if (configs == null || configs.isEmpty()) {
      return Collections.emptyMap();
    }
    Map<String, Object> copy = new HashMap<>();
    configs.forEach((key, value) -> {
      if (key != null && value != null) {
        copy.put(key, value);
      }
    });
    return Collections.unmodifiableMap(copy);
  }

  private static Optional<String> normalize(Optional<String> kekUrl) {
    if (kekUrl == null || kekUrl.isEmpty() || kekUrl.get().isBlank()) {
      return Optional.empty();
    }
    return Optional.of(kekUrl.get());
  }

  /**
   * Creates and registers an Alibaba Cloud KMS client with the Tink runtime.
   */
  public static void register(Optional<String> keyUri, Optional<String> credentialPath)
      throws GeneralSecurityException {
    registerWithOperationsFactory(
        keyUri,
        credentialPath,
        new AliCloudKmsSdkOperations.Factory(),
        System::getenv);
  }

  static void registerWithOperationsFactory(
      Optional<String> keyUri,
      Optional<String> credentialPath,
      AliCloudKmsOperationsFactory operationsFactory,
      AliCloudKmsConfig.Env env) throws GeneralSecurityException {
    AliCloudKmsClient client = new AliCloudKmsClient(
        Collections.emptyMap(),
        keyUri,
        operationsFactory,
        env);
    if (credentialPath.isPresent()) {
      client = (AliCloudKmsClient) client.withCredentials(credentialPath.get());
    } else {
      client = (AliCloudKmsClient) client.withDefaultCredentials();
    }
    KmsClients.add(client);
  }
}
