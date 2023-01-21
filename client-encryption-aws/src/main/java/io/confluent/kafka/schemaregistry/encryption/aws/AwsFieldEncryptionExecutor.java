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

package io.confluent.kafka.schemaregistry.encryption.aws;

import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.DefaultAWSCredentialsProviderChain;
import com.amazonaws.auth.PropertiesCredentials;
import com.amazonaws.services.kms.AWSKMS;
import com.google.crypto.tink.KmsClient;
import com.google.crypto.tink.KmsClients;
import io.confluent.kafka.schemaregistry.encryption.FieldEncryptionExecutor;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.lang.reflect.Field;
import java.nio.charset.StandardCharsets;
import java.security.GeneralSecurityException;
import java.util.Map;
import java.util.Optional;

public class AwsFieldEncryptionExecutor extends FieldEncryptionExecutor {

  public static final String ACCESS_KEY_ID = "access.key.id";
  public static final String SECRET_ACCESS_KEY = "secret.access.key";

  private AWSCredentialsProvider credentials;

  public AwsFieldEncryptionExecutor() {
  }

  @Override
  public void configure(Map<String, ?> configs) {
    try {
      super.configure(configs);
      String keyId = (String) configs.get(DEFAULT_KMS_KEY_ID);
      // Key id is not mandatory for decryption
      String keyUri = keyId != null ? AwsKmsClient.PREFIX + keyId : null;
      setDefaultKekId(keyUri);
      String accessKey = (String) configs.get(ACCESS_KEY_ID);
      String secretKey = (String) configs.get(SECRET_ACCESS_KEY);
      if (accessKey != null && secretKey != null) {
        String keys = "accessKey=" + accessKey + "\n" + "secretKey=" + secretKey + "\n";
        credentials = new AWSStaticCredentialsProvider(new PropertiesCredentials(
            new ByteArrayInputStream(keys.getBytes(StandardCharsets.UTF_8))));
      } else {
        credentials = new DefaultAWSCredentialsProviderChain();
      }
      // register client w/o keyUri so it can be overridden
      registerKmsClient(Optional.empty());
    } catch (GeneralSecurityException | IOException e) {
      throw new IllegalArgumentException(e);
    }
  }

  @Override
  public KmsClient registerKmsClient(Optional<String> kekId) throws GeneralSecurityException {
    return registerWithAwsKms(kekId, Optional.of(credentials), (AWSKMS) getTestClient());
  }

  public static KmsClient registerWithAwsKms(
      Optional<String> keyUri, Optional<AWSCredentialsProvider> credentials, AWSKMS awsKms)
      throws GeneralSecurityException {
    AwsKmsClient client;
    if (keyUri.isPresent()) {
      client = new AwsKmsClient(keyUri.get());
    } else {
      client = new AwsKmsClient();
    }
    if (credentials.isPresent()) {
      client.withCredentialsProvider(credentials.get());
    } else {
      client.withDefaultCredentials();
    }
    if (awsKms != null) {
      setAwsKms(client, awsKms);
    }
    KmsClients.add(client);
    return client;
  }

  private static void setAwsKms(AwsKmsClient client, AWSKMS awsKms) {
    try {
      Field field = AwsKmsClient.class.getDeclaredField("awsKms");
      field.setAccessible(true);
      field.set(client, awsKms);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }
}

