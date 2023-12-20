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
import com.google.crypto.tink.integration.awskms.AwsKmsClient;
import io.confluent.kafka.schemaregistry.encryption.tink.KmsDriver;
import java.io.ByteArrayInputStream;
import java.lang.reflect.Field;
import java.nio.charset.StandardCharsets;
import java.security.GeneralSecurityException;
import java.util.Map;
import java.util.Optional;

public class AwsKmsDriver implements KmsDriver {

  public static final String ACCESS_KEY_ID = "access.key.id";
  public static final String SECRET_ACCESS_KEY = "secret.access.key";

  public AwsKmsDriver() {
  }

  @Override
  public String getKeyUrlPrefix() {
    return AwsKmsClient.PREFIX;
  }

  private AWSCredentialsProvider getCredentials(Map<String, ?> configs)
      throws GeneralSecurityException {
    try {
      String accessKey = (String) configs.get(ACCESS_KEY_ID);
      String secretKey = (String) configs.get(SECRET_ACCESS_KEY);
      if (accessKey != null && secretKey != null) {
        String keys = "accessKey=" + accessKey + "\n" + "secretKey=" + secretKey + "\n";
        return new AWSStaticCredentialsProvider(new PropertiesCredentials(
            new ByteArrayInputStream(keys.getBytes(StandardCharsets.UTF_8))));
      } else {
        return new DefaultAWSCredentialsProviderChain();
      }
    } catch (Exception e) {
      throw new GeneralSecurityException("cannot load credentials", e);
    }
  }

  @Override
  public KmsClient newKmsClient(Map<String, ?> configs, Optional<String> kekUrl)
      throws GeneralSecurityException {
    AWSKMS testClient = (AWSKMS) getTestClient(configs);
    Optional<AWSCredentialsProvider> creds = testClient != null
        ? Optional.empty()
        : Optional.of(getCredentials(configs));
    return newKmsClientWithAwsKms(kekUrl, creds, testClient);
  }

  protected static KmsClient newKmsClientWithAwsKms(
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

