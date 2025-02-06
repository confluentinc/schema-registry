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

import com.google.common.base.Splitter;
import com.google.crypto.tink.KmsClient;
import io.confluent.kafka.schemaregistry.encryption.tink.KmsDriver;
import java.lang.reflect.Field;
import java.security.GeneralSecurityException;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider;
import software.amazon.awssdk.auth.credentials.ProfileCredentialsProvider;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.sts.StsClient;
import software.amazon.awssdk.services.sts.auth.StsAssumeRoleCredentialsProvider;
import software.amazon.awssdk.services.sts.model.AssumeRoleRequest;

public class AwsKmsDriver implements KmsDriver {

  public static final String ACCESS_KEY_ID = "access.key.id";
  public static final String SECRET_ACCESS_KEY = "secret.access.key";
  public static final String PROFILE = "profile";
  public static final String ROLE_ARN = "role.arn";
  public static final String ROLE_SESSION_NAME = "role.session.name";
  public static final String ROLE_EXTERNAL_ID = "role.external.id";

  public static final String AWS_ROLE_ARN = "AWS_ROLE_ARN";
  public static final String AWS_ROLE_SESSION_NAME = "AWS_ROLE_SESSION_NAME";
  public static final String AWS_ROLE_EXTERNAL_ID = "AWS_ROLE_EXTERNAL_ID";

  public AwsKmsDriver() {
  }

  @Override
  public String getKeyUrlPrefix() {
    return AwsKmsClient.PREFIX;
  }

  private AwsCredentialsProvider getCredentials(Map<String, ?> configs, Optional<String> kekUrl)
      throws GeneralSecurityException {
    try {
      String roleArn = (String) configs.get(ROLE_ARN);
      if (roleArn == null) {
        roleArn = System.getenv(AWS_ROLE_ARN);
      }
      String roleSessionName = (String) configs.get(ROLE_SESSION_NAME);
      if (roleSessionName == null) {
        roleSessionName = System.getenv(AWS_ROLE_SESSION_NAME);
      }
      String roleExternalId = (String) configs.get(ROLE_EXTERNAL_ID);
      if (roleExternalId == null) {
        roleExternalId = System.getenv(AWS_ROLE_EXTERNAL_ID);
      }
      String accessKey = (String) configs.get(ACCESS_KEY_ID);
      String secretKey = (String) configs.get(SECRET_ACCESS_KEY);
      String profile = (String) configs.get(PROFILE);
      AwsCredentialsProvider provider;
      if (accessKey != null && secretKey != null) {
        provider = StaticCredentialsProvider.create(
            AwsBasicCredentials.create(accessKey, secretKey));
      } else if (profile != null) {
        provider = ProfileCredentialsProvider.create(profile);
      } else {
        provider = DefaultCredentialsProvider.create();
      }
      if (roleArn != null) {
        Region region = getRegionFromKeyId(
            AwsKmsClient.removePrefix(AwsKmsClient.PREFIX, kekUrl.get()));
        return buildRoleProvider(provider, region, roleArn, roleSessionName, roleExternalId);
      } else {
        return provider;
      }
    } catch (Exception e) {
      throw new GeneralSecurityException("cannot load credentials", e);
    }
  }

  private AwsCredentialsProvider buildRoleProvider(
      AwsCredentialsProvider provider, Region region,
      String roleArn, String roleSessionName, String roleExternalId) {
    StsClient stsClient = StsClient.builder()
        .credentialsProvider(provider)
        .region(region)
        .build();
    return StsAssumeRoleCredentialsProvider.builder()
        .stsClient(stsClient)
        .refreshRequest(() -> buildRoleRequest(roleArn, roleSessionName, roleExternalId))
        .build();
  }

  private AssumeRoleRequest buildRoleRequest(
      String roleArn, String roleSessionName, String roleExternalId) {
    AssumeRoleRequest.Builder req = AssumeRoleRequest.builder()
        .roleArn(roleArn)
        .roleSessionName(roleSessionName != null ? roleSessionName : "confluent-encrypt");
    if (roleExternalId != null) {
      req = req.externalId(roleExternalId);
    }
    return req.build();
  }

  public static Region getRegionFromKeyId(String keyId) {
    List<String> tokens = Splitter.on(':').splitToList(keyId);
    if (tokens.size() < 4) {
      throw new IllegalArgumentException("invalid key URI");
    }
    String regionName = tokens.get(3);
    return Region.of(regionName);
  }

  @Override
  public KmsClient newKmsClient(Map<String, ?> configs, Optional<String> kekUrl)
      throws GeneralSecurityException {
    software.amazon.awssdk.services.kms.KmsClient testClient =
        (software.amazon.awssdk.services.kms.KmsClient) getTestClient(configs);
    Optional<AwsCredentialsProvider> creds = testClient != null
        ? Optional.empty()
        : Optional.of(getCredentials(configs, kekUrl));
    return newKmsClientWithAwsKms(kekUrl, creds, testClient);
  }

  protected static KmsClient newKmsClientWithAwsKms(
      Optional<String> keyUri, Optional<AwsCredentialsProvider> credentials,
      software.amazon.awssdk.services.kms.KmsClient awsKms)
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

  private static void setAwsKms(AwsKmsClient client,
      software.amazon.awssdk.services.kms.KmsClient awsKms) {
    try {
      Field field = AwsKmsClient.class.getDeclaredField("awsKms");
      field.setAccessible(true);
      field.set(client, awsKms);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }
}

