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

import com.aliyun.credentials.Client;
import com.aliyun.credentials.provider.DefaultCredentialsProvider;
import com.aliyun.credentials.provider.RamRoleArnCredentialProvider;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.GeneralSecurityException;
import java.util.Map;
import java.util.Objects;

final class AliCloudKmsConfig {

  static final String ENDPOINT_CONFIG = "alicloud.kms.endpoint";
  static final String CA_FILE_CONFIG = "alicloud.kms.caFile";
  static final String CREDENTIAL_TYPE_CONFIG = "alicloud.kms.credentialType";
  static final String ACCESS_KEY_ID_CONFIG = "alicloud.kms.accessKeyId";
  static final String ACCESS_KEY_SECRET_CONFIG = "alicloud.kms.accessKeySecret";
  static final String SECURITY_TOKEN_CONFIG = "alicloud.kms.securityToken";
  static final String ROLE_ARN_CONFIG = "alicloud.kms.roleArn";
  static final String ROLE_SESSION_NAME_CONFIG = "alicloud.kms.roleSessionName";
  static final String ROLE_SESSION_EXPIRATION_CONFIG = "alicloud.kms.roleSessionExpiration";
  static final String POLICY_CONFIG = "alicloud.kms.policy";
  static final String STS_ENDPOINT_CONFIG = "alicloud.kms.stsEndpoint";
  static final String EXTERNAL_ID_CONFIG = "alicloud.kms.externalId";
  static final String DEFAULT_RULE_PARAM_PREFIX = "rule.executors._default_.param.";

  static final String ACCESS_KEY_ID_ENV = "ALIBABA_CLOUD_ACCESS_KEY_ID";
  static final String ACCESS_KEY_SECRET_ENV = "ALIBABA_CLOUD_ACCESS_KEY_SECRET";
  static final String SECURITY_TOKEN_ENV = "ALIBABA_CLOUD_SECURITY_TOKEN";
  static final String ENDPOINT_ENV = "ALICLOUD_KMS_ENDPOINT";
  static final String CA_FILE_ENV = "ALICLOUD_KMS_CA_FILE";
  static final String CREDENTIAL_TYPE_ENV = "ALICLOUD_KMS_CREDENTIAL_TYPE";
  static final String ROLE_ARN_ENV = "ALICLOUD_KMS_ROLE_ARN";
  static final String ALIBABA_CLOUD_ROLE_ARN_ENV = "ALIBABA_CLOUD_ROLE_ARN";
  static final String ROLE_SESSION_NAME_ENV = "ALICLOUD_KMS_ROLE_SESSION_NAME";
  static final String ALIBABA_CLOUD_ROLE_SESSION_NAME_ENV = "ALIBABA_CLOUD_ROLE_SESSION_NAME";
  static final String ROLE_SESSION_EXPIRATION_ENV = "ALICLOUD_KMS_ROLE_SESSION_EXPIRATION";
  static final String POLICY_ENV = "ALICLOUD_KMS_ROLE_POLICY";
  static final String STS_ENDPOINT_ENV = "ALICLOUD_KMS_STS_ENDPOINT";
  static final String EXTERNAL_ID_ENV = "ALICLOUD_KMS_EXTERNAL_ID";

  private static final String DEFAULT_ROLE_SESSION_NAME = "alicloud-kms-csfle";

  private final String regionId;
  private final String endpoint;
  private final String caFile;
  private final String caContent;
  private final CredentialType credentialType;
  private final String accessKeyId;
  private final String accessKeySecret;
  private final String securityToken;
  private final String roleArn;
  private final String roleSessionName;
  private final Integer roleSessionExpiration;
  private final String policy;
  private final String stsEndpoint;
  private final String externalId;

  private AliCloudKmsConfig(
      String regionId,
      String endpoint,
      String caFile,
      String caContent,
      CredentialType credentialType,
      String accessKeyId,
      String accessKeySecret,
      String securityToken,
      String roleArn,
      String roleSessionName,
      Integer roleSessionExpiration,
      String policy,
      String stsEndpoint,
      String externalId) {
    this.regionId = regionId;
    this.endpoint = endpoint;
    this.caFile = caFile;
    this.caContent = caContent;
    this.credentialType = credentialType;
    this.accessKeyId = accessKeyId;
    this.accessKeySecret = accessKeySecret;
    this.securityToken = securityToken;
    this.roleArn = roleArn;
    this.roleSessionName = roleSessionName;
    this.roleSessionExpiration = roleSessionExpiration;
    this.policy = policy;
    this.stsEndpoint = stsEndpoint;
    this.externalId = externalId;
  }

  static AliCloudKmsConfig from(Map<String, ?> configs, String regionId, Env env)
      throws GeneralSecurityException {
    Objects.requireNonNull(regionId, "regionId");
    Objects.requireNonNull(env, "env");

    String accessKeyId = value(configs, ACCESS_KEY_ID_CONFIG, env, ACCESS_KEY_ID_ENV);
    String accessKeySecret = value(configs, ACCESS_KEY_SECRET_CONFIG, env, ACCESS_KEY_SECRET_ENV);
    String securityToken = value(configs, SECURITY_TOKEN_CONFIG, env, SECURITY_TOKEN_ENV);
    String roleArn = value(configs, ROLE_ARN_CONFIG, env, ROLE_ARN_ENV, ALIBABA_CLOUD_ROLE_ARN_ENV);
    CredentialType credentialType = CredentialType.from(
        value(configs, CREDENTIAL_TYPE_CONFIG, env, CREDENTIAL_TYPE_ENV),
        accessKeyId,
        accessKeySecret,
        securityToken,
        roleArn);

    if (credentialType.requiresSourceKeys() && (isBlank(accessKeyId) || isBlank(accessKeySecret))) {
      throw new GeneralSecurityException(
          "Alibaba Cloud KMS credentials are required: configure "
              + ACCESS_KEY_ID_CONFIG + "/" + ACCESS_KEY_SECRET_CONFIG
              + " or " + ACCESS_KEY_ID_ENV + "/" + ACCESS_KEY_SECRET_ENV);
    }
    if (credentialType == CredentialType.STS && isBlank(securityToken)) {
      throw new GeneralSecurityException(
          "Alibaba Cloud KMS STS credentials require " + SECURITY_TOKEN_CONFIG
              + " or " + SECURITY_TOKEN_ENV);
    }

    String roleSessionName = value(
        configs,
        ROLE_SESSION_NAME_CONFIG,
        env,
        ROLE_SESSION_NAME_ENV,
        ALIBABA_CLOUD_ROLE_SESSION_NAME_ENV);
    String roleSessionExpirationValue = value(
        configs,
        ROLE_SESSION_EXPIRATION_CONFIG,
        env,
        ROLE_SESSION_EXPIRATION_ENV);
    Integer roleSessionExpiration = null;

    if (credentialType == CredentialType.RAM_ROLE_ARN) {
      if (isBlank(roleArn)) {
        throw new GeneralSecurityException(
            "Alibaba Cloud KMS AssumeRole credentials require " + ROLE_ARN_CONFIG
                + " or " + ROLE_ARN_ENV + "/" + ALIBABA_CLOUD_ROLE_ARN_ENV);
      }
      if (isBlank(roleSessionName)) {
        roleSessionName = DEFAULT_ROLE_SESSION_NAME;
      }
      roleSessionExpiration = integerValue(
          roleSessionExpirationValue,
          ROLE_SESSION_EXPIRATION_CONFIG);
    }

    String caFile = value(configs, CA_FILE_CONFIG, env, CA_FILE_ENV);

    return new AliCloudKmsConfig(
        regionId,
        value(configs, ENDPOINT_CONFIG, env, ENDPOINT_ENV),
        caFile,
        caContent(caFile),
        credentialType,
        accessKeyId,
        accessKeySecret,
        securityToken,
        roleArn,
        roleSessionName,
        roleSessionExpiration,
        value(configs, POLICY_CONFIG, env, POLICY_ENV),
        value(configs, STS_ENDPOINT_CONFIG, env, STS_ENDPOINT_ENV),
        value(configs, EXTERNAL_ID_CONFIG, env, EXTERNAL_ID_ENV));
  }

  String regionId() {
    return regionId;
  }

  String endpoint() {
    return endpoint;
  }

  String caFile() {
    return caFile;
  }

  String caContent() {
    return caContent;
  }

  CredentialType credentialType() {
    return credentialType;
  }

  String accessKeyId() {
    return accessKeyId;
  }

  String accessKeySecret() {
    return accessKeySecret;
  }

  String securityToken() {
    return securityToken;
  }

  String roleArn() {
    return roleArn;
  }

  String roleSessionName() {
    return roleSessionName;
  }

  Integer roleSessionExpiration() {
    return roleSessionExpiration;
  }

  String policy() {
    return policy;
  }

  String stsEndpoint() {
    return stsEndpoint;
  }

  String externalId() {
    return externalId;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof AliCloudKmsConfig)) {
      return false;
    }
    AliCloudKmsConfig that = (AliCloudKmsConfig) o;
    return Objects.equals(regionId, that.regionId)
        && Objects.equals(endpoint, that.endpoint)
        && Objects.equals(caContent, that.caContent)
        && credentialType == that.credentialType
        && Objects.equals(accessKeyId, that.accessKeyId)
        && Objects.equals(accessKeySecret, that.accessKeySecret)
        && Objects.equals(securityToken, that.securityToken)
        && Objects.equals(roleArn, that.roleArn)
        && Objects.equals(roleSessionName, that.roleSessionName)
        && Objects.equals(roleSessionExpiration, that.roleSessionExpiration)
        && Objects.equals(policy, that.policy)
        && Objects.equals(stsEndpoint, that.stsEndpoint)
        && Objects.equals(externalId, that.externalId);
  }

  @Override
  public int hashCode() {
    return Objects.hash(regionId, endpoint, caContent, credentialType,
        accessKeyId, accessKeySecret, securityToken,
        roleArn, roleSessionName, roleSessionExpiration,
        policy, stsEndpoint, externalId);
  }

  Client credentialsClient() {
    if (credentialType == CredentialType.DEFAULT_CHAIN) {
      return new Client();
    }
    if (credentialType == CredentialType.RAM_ROLE_ARN) {
      return new Client(ramRoleArnProvider());
    }
    return new Client(credentialsConfig());
  }

  com.aliyun.credentials.models.Config credentialsConfig() {
    com.aliyun.credentials.models.Config config = new com.aliyun.credentials.models.Config()
        .setType(credentialType.value())
        .setAccessKeyId(accessKeyId)
        .setAccessKeySecret(accessKeySecret)
        .setSecurityToken(securityToken);

    if (credentialType == CredentialType.RAM_ROLE_ARN) {
      config
          .setRoleArn(roleArn)
          .setRoleSessionName(roleSessionName)
          .setPolicy(policy)
          .setSTSEndpoint(stsEndpoint)
          .setExternalId(externalId);
      if (roleSessionExpiration != null) {
        config.setRoleSessionExpiration(roleSessionExpiration);
      }
    }
    return config;
  }

  private RamRoleArnCredentialProvider ramRoleArnProvider() {
    RamRoleArnCredentialProvider.Builder builder = RamRoleArnCredentialProvider.builder()
        .roleArn(roleArn)
        .roleSessionName(roleSessionName)
        .policy(policy)
        .STSEndpoint(stsEndpoint)
        .externalId(externalId);
    if (roleSessionExpiration != null) {
      builder.durationSeconds(roleSessionExpiration);
    }
    if (isBlank(accessKeyId) || isBlank(accessKeySecret)) {
      builder.credentialsProvider(new DefaultCredentialsProvider());
    } else {
      builder
          .accessKeyId(accessKeyId)
          .accessKeySecret(accessKeySecret)
          .securityToken(securityToken);
    }
    return builder.build();
  }

  private static String value(Map<String, ?> configs, String key, Env env, String... envNames) {
    String configured = configValue(configs, key);
    if (!isBlank(configured)) {
      return configured;
    }
    if (envNames == null) {
      return null;
    }
    for (String envName : envNames) {
      if (envName != null) {
        String value = env.get(envName);
        if (!isBlank(value)) {
          return value;
        }
      }
    }
    return null;
  }

  private static String configValue(Map<String, ?> configs, String key) {
    if (configs == null) {
      return null;
    }
    Object raw = configs.get(key);
    if (raw == null) {
      raw = configs.get(DEFAULT_RULE_PARAM_PREFIX + key);
    }
    return raw == null ? null : raw.toString();
  }

  private static boolean isBlank(String value) {
    return value == null || value.isBlank();
  }

  private static Integer integerValue(String value, String key) throws GeneralSecurityException {
    if (isBlank(value)) {
      return null;
    }
    try {
      int parsed = Integer.parseInt(value);
      if (parsed < 900) {
        throw new GeneralSecurityException(key + " must be at least 900 seconds");
      }
      return parsed;
    } catch (NumberFormatException e) {
      throw new GeneralSecurityException(key + " must be an integer number of seconds", e);
    }
  }

  private static String caContent(String caFile) throws GeneralSecurityException {
    if (isBlank(caFile)) {
      return null;
    }
    try {
      String content = Files.readString(Path.of(caFile), StandardCharsets.UTF_8);
      if (isBlank(content)) {
        throw new GeneralSecurityException("Alibaba Cloud KMS CA file is empty: " + caFile);
      }
      return content;
    } catch (IOException e) {
      throw new GeneralSecurityException("failed to read Alibaba Cloud KMS CA file: " + caFile, e);
    }
  }

  enum CredentialType {
    DEFAULT_CHAIN("default"),
    ACCESS_KEY("access_key"),
    STS("sts"),
    RAM_ROLE_ARN("ram_role_arn");

    private final String value;

    CredentialType(String value) {
      this.value = value;
    }

    String value() {
      return value;
    }

    boolean requiresSourceKeys() {
      return this == ACCESS_KEY || this == STS;
    }

    static CredentialType from(
        String raw,
        String accessKeyId,
        String accessKeySecret,
        String securityToken,
        String roleArn) throws GeneralSecurityException {
      if (isBlank(raw)) {
        if (!isBlank(roleArn)) {
          return RAM_ROLE_ARN;
        }
        if (!isBlank(securityToken)) {
          return STS;
        }
        return isBlank(accessKeyId) || isBlank(accessKeySecret) ? DEFAULT_CHAIN : ACCESS_KEY;
      }
      String normalized = raw.trim().toLowerCase(java.util.Locale.US).replace('-', '_');
      for (CredentialType type : values()) {
        if (type.value.equals(normalized)) {
          return type;
        }
      }
      throw new GeneralSecurityException(
          "unsupported Alibaba Cloud KMS credential type " + raw
              + "; supported explicit values are access_key, sts, ram_role_arn. "
              + "Unset " + CREDENTIAL_TYPE_CONFIG + " to use the default provider chain");
    }
  }

  @FunctionalInterface
  interface Env {
    String get(String name);
  }
}
