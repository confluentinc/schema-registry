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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

import com.google.crypto.tink.Aead;
import io.confluent.kafka.schemaregistry.encryption.tink.KmsDriver;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.ServiceLoader;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.Test;

public class AliCloudKmsClientTest {

  @Test
  public void supportsGenericAliCloudKmsUris() throws Exception {
    AliCloudKmsClient client = new AliCloudKmsClient();

    assertTrue(client.doesSupport("alicloud-kms://cn-chengdu/key-1"));
    assertFalse(client.doesSupport("local-kms://key-1"));
  }

  @Test
  public void supportsOnlyBoundUriWhenKekUrlIsProvided() throws Exception {
    AliCloudKmsClient client = new AliCloudKmsClient("alicloud-kms://cn-chengdu/key-1");

    assertTrue(client.doesSupport("alicloud-kms://cn-chengdu/key-1"));
    assertFalse(client.doesSupport("alicloud-kms://cn-chengdu/key-2"));
  }

  @Test
  public void buildsAeadFromExplicitConfigBeforeEnvironmentFallback() throws Exception {
    List<AliCloudKmsConfig> captured = new ArrayList<>();
    AliCloudKmsClient client = new AliCloudKmsClient(
        Map.of(
            AliCloudKmsConfig.ACCESS_KEY_ID_CONFIG, "configured-ak",
            AliCloudKmsConfig.ACCESS_KEY_SECRET_CONFIG, "configured-sk",
            AliCloudKmsConfig.ENDPOINT_CONFIG, "kms-vpc.cn-chengdu.aliyuncs.com"),
        Optional.empty(),
        config -> {
          captured.add(config);
          return new NoopOperations();
        },
        env(Map.of(
            AliCloudKmsConfig.ACCESS_KEY_ID_ENV, "env-ak",
            AliCloudKmsConfig.ACCESS_KEY_SECRET_ENV, "env-sk")));

    Aead aead = client.getAead("alicloud-kms://cn-chengdu/alias%2Fcsfle");

    assertNotNull(aead);
    assertEquals("cn-chengdu", captured.get(0).regionId());
    assertEquals("configured-ak", captured.get(0).accessKeyId());
    assertEquals("configured-sk", captured.get(0).accessKeySecret());
    assertEquals("kms-vpc.cn-chengdu.aliyuncs.com", captured.get(0).endpoint());
    assertEquals(AliCloudKmsConfig.CredentialType.ACCESS_KEY, captured.get(0).credentialType());
  }

  @Test
  public void fallsBackToEnvironmentCredentials() throws Exception {
    List<AliCloudKmsConfig> captured = new ArrayList<>();
    AliCloudKmsClient client = new AliCloudKmsClient(
        Map.of(),
        Optional.empty(),
        config -> {
          captured.add(config);
          return new NoopOperations();
        },
        env(Map.of(
            AliCloudKmsConfig.ACCESS_KEY_ID_ENV, "env-ak",
            AliCloudKmsConfig.ACCESS_KEY_SECRET_ENV, "env-sk",
            AliCloudKmsConfig.SECURITY_TOKEN_ENV, "env-token")));

    client.getAead("alicloud-kms://cn-chengdu/key-1");

    assertEquals("env-ak", captured.get(0).accessKeyId());
    assertEquals("env-sk", captured.get(0).accessKeySecret());
    assertEquals("env-token", captured.get(0).securityToken());
    assertEquals(AliCloudKmsConfig.CredentialType.STS, captured.get(0).credentialType());
    assertEquals("sts", captured.get(0).credentialsConfig().getType());
  }

  @Test
  public void fallsBackToEnvironmentEndpoint() throws Exception {
    List<AliCloudKmsConfig> captured = new ArrayList<>();
    Path caFile = Files.createTempFile("alicloud-kms-ca", ".pem");
    Files.writeString(caFile, "-----BEGIN CERTIFICATE-----\ncert\n-----END CERTIFICATE-----\n");
    AliCloudKmsClient client = new AliCloudKmsClient(
        Map.of(
            AliCloudKmsConfig.ACCESS_KEY_ID_CONFIG, "ak",
            AliCloudKmsConfig.ACCESS_KEY_SECRET_CONFIG, "sk"),
        Optional.empty(),
        config -> {
          captured.add(config);
          return new NoopOperations();
        },
        env(Map.of(
            AliCloudKmsConfig.ENDPOINT_ENV, "kst-example.cryptoservice.kms.aliyuncs.com",
            AliCloudKmsConfig.CA_FILE_ENV, caFile.toString())));

    client.getAead("alicloud-kms://cn-chengdu/key-1");

    assertEquals("kst-example.cryptoservice.kms.aliyuncs.com", captured.get(0).endpoint());
    assertEquals(caFile.toString(), captured.get(0).caFile());
    assertEquals(Files.readString(caFile), captured.get(0).caContent());
  }

  @Test
  public void ruleExecutorPrefixCanConfigureEndpointCaFile() throws Exception {
    List<AliCloudKmsConfig> captured = new ArrayList<>();
    Path caFile = Files.createTempFile("alicloud-kms-ca", ".pem");
    Files.writeString(caFile, "-----BEGIN CERTIFICATE-----\ncert\n-----END CERTIFICATE-----\n");
    String prefix = AliCloudKmsConfig.DEFAULT_RULE_PARAM_PREFIX;
    AliCloudKmsClient client = new AliCloudKmsClient(
        Map.of(
            prefix + AliCloudKmsConfig.ACCESS_KEY_ID_CONFIG, "source-ak",
            prefix + AliCloudKmsConfig.ACCESS_KEY_SECRET_CONFIG, "source-sk",
            prefix + AliCloudKmsConfig.ENDPOINT_CONFIG, "kst-example.cryptoservice.kms.aliyuncs.com",
            prefix + AliCloudKmsConfig.CA_FILE_CONFIG, caFile.toString()),
        Optional.empty(),
        config -> {
          captured.add(config);
          return new NoopOperations();
        },
        name -> null);

    client.getAead("alicloud-kms://cn-chengdu/key-1");

    assertEquals("kst-example.cryptoservice.kms.aliyuncs.com", captured.get(0).endpoint());
    assertEquals(caFile.toString(), captured.get(0).caFile());
    assertEquals(Files.readString(caFile), captured.get(0).caContent());
  }

  @Test
  public void caBundleKeepsFullCertificateChain() throws Exception {
    List<AliCloudKmsConfig> captured = new ArrayList<>();
    Path caFile = Files.createTempFile("alicloud-kms-ca-bundle", ".pem");
    String first = "-----BEGIN CERTIFICATE-----\nroot-ca\n-----END CERTIFICATE-----\n";
    String second = "-----BEGIN CERTIFICATE-----\nregion-ca\n-----END CERTIFICATE-----\n";
    Files.writeString(caFile, first + second);
    AliCloudKmsClient client = new AliCloudKmsClient(
        Map.of(
            AliCloudKmsConfig.ACCESS_KEY_ID_CONFIG, "ak",
            AliCloudKmsConfig.ACCESS_KEY_SECRET_CONFIG, "sk",
            AliCloudKmsConfig.CA_FILE_CONFIG, caFile.toString()),
        Optional.empty(),
        config -> {
          captured.add(config);
          return new NoopOperations();
        },
        name -> null);

    client.getAead("alicloud-kms://cn-chengdu/key-1");

    assertEquals(first + second, captured.get(0).caContent());
  }

  @Test
  public void usesDefaultCredentialsProviderWhenNoExplicitCredentialsAreConfigured() throws Exception {
    List<AliCloudKmsConfig> captured = new ArrayList<>();
    AliCloudKmsClient client = new AliCloudKmsClient(
        Map.of(),
        Optional.empty(),
        config -> {
          captured.add(config);
          return new NoopOperations();
        },
        name -> null);

    client.getAead("alicloud-kms://cn-chengdu/key-1");

    assertEquals(AliCloudKmsConfig.CredentialType.DEFAULT_CHAIN, captured.get(0).credentialType());
  }

  @Test
  public void keepsExplicitDefaultCredentialTypeAsCompatibilityAlias() throws Exception {
    List<AliCloudKmsConfig> captured = new ArrayList<>();
    AliCloudKmsClient client = new AliCloudKmsClient(
        Map.of(AliCloudKmsConfig.CREDENTIAL_TYPE_CONFIG, "default"),
        Optional.empty(),
        config -> {
          captured.add(config);
          return new NoopOperations();
        },
        name -> null);

    client.getAead("alicloud-kms://cn-chengdu/key-1");

    assertEquals(AliCloudKmsConfig.CredentialType.DEFAULT_CHAIN, captured.get(0).credentialType());
  }

  @Test
  public void buildsRamRoleArnCredentialsFromExplicitConfig() throws Exception {
    List<AliCloudKmsConfig> captured = new ArrayList<>();
    AliCloudKmsClient client = new AliCloudKmsClient(
        Map.of(
            AliCloudKmsConfig.CREDENTIAL_TYPE_CONFIG, "ram_role_arn",
            AliCloudKmsConfig.ACCESS_KEY_ID_CONFIG, "source-ak",
            AliCloudKmsConfig.ACCESS_KEY_SECRET_CONFIG, "source-sk",
            AliCloudKmsConfig.SECURITY_TOKEN_CONFIG, "source-token",
            AliCloudKmsConfig.ROLE_ARN_CONFIG, "acs:ram::1234567890123456:role/csfle",
            AliCloudKmsConfig.ROLE_SESSION_NAME_CONFIG, "csfle-poc",
            AliCloudKmsConfig.ROLE_SESSION_EXPIRATION_CONFIG, "1800",
            AliCloudKmsConfig.POLICY_CONFIG, "{\"Version\":\"1\"}",
            AliCloudKmsConfig.STS_ENDPOINT_CONFIG, "sts.cn-chengdu.aliyuncs.com",
            AliCloudKmsConfig.EXTERNAL_ID_CONFIG, "external-1"),
        Optional.empty(),
        config -> {
          captured.add(config);
          return new NoopOperations();
        },
        name -> null);

    client.getAead("alicloud-kms://cn-chengdu/key-1");

    AliCloudKmsConfig config = captured.get(0);
    assertEquals(AliCloudKmsConfig.CredentialType.RAM_ROLE_ARN, config.credentialType());
    assertEquals("source-ak", config.accessKeyId());
    assertEquals("source-sk", config.accessKeySecret());
    assertEquals("source-token", config.securityToken());
    assertEquals("acs:ram::1234567890123456:role/csfle", config.roleArn());
    assertEquals("csfle-poc", config.roleSessionName());
    assertEquals(Integer.valueOf(1800), config.roleSessionExpiration());
    assertEquals("{\"Version\":\"1\"}", config.policy());
    assertEquals("sts.cn-chengdu.aliyuncs.com", config.stsEndpoint());
    assertEquals("external-1", config.externalId());
    assertEquals("ram_role_arn", config.credentialsConfig().getType());
    assertEquals("acs:ram::1234567890123456:role/csfle", config.credentialsConfig().getRoleArn());
  }

  @Test
  public void ruleExecutorPrefixCanConfigureRamRoleArnCredentials() throws Exception {
    List<AliCloudKmsConfig> captured = new ArrayList<>();
    String prefix = AliCloudKmsConfig.DEFAULT_RULE_PARAM_PREFIX;
    AliCloudKmsClient client = new AliCloudKmsClient(
        Map.of(
            prefix + AliCloudKmsConfig.CREDENTIAL_TYPE_CONFIG, "ram-role-arn",
            prefix + AliCloudKmsConfig.ACCESS_KEY_ID_CONFIG, "source-ak",
            prefix + AliCloudKmsConfig.ACCESS_KEY_SECRET_CONFIG, "source-sk",
            prefix + AliCloudKmsConfig.ROLE_ARN_CONFIG, "acs:ram::1234567890123456:role/csfle"),
        Optional.empty(),
        config -> {
          captured.add(config);
          return new NoopOperations();
        },
        name -> null);

    client.getAead("alicloud-kms://cn-chengdu/key-1");

    assertEquals(AliCloudKmsConfig.CredentialType.RAM_ROLE_ARN, captured.get(0).credentialType());
    assertEquals("alicloud-kms-csfle", captured.get(0).roleSessionName());
  }

  @Test
  public void roleArnSelectsRamRoleArnCredentialsWithoutExplicitCredentialType() throws Exception {
    List<AliCloudKmsConfig> captured = new ArrayList<>();
    AliCloudKmsClient client = new AliCloudKmsClient(
        Map.of(
            AliCloudKmsConfig.ACCESS_KEY_ID_CONFIG, "source-ak",
            AliCloudKmsConfig.ACCESS_KEY_SECRET_CONFIG, "source-sk",
            AliCloudKmsConfig.ROLE_ARN_CONFIG, "acs:ram::1234567890123456:role/csfle"),
        Optional.empty(),
        config -> {
          captured.add(config);
          return new NoopOperations();
        },
        name -> null);

    client.getAead("alicloud-kms://cn-chengdu/key-1");

    assertEquals(AliCloudKmsConfig.CredentialType.RAM_ROLE_ARN, captured.get(0).credentialType());
  }

  @Test
  public void fallsBackToRoleArnEnvironmentVariables() throws Exception {
    List<AliCloudKmsConfig> captured = new ArrayList<>();
    AliCloudKmsClient client = new AliCloudKmsClient(
        Map.of(AliCloudKmsConfig.CREDENTIAL_TYPE_CONFIG, "ram_role_arn"),
        Optional.empty(),
        config -> {
          captured.add(config);
          return new NoopOperations();
        },
        env(Map.of(
            AliCloudKmsConfig.ACCESS_KEY_ID_ENV, "env-ak",
            AliCloudKmsConfig.ACCESS_KEY_SECRET_ENV, "env-sk",
            AliCloudKmsConfig.ALIBABA_CLOUD_ROLE_ARN_ENV,
            "acs:ram::1234567890123456:role/csfle-env",
            AliCloudKmsConfig.ALIBABA_CLOUD_ROLE_SESSION_NAME_ENV, "env-session")));

    client.getAead("alicloud-kms://cn-chengdu/key-1");

    assertEquals("acs:ram::1234567890123456:role/csfle-env", captured.get(0).roleArn());
    assertEquals("env-session", captured.get(0).roleSessionName());
  }

  @Test
  public void rejectsRamRoleArnCredentialsWithoutRoleArn() throws Exception {
    AliCloudKmsClient client = new AliCloudKmsClient(
        Map.of(
            AliCloudKmsConfig.CREDENTIAL_TYPE_CONFIG, "ram_role_arn",
            AliCloudKmsConfig.ACCESS_KEY_ID_CONFIG, "source-ak",
            AliCloudKmsConfig.ACCESS_KEY_SECRET_CONFIG, "source-sk"),
        Optional.empty(),
        config -> new NoopOperations(),
        name -> null);

    assertThrows(
        java.security.GeneralSecurityException.class,
        () -> client.getAead("alicloud-kms://cn-chengdu/key-1"));
  }

  @Test
  public void rejectsUnsupportedCredentialTypeWithoutAdvertisingDefaultAsPublicOption()
      throws Exception {
    AliCloudKmsClient client = new AliCloudKmsClient(
        Map.of(AliCloudKmsConfig.CREDENTIAL_TYPE_CONFIG, "profile"),
        Optional.empty(),
        config -> new NoopOperations(),
        name -> null);

    java.security.GeneralSecurityException error = assertThrows(
        java.security.GeneralSecurityException.class,
        () -> client.getAead("alicloud-kms://cn-chengdu/key-1"));

    assertTrue(error.getMessage().contains("access_key, sts, ram_role_arn"));
    assertFalse(error.getMessage().contains("default, access_key"));
  }

  @Test
  public void cachesOperationsForSameConfig() throws Exception {
    int[] factoryCallCount = {0};
    AliCloudKmsClient client = new AliCloudKmsClient(
        Map.of(
            AliCloudKmsConfig.ACCESS_KEY_ID_CONFIG, "ak",
            AliCloudKmsConfig.ACCESS_KEY_SECRET_CONFIG, "sk"),
        Optional.empty(),
        config -> {
          factoryCallCount[0]++;
          return new NoopOperations();
        },
        name -> null);

    client.getAead("alicloud-kms://cn-chengdu/key-1");
    client.getAead("alicloud-kms://cn-chengdu/key-2");

    assertEquals(1, factoryCallCount[0]);
  }

  @Test
  public void createsNewOperationsForDifferentRegion() throws Exception {
    int[] factoryCallCount = {0};
    AliCloudKmsClient client = new AliCloudKmsClient(
        Map.of(
            AliCloudKmsConfig.ACCESS_KEY_ID_CONFIG, "ak",
            AliCloudKmsConfig.ACCESS_KEY_SECRET_CONFIG, "sk"),
        Optional.empty(),
        config -> {
          factoryCallCount[0]++;
          return new NoopOperations();
        },
        name -> null);

    client.getAead("alicloud-kms://cn-chengdu/key-1");
    client.getAead("alicloud-kms://cn-hangzhou/key-1");

    assertEquals(2, factoryCallCount[0]);
  }

  @Test
  public void boundClientSupportsCaseInsensitiveScheme() throws Exception {
    AliCloudKmsClient client = new AliCloudKmsClient(
        Map.of(
            AliCloudKmsConfig.ACCESS_KEY_ID_CONFIG, "ak",
            AliCloudKmsConfig.ACCESS_KEY_SECRET_CONFIG, "sk"),
        Optional.of("alicloud-kms://cn-chengdu/key-1"),
        config -> new NoopOperations(),
        name -> null);

    assertTrue(client.doesSupport("AliCloud-KMS://cn-chengdu/key-1"));
    assertTrue(client.doesSupport("ALICLOUD-KMS://cn-chengdu/key-1"));
    assertFalse(client.doesSupport("aliyun-kms://cn-chengdu/key-1"));
    assertFalse(client.doesSupport("alicloud-kms://cn-chengdu/key-2"));
  }

  @Test
  public void concurrentCacheMissCreatesOperationsOnlyOnce() throws Exception {
    int threads = 8;
    AtomicInteger factoryCallCount = new AtomicInteger();
    CyclicBarrier barrier = new CyclicBarrier(threads);
    AliCloudKmsClient client = new AliCloudKmsClient(
        Map.of(
            AliCloudKmsConfig.ACCESS_KEY_ID_CONFIG, "ak",
            AliCloudKmsConfig.ACCESS_KEY_SECRET_CONFIG, "sk"),
        Optional.empty(),
        config -> {
          factoryCallCount.incrementAndGet();
          return new NoopOperations();
        },
        name -> null);

    List<Thread> workers = new ArrayList<>();
    List<Throwable> errors = new ArrayList<>();
    for (int i = 0; i < threads; i++) {
      Thread t = new Thread(() -> {
        try {
          barrier.await();
          client.getAead("alicloud-kms://cn-chengdu/key-1");
        } catch (Throwable e) {
          synchronized (errors) {
            errors.add(e);
          }
        }
      });
      workers.add(t);
      t.start();
    }
    for (Thread t : workers) {
      t.join(5000);
    }

    assertTrue("unexpected errors: " + errors, errors.isEmpty());
    assertEquals(1, factoryCallCount.get());
  }

  @Test
  public void serviceLoaderFindsAliCloudKmsDriver() {
    boolean found = false;
    for (KmsDriver driver : ServiceLoader.load(KmsDriver.class)) {
      if (driver instanceof AliCloudKmsDriver) {
        found = true;
        break;
      }
    }

    assertTrue(found);
  }

  private static final class NoopOperations implements AliCloudKmsOperations {

    @Override
    public String encrypt(String keyId, String plaintextBase64, Map<String, ?> encryptionContext) {
      return "ciphertext";
    }

    @Override
    public DecryptResult decrypt(String ciphertextBlob, Map<String, ?> encryptionContext) {
      return new DecryptResult("cGxhaW50ZXh0", "key-1");
    }
  }

  private static AliCloudKmsConfig.Env env(Map<String, String> values) {
    return values::get;
  }
}
