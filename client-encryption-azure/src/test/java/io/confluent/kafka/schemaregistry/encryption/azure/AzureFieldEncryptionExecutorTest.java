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

package io.confluent.kafka.schemaregistry.encryption.azure;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.azure.core.exception.HttpResponseException;
import com.azure.core.http.HttpResponse;
import com.azure.security.keyvault.keys.cryptography.CryptographyClient;
import com.azure.security.keyvault.keys.cryptography.models.DecryptResult;
import com.azure.security.keyvault.keys.cryptography.models.EncryptResult;
import com.azure.security.keyvault.keys.cryptography.models.EncryptionAlgorithm;
import com.azure.security.keyvault.keys.models.KeyVaultKey;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSortedSet;
import io.confluent.dekregistry.client.rest.entities.Dek;
import io.confluent.kafka.schemaregistry.avro.AvroSchema;
import io.confluent.kafka.schemaregistry.client.rest.entities.Metadata;
import io.confluent.kafka.schemaregistry.client.rest.entities.Rule;
import io.confluent.kafka.schemaregistry.client.rest.entities.RuleSet;
import io.confluent.kafka.schemaregistry.encryption.FieldEncryptionExecutor;
import io.confluent.kafka.schemaregistry.encryption.FieldEncryptionExecutorTest;
import io.confluent.kafka.schemaregistry.encryption.EncryptionProperties;
import io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig;
import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import java.nio.charset.StandardCharsets;
import java.security.GeneralSecurityException;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.IndexedRecord;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.junit.Test;

public class AzureFieldEncryptionExecutorTest extends FieldEncryptionExecutorTest {

  private static final String VERSION_A = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
  private static final String VERSION_B = "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb";

  public AzureFieldEncryptionExecutorTest() throws Exception {
    super();
  }

  @Override
  protected EncryptionProperties getFieldEncryptionProperties(
      List<String> ruleNames, Class<?> ruleExecutor) {
    return new AzureEncryptionProperties(ruleNames, ruleExecutor);
  }

  private static HttpResponseException httpException(int statusCode) {
    HttpResponse response = mock(HttpResponse.class);
    when(response.getStatusCode()).thenReturn(statusCode);
    return new HttpResponseException("denied", response);
  }

  @Test
  public void testIsAccessDeniedForForbiddenAndUnauthorized() {
    AzureKmsDriver driver = new AzureKmsDriver();
    assertTrue(driver.isAccessDeniedException(httpException(403)));
    assertTrue(driver.isAccessDeniedException(httpException(401)));
  }

  @Test
  public void testIsNotAccessDeniedForOtherErrors() {
    AzureKmsDriver driver = new AzureKmsDriver();
    assertFalse(driver.isAccessDeniedException(httpException(404)));
    assertFalse(driver.isAccessDeniedException(httpException(500)));
    assertFalse(driver.isAccessDeniedException(new RuntimeException("not azure")));
  }

  @Test
  public void testIsAccessDeniedWalksCauseChain() {
    AzureKmsDriver driver = new AzureKmsDriver();
    assertTrue(driver.isAccessDenied(
        new GeneralSecurityException("encryption failed", httpException(403))));
    assertFalse(driver.isAccessDenied(
        new GeneralSecurityException("encryption failed", httpException(503))));
    assertFalse(driver.isAccessDenied(null));
  }

  @Test
  public void testGetVersionedKeyIdReturnsUnchangedWhenAlreadyVersioned() throws Exception {
    String versionedKeyId = "https://yokota1.vault.azure.net/keys/key1/1234567890";
    @SuppressWarnings("unchecked")
    Function<String, KeyVaultKey> keyResolver = mock(Function.class);
    Map<String, Object> configs = new HashMap<>();
    configs.put(AzureKmsDriver.TEST_KEY_CLIENT, keyResolver);

    String result = AzureKmsDriver.getVersionedKeyId(configs, versionedKeyId);

    assertEquals(versionedKeyId, result);
    verify(keyResolver, never()).apply(any());
  }

  @Test
  public void testGetVersionedKeyIdResolvesVersionlessKeyId() throws Exception {
    String versionlessKeyId = "https://yokota1.vault.azure.net/keys/key1";
    String resolvedKeyId = "https://yokota1.vault.azure.net/keys/key1/" + VERSION_A;
    KeyVaultKey keyVaultKey = mock(KeyVaultKey.class);
    when(keyVaultKey.getId()).thenReturn(resolvedKeyId);
    @SuppressWarnings("unchecked")
    Function<String, KeyVaultKey> keyResolver = mock(Function.class);
    when(keyResolver.apply("key1")).thenReturn(keyVaultKey);
    Map<String, Object> configs = new HashMap<>();
    configs.put(AzureKmsDriver.TEST_KEY_CLIENT, keyResolver);

    String result = AzureKmsDriver.getVersionedKeyId(configs, versionlessKeyId);

    assertEquals(resolvedKeyId, result);
    verify(keyResolver).apply("key1");
  }

  @Test(expected = GeneralSecurityException.class)
  public void testGetVersionedKeyIdThrowsForMalformedKeyId() throws Exception {
    Map<String, Object> configs = new HashMap<>();
    AzureKmsDriver.getVersionedKeyId(configs, "https://yokota1.vault.azure.net/notkeys/key1");
  }

  @Test(expected = GeneralSecurityException.class)
  public void testGetVersionedKeyIdThrowsForInvalidUri() throws Exception {
    Map<String, Object> configs = new HashMap<>();
    AzureKmsDriver.getVersionedKeyId(configs, "::not a uri::");
  }

  @Test
  public void testWithVersionCombinesVersionlessKeyIdWithExplicitVersion() throws Exception {
    String versionlessKeyId = "https://yokota1.vault.azure.net/keys/key1";

    String result = AzureKmsDriver.withVersion(versionlessKeyId, VERSION_A);

    assertEquals("https://yokota1.vault.azure.net/keys/key1/" + VERSION_A, result);
  }

  @Test
  public void testWithVersionIgnoresAnyExistingVersionInKeyId() throws Exception {
    String versionedKeyId = "https://yokota1.vault.azure.net/keys/key1/" + VERSION_A;

    String result = AzureKmsDriver.withVersion(versionedKeyId, VERSION_B);

    assertEquals("https://yokota1.vault.azure.net/keys/key1/" + VERSION_B, result);
  }

  @Test(expected = GeneralSecurityException.class)
  public void testWithVersionThrowsForMalformedKeyId() throws Exception {
    AzureKmsDriver.withVersion("https://yokota1.vault.azure.net/notkeys/key1", VERSION_A);
  }

  // ==================== AzureKmsAead unit tests ====================

  private static CryptographyClient fakeCryptographyClient() throws Exception {
    return AzureEncryptionProperties.mockClient("unused-key-id-for-mock");
  }

  private static AzureKmsAead.EncryptTarget fixedEncryptTarget(
      CryptographyClient client, String version) {
    return () -> new AzureKmsAead.EncryptTarget.Resolved(client, version);
  }

  @Test
  public void testAeadEncryptWithoutEncryptTargetReturnsRawCiphertext() throws Exception {
    CryptographyClient client = fakeCryptographyClient();
    AzureKmsAead aead = new AzureKmsAead(client, EncryptionAlgorithm.RSA_OAEP_256);

    byte[] ciphertext = aead.encrypt("hello".getBytes(StandardCharsets.UTF_8), new byte[0]);

    assertFalse(startsWithAzureV1Prefix(ciphertext));
    byte[] plaintext = aead.decrypt(ciphertext, new byte[0]);
    assertArrayEquals("hello".getBytes(StandardCharsets.UTF_8), plaintext);
  }

  @Test
  public void testAeadEncryptWithEncryptTargetPrefixesOutputWithoutDoubleEncoding()
      throws Exception {
    CryptographyClient client = fakeCryptographyClient();
    AzureKmsAead aead = new AzureKmsAead(
        client, EncryptionAlgorithm.RSA_OAEP_256, fixedEncryptTarget(client, VERSION_A), null);

    byte[] plaintext = "hello".getBytes(StandardCharsets.UTF_8);
    byte[] ciphertext = aead.encrypt(plaintext, new byte[0]);
    byte[] rawWrapped = client.encrypt(EncryptionAlgorithm.RSA_OAEP_256, plaintext).getCipherText();

    assertTrue(startsWithAzureV1Prefix(ciphertext));
    assertEquals(VERSION_A, extractAzureV1Version(ciphertext));
    // The ciphertext bytes are appended directly, not base64-encoded a second time: total length
    // is exactly prefix + version + colon + the raw wrapped bytes, nothing more.
    assertEquals("azure:v1:".length() + 32 + 1 + rawWrapped.length, ciphertext.length);
  }

  @Test
  public void testAeadDecryptUsesEmbeddedVersionViaClientFactory() throws Exception {
    CryptographyClient client = fakeCryptographyClient();
    @SuppressWarnings("unchecked")
    Function<String, CryptographyClient> clientFactory = mock(Function.class);
    when(clientFactory.apply(VERSION_A)).thenReturn(client);
    AzureKmsAead encryptingAead = new AzureKmsAead(
        client, EncryptionAlgorithm.RSA_OAEP_256, fixedEncryptTarget(client, VERSION_A),
        clientFactory);
    byte[] ciphertext = encryptingAead.encrypt("hello".getBytes(StandardCharsets.UTF_8), new byte[0]);

    // A fresh Aead whose default client is unrelated; decrypt must still work by resolving the
    // embedded version through clientFactory rather than using the default client.
    CryptographyClient unrelatedDefaultClient = mock(CryptographyClient.class);
    AzureKmsAead decryptingAead = new AzureKmsAead(
        unrelatedDefaultClient, EncryptionAlgorithm.RSA_OAEP_256, null, clientFactory);

    byte[] plaintext = decryptingAead.decrypt(ciphertext, new byte[0]);

    assertArrayEquals("hello".getBytes(StandardCharsets.UTF_8), plaintext);
    verify(clientFactory).apply(VERSION_A);
  }

  @Test
  public void testAeadDecryptFallsBackToDefaultClientForLegacyCiphertext() throws Exception {
    CryptographyClient client = fakeCryptographyClient();
    AzureKmsAead encryptingAead = new AzureKmsAead(client, EncryptionAlgorithm.RSA_OAEP_256);
    byte[] legacyCiphertext =
        encryptingAead.encrypt("hello".getBytes(StandardCharsets.UTF_8), new byte[0]);

    // No clientFactory at all: must still work since the ciphertext carries no version prefix.
    AzureKmsAead decryptingAead = new AzureKmsAead(client, EncryptionAlgorithm.RSA_OAEP_256);
    byte[] plaintext = decryptingAead.decrypt(legacyCiphertext, new byte[0]);

    assertArrayEquals("hello".getBytes(StandardCharsets.UTF_8), plaintext);
  }

  @Test(expected = GeneralSecurityException.class)
  public void testAeadDecryptThrowsForPrefixedCiphertextWithNoClientFactory() throws Exception {
    CryptographyClient client = fakeCryptographyClient();
    AzureKmsAead encryptingAead = new AzureKmsAead(
        client, EncryptionAlgorithm.RSA_OAEP_256, fixedEncryptTarget(client, VERSION_A),
        version -> client);
    byte[] ciphertext = encryptingAead.encrypt("hello".getBytes(StandardCharsets.UTF_8), new byte[0]);

    AzureKmsAead decryptingAead = new AzureKmsAead(client, EncryptionAlgorithm.RSA_OAEP_256);
    decryptingAead.decrypt(ciphertext, new byte[0]);
  }

  // ==================== End-to-end produce/consume tests ====================

  private static final String AZURE_V1_PREFIX = "azure:v1:";

  // Mirrors AzureKmsAead's own parsing: only the fixed-width ASCII header is ever decoded as a
  // string; the remaining (possibly non-UTF-8) ciphertext bytes are never stringified.
  private static boolean startsWithAzureV1Prefix(byte[] encryptedKeyMaterial) {
    byte[] prefixBytes = AZURE_V1_PREFIX.getBytes(StandardCharsets.US_ASCII);
    if (encryptedKeyMaterial.length < prefixBytes.length) {
      return false;
    }
    for (int i = 0; i < prefixBytes.length; i++) {
      if (encryptedKeyMaterial[i] != prefixBytes[i]) {
        return false;
      }
    }
    return true;
  }

  private static String extractAzureV1Version(byte[] encryptedKeyMaterial) {
    if (!startsWithAzureV1Prefix(encryptedKeyMaterial)) {
      return null;
    }
    int prefixLength = AZURE_V1_PREFIX.length();
    if (encryptedKeyMaterial.length < prefixLength + 32 + 1
        || encryptedKeyMaterial[prefixLength + 32] != ':') {
      return null;
    }
    return new String(
        encryptedKeyMaterial, prefixLength, 32, StandardCharsets.US_ASCII);
  }

  @Test
  public void testKafkaAvroSerializerAzureKeyVersionSave() throws Exception {
    IndexedRecord avroRecord = createUserRecord();
    AvroSchema avroSchema = new AvroSchema(createUserSchema());
    Rule rule = new Rule("rule1", null, null, null,
        FieldEncryptionExecutor.TYPE, ImmutableSortedSet.of("PII"), null, null, null, null, false);
    RuleSet ruleSet = new RuleSet(Collections.emptyList(), ImmutableList.of(rule));
    Map<String, String> properties = new HashMap<>();
    properties.put(FieldEncryptionExecutor.ENCRYPT_KEK_NAME, "kek3");
    Metadata metadata = getMetadata(properties);
    avroSchema = avroSchema.copy(metadata, ruleSet);
    schemaRegistry.register(topic + "-value", avroSchema);

    // kek3 is pre-created with a versionless kms key id and the toggle enabled, simulating a
    // customer who does not want to hardcode a specific Azure key version.
    String versionlessKeyId = "https://yokota1.vault.azure.net/keys/key1";
    String resolvedKeyIdA = versionlessKeyId + "/" + VERSION_A;
    String resolvedKeyIdB = versionlessKeyId + "/" + VERSION_B;
    KeyVaultKey keyA = mock(KeyVaultKey.class);
    when(keyA.getId()).thenReturn(resolvedKeyIdA);
    KeyVaultKey keyB = mock(KeyVaultKey.class);
    when(keyB.getId()).thenReturn(resolvedKeyIdB);
    @SuppressWarnings("unchecked")
    Function<String, KeyVaultKey> keyResolver = mock(Function.class);
    // Simulates a rotation happening between the first and second dek creation: whatever is
    // "current" at the moment of each individual produce is what gets resolved and pinned.
    when(keyResolver.apply("key1")).thenReturn(keyA, keyB);

    Map<String, String> kmsProps = new HashMap<>();
    kmsProps.put(AzureKmsDriver.ENCRYPT_AZURE_KEY_VERSION_SAVE, "true");
    dekRegistry.createKek("kek3", "azure-kms", versionlessKeyId, kmsProps, null, false);

    Map<String, Object> props = fieldEncryptionProps.getClientProperties("mock://");
    props.put(AbstractKafkaSchemaSerDeConfig.RULE_EXECUTORS + ".rule1.param."
        + AzureKmsDriver.TEST_KEY_CLIENT, keyResolver);
    KafkaAvroSerializer serializer = new KafkaAvroSerializer(schemaRegistry, props);
    KafkaAvroDeserializer deserializer = new KafkaAvroDeserializer(schemaRegistry, props);

    RecordHeaders headers = new RecordHeaders();
    byte[] bytes = serializer.serialize(topic, headers, avroRecord);

    // The stored encryptedKeyMaterial must be self-describing with the exact resolved key
    // version that was actually used to wrap this dek, not the KEK's versionless configured id.
    Dek dek = dekRegistry.getDekLatestVersion("kek3", topic + "-value", null, false);
    String version = extractAzureV1Version(dek.getEncryptedKeyMaterialBytes());
    assertNotNull("expected encryptedKeyMaterial to carry an azure:v1: prefix", version);
    assertEquals(VERSION_A, version);
    verify(keyResolver, times(1)).apply("key1");

    GenericRecord record = (GenericRecord) deserializer.deserialize(topic, headers, bytes);
    assertEquals("testUser", record.get("name").toString());
    // Consuming must not re-resolve a version; it uses the version embedded in the prefix, so
    // the resolver call count must not have grown.
    verify(keyResolver, times(1)).apply("key1");

    // A second subject sharing the same (still-versionless) KEK, produced after "rotation" (the
    // resolver's second canned response), must independently pin its own dek to versionB, while
    // the first subject's already-created dek above remains tied to versionA.
    String topic2 = "test2";
    AvroSchema avroSchema2 = new AvroSchema(createUserSchema());
    avroSchema2 = avroSchema2.copy(metadata, ruleSet);
    schemaRegistry.register(topic2 + "-value", avroSchema2);
    serializer.serialize(topic2, headers, createUserRecord());

    Dek dek2 = dekRegistry.getDekLatestVersion("kek3", topic2 + "-value", null, false);
    String version2 = extractAzureV1Version(dek2.getEncryptedKeyMaterialBytes());
    assertNotNull(version2);
    assertEquals(VERSION_B, version2);

    // The first dek, wrapped before the simulated rotation, must still be independently readable.
    GenericRecord recordAgain = (GenericRecord) deserializer.deserialize(topic, headers, bytes);
    assertEquals("testUser", recordAgain.get("name").toString());
    verify(keyResolver, times(2)).apply("key1");
  }

  @Test
  public void testKafkaAvroSerializerAzureKeyVersionSaveDisabledByDefault() throws Exception {
    IndexedRecord avroRecord = createUserRecord();
    AvroSchema avroSchema = new AvroSchema(createUserSchema());
    Rule rule = new Rule("rule1", null, null, null,
        FieldEncryptionExecutor.TYPE, ImmutableSortedSet.of("PII"), null, null, null, null, false);
    RuleSet ruleSet = new RuleSet(Collections.emptyList(), ImmutableList.of(rule));
    Metadata metadata = getMetadata("kek1");
    avroSchema = avroSchema.copy(metadata, ruleSet);
    schemaRegistry.register(topic + "-value", avroSchema);

    RecordHeaders headers = new RecordHeaders();
    avroSerializer.serialize(topic, headers, avroRecord);

    Dek dek = dekRegistry.getDekLatestVersion("kek1", topic + "-value", null, false);
    assertEquals("no azure:v1: prefix should be written when the toggle is not set",
        null, extractAzureV1Version(dek.getEncryptedKeyMaterialBytes()));
  }
}
