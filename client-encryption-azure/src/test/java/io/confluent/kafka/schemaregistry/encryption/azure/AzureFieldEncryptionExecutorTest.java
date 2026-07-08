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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.azure.core.exception.HttpResponseException;
import com.azure.core.http.HttpResponse;
import com.azure.security.keyvault.keys.models.KeyVaultKey;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSortedSet;
import io.confluent.dekregistry.client.rest.entities.Dek;
import io.confluent.kafka.schemaregistry.avro.AvroSchema;
import io.confluent.kafka.schemaregistry.client.rest.entities.Metadata;
import io.confluent.kafka.schemaregistry.client.rest.entities.Rule;
import io.confluent.kafka.schemaregistry.client.rest.entities.RuleSet;
import io.confluent.kafka.schemaregistry.encryption.EncryptionExecutor;
import io.confluent.kafka.schemaregistry.encryption.FieldEncryptionExecutor;
import io.confluent.kafka.schemaregistry.encryption.FieldEncryptionExecutorTest;
import io.confluent.kafka.schemaregistry.encryption.EncryptionProperties;
import io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig;
import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
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
    AzureKmsDriver driver = new AzureKmsDriver();
    String versionedKeyId = "https://yokota1.vault.azure.net/keys/key1/1234567890";
    @SuppressWarnings("unchecked")
    Function<String, KeyVaultKey> keyResolver = mock(Function.class);
    Map<String, Object> configs = new HashMap<>();
    configs.put(AzureKmsDriver.TEST_KEY_CLIENT, keyResolver);

    String result = driver.getVersionedKeyId(configs, versionedKeyId);

    assertEquals(versionedKeyId, result);
    verify(keyResolver, never()).apply(any());
  }

  @Test
  public void testGetVersionedKeyIdResolvesVersionlessKeyId() throws Exception {
    AzureKmsDriver driver = new AzureKmsDriver();
    String versionlessKeyId = "https://yokota1.vault.azure.net/keys/key1";
    String resolvedKeyId = "https://yokota1.vault.azure.net/keys/key1/resolvedVersion";
    KeyVaultKey keyVaultKey = mock(KeyVaultKey.class);
    when(keyVaultKey.getId()).thenReturn(resolvedKeyId);
    @SuppressWarnings("unchecked")
    Function<String, KeyVaultKey> keyResolver = mock(Function.class);
    when(keyResolver.apply("key1")).thenReturn(keyVaultKey);
    Map<String, Object> configs = new HashMap<>();
    configs.put(AzureKmsDriver.TEST_KEY_CLIENT, keyResolver);

    String result = driver.getVersionedKeyId(configs, versionlessKeyId);

    assertEquals(resolvedKeyId, result);
    verify(keyResolver).apply("key1");
  }

  @Test(expected = GeneralSecurityException.class)
  public void testGetVersionedKeyIdThrowsForMalformedKeyId() throws Exception {
    AzureKmsDriver driver = new AzureKmsDriver();
    Map<String, Object> configs = new HashMap<>();
    driver.getVersionedKeyId(configs, "https://yokota1.vault.azure.net/notkeys/key1");
  }

  @Test(expected = GeneralSecurityException.class)
  public void testGetVersionedKeyIdThrowsForInvalidUri() throws Exception {
    AzureKmsDriver driver = new AzureKmsDriver();
    Map<String, Object> configs = new HashMap<>();
    driver.getVersionedKeyId(configs, "::not a uri::");
  }

  @Test
  public void testKafkaAvroSerializerKmsKeyIdSave() throws Exception {
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
    String resolvedKeyIdA = "https://yokota1.vault.azure.net/keys/key1/versionA";
    String resolvedKeyIdB = "https://yokota1.vault.azure.net/keys/key1/versionB";
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
    kmsProps.put(EncryptionExecutor.ENCRYPT_KMS_KEY_ID_SAVE, "true");
    dekRegistry.createKek("kek3", "azure-kms", versionlessKeyId, kmsProps, null, false);

    Map<String, Object> props = fieldEncryptionProps.getClientProperties("mock://");
    props.put(AbstractKafkaSchemaSerDeConfig.RULE_EXECUTORS + ".rule1.param."
        + AzureKmsDriver.TEST_KEY_CLIENT, keyResolver);
    KafkaAvroSerializer serializer = new KafkaAvroSerializer(schemaRegistry, props);
    KafkaAvroDeserializer deserializer = new KafkaAvroDeserializer(schemaRegistry, props);

    RecordHeaders headers = new RecordHeaders();
    byte[] bytes = serializer.serialize(topic, headers, avroRecord);

    // The stored encryptedKeyMaterial must be self-describing with the exact resolved kms key id
    // that was actually used to wrap this dek, not the KEK's versionless configured id.
    Dek dek = dekRegistry.getDekLatestVersion("kek3", topic + "-value", null, false);
    Map.Entry<String, byte[]> parsed =
        EncryptionExecutor.extractKmsKeyId(dek.getEncryptedKeyMaterialBytes());
    assertNotNull("expected encryptedKeyMaterial to carry a kms key id prefix", parsed);
    assertEquals(resolvedKeyIdA, parsed.getKey());
    verify(keyResolver, times(1)).apply("key1");

    GenericRecord record = (GenericRecord) deserializer.deserialize(topic, headers, bytes);
    assertEquals("testUser", record.get("name").toString());
    // Consuming must not re-resolve a version; it uses the id embedded in the prefix, so the
    // resolver call count must not have grown.
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
    Map.Entry<String, byte[]> parsed2 =
        EncryptionExecutor.extractKmsKeyId(dek2.getEncryptedKeyMaterialBytes());
    assertNotNull(parsed2);
    assertEquals(resolvedKeyIdB, parsed2.getKey());

    // The first dek, wrapped before the simulated rotation, must still be independently readable.
    GenericRecord recordAgain = (GenericRecord) deserializer.deserialize(topic, headers, bytes);
    assertEquals("testUser", recordAgain.get("name").toString());
    verify(keyResolver, times(2)).apply("key1");
  }

  @Test
  public void testKafkaAvroSerializerKmsKeyIdSaveDisabledByDefault() throws Exception {
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
    assertNull("no kms key id prefix should be written when the toggle is not set",
        EncryptionExecutor.extractKmsKeyId(dek.getEncryptedKeyMaterialBytes()));
  }
}

