/*
 * Copyright 2025 Confluent Inc.
 *
 * Licensed under the Confluent Community License (the "License"); you may not use
 * this file except in compliance with the License.  You may obtain a copy of the
 * License at
 *
 * http://www.confluent.io/confluent-community-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OF ANY KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations under the License.
 */

package io.confluent.dekregistry.storage;

import com.google.crypto.tink.Aead;
import com.google.crypto.tink.KmsClient;
import io.confluent.dekregistry.client.rest.entities.CreateDekRequest;
import io.confluent.dekregistry.client.rest.entities.CreateKekRequest;
import io.confluent.dekregistry.client.rest.entities.KeyType;
import io.confluent.dekregistry.client.rest.entities.UpdateKekRequest;
import io.confluent.dekregistry.metrics.MetricsManager;
import io.confluent.dekregistry.testutil.TestKmsDriver;
import io.confluent.kafka.schemaregistry.ClusterTestHarness;
import io.confluent.kafka.schemaregistry.encryption.tink.DekFormat;
import io.confluent.kafka.schemaregistry.exceptions.SchemaRegistryException;
import io.confluent.kafka.schemaregistry.rest.SchemaRegistryConfig;
import io.confluent.kafka.schemaregistry.storage.KafkaSchemaRegistry;
import io.confluent.kafka.schemaregistry.storage.serialization.SchemaRegistrySerializer;
import io.kcache.KeyValue;

import java.util.*;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.*;

public class DekRegistryTest extends ClusterTestHarness {

    private KafkaSchemaRegistry schemaRegistry;

    private MetricsManager metricsManager;

    private DekRegistry dekRegistry;

    private KeyEncryptionKey kek;

    @Before
    public void setUp() throws Exception {
        super.setUp();
        Properties props = new Properties();
        props.put(SchemaRegistryConfig.KAFKASTORE_BOOTSTRAP_SERVERS_CONFIG, brokerList);
        props.put(SchemaRegistryConfig.KAFKASTORE_TOPIC_CONFIG, ClusterTestHarness.KAFKASTORE_TOPIC);
        props.put(SchemaRegistryConfig.SSL_ENDPOINT_IDENTIFICATION_ALGORITHM_CONFIG, "");
        SchemaRegistryConfig config = new SchemaRegistryConfig(props);
        schemaRegistry = new KafkaSchemaRegistry(config, new SchemaRegistrySerializer());
        metricsManager = new MetricsManager(schemaRegistry);
        dekRegistry = new DekRegistry(schemaRegistry, metricsManager);
        dekRegistry.init();

        CreateKekRequest request = CreateKekRequest.fromJson("{\"name\": \"kekName1\", \"kmsType\": \"test-kms\", \"kmsKeyId\": \"kmsId\", \"kmsProps\": {\"property1\": \"value1\", \"property2\": \"value2\"}, \"doc\": \"Test Documentation\", \"shared\": true, \"deleted\": false}");
        kek = dekRegistry.createKek(request);
        TestKmsDriver t = new TestKmsDriver();
        KmsClient client = t.newKmsClient(null, Optional.of("test-kms://kmsId"));
        Aead aead = client.getAead("test-kms://kmsId");
        byte[] encryptedDek = aead.encrypt("rawDek1".getBytes(), DekRegistry.EMPTY_AAD);
        String encryptedKeyMaterial = Base64.getEncoder().encodeToString(encryptedDek);
        CreateDekRequest dekRequest = CreateDekRequest.fromJson(String.format("{\"subject\": \"subject1\", \"version\": \"2\", \"algorithm\": \"AES256_GCM\", \"encryptedKeyMaterial\": \"%s\", \"deleted\": true}", encryptedKeyMaterial));
        dekRegistry.createDek(kek.getName(), dekRequest);
    }

    @Test
    public void testCreateKek() throws Exception {
        assertNotNull(kek);
        assertFalse(kek.isDeleted());
        assertEquals("kekName1", kek.getName());
        assertEquals("test-kms", kek.getKmsType());
        assertEquals("kmsId", kek.getKmsKeyId());
        assertEquals("value1", kek.getKmsProps().get("property1"));
        assertEquals("value2", kek.getKmsProps().get("property2"));
        assertEquals("Test Documentation", kek.getDoc());
    }

    @Test
    public void testGetKekNames() throws Exception {
        CreateKekRequest kRequest2 = CreateKekRequest.fromJson("{\"name\": \"kekName2\", \"kmsType\": \"test-kms\", \"kmsKeyId\": \"kmsId\", \"kmsProps\": {\"property1\": \"value1\", \"property2\": \"value2\"}, \"doc\": \"Test Documentation\", \"shared\": true, \"deleted\": true}");
        KeyEncryptionKey kek2 = dekRegistry.createKek(kRequest2);
        // Test get without a subject prefix.
        // Includes deleted ones.
        assertEquals(Arrays.asList("kekName1", "kekName2"), dekRegistry.getKekNames(null, true));
        // Ignores deleted ones.
        assertEquals(Collections.singletonList("kekName1"), dekRegistry.getKekNames(Collections.emptyList(), false));

        // Test get with a non-existing subject prefix.
        assertEquals(Collections.emptyList(), dekRegistry.getKekNames(Collections.singletonList("non_exist_prefix"), false));

        // Test get by subject prefix with Dek.
        TestKmsDriver t = new TestKmsDriver();
        KmsClient client = t.newKmsClient(null, Optional.of("test-kms://kmsId"));
        Aead aead = client.getAead("test-kms://kmsId");
        byte[] encryptedDek = aead.encrypt("rawDek2".getBytes(), DekRegistry.EMPTY_AAD);
        String encryptedKeyMaterial = Base64.getEncoder().encodeToString(encryptedDek);
        CreateDekRequest dekRequest = CreateDekRequest.fromJson(String.format("{\"subject\": \"subject2\", \"version\": \"2\", \"algorithm\": \"AES256_GCM\", \"encryptedKeyMaterial\": \"%s\", \"deleted\": false}", encryptedKeyMaterial)
        );
        dekRegistry.createDek(kek2.getName(), dekRequest);
        assertEquals(Arrays.asList("kekName1", "kekName2"), dekRegistry.getKekNames(Collections.singletonList("sub"), true));
        assertEquals(Collections.singletonList("kekName1"), dekRegistry.getKekNames(Collections.singletonList("subject1"), true));
        assertEquals(Collections.emptyList(), dekRegistry.getKekNames(Collections.singletonList("subject1"), false));
    }

    @Test
    public void testDeleteKek() throws SchemaRegistryException  {
        // First soft delete.
        dekRegistry.deleteKek("kekName1", false);
        kek = dekRegistry.getKek("kekName1",false);
        assertNull(kek);

        // Then undelete it.
        dekRegistry.undeleteKek("kekName1");
        kek = dekRegistry.getKek("kekName1",false);
        assertNotNull(kek);
        assertFalse(kek.isDeleted());
        assertEquals("kekName1", kek.getName());
        assertEquals("test-kms", kek.getKmsType());
        assertEquals("kmsId", kek.getKmsKeyId());
        assertEquals("value1", kek.getKmsProps().get("property1"));
        assertEquals("value2", kek.getKmsProps().get("property2"));
        assertEquals("Test Documentation", kek.getDoc());
        assertFalse(kek.isDeleted());
    }

    @Test
    public void testClearKekDoc() throws SchemaRegistryException  {
        kek = dekRegistry.getKek("kekName1",false);

        UpdateKekRequest request = new UpdateKekRequest();
        request.setDoc(Optional.empty());

        kek = dekRegistry.putKek("kekName1", request);
        assertNotNull(kek);
        assertFalse(kek.isDeleted());
        assertEquals("kekName1", kek.getName());
        assertEquals("test-kms", kek.getKmsType());
        assertEquals("kmsId", kek.getKmsKeyId());
        assertEquals("value1", kek.getKmsProps().get("property1"));
        assertEquals("value2", kek.getKmsProps().get("property2"));
        assertNull(kek.getDoc());

        request = new UpdateKekRequest();
        request.setDoc(Optional.of("Test Documentation"));

        kek = dekRegistry.putKek("kekName1", request);
        assertNotNull(kek);
        assertFalse(kek.isDeleted());
        assertEquals("kekName1", kek.getName());
        assertEquals("test-kms", kek.getKmsType());
        assertEquals("kmsId", kek.getKmsKeyId());
        assertEquals("value1", kek.getKmsProps().get("property1"));
        assertEquals("value2", kek.getKmsProps().get("property2"));
        assertEquals("Test Documentation", kek.getDoc());
    }

    @Test
    public void testGetKeks() throws Exception {
        List<KeyValue<EncryptionKeyId, EncryptionKey>> keks = dekRegistry.getKeks(schemaRegistry.tenant(), true);
        assertEquals(1, keks.size());
        KeyValue<EncryptionKeyId, EncryptionKey> kv = keks.get(0);
        assertEquals(schemaRegistry.tenant(), kv.key.getTenant());
        assertEquals(KeyType.KEK, kv.key.getType());
        assertEquals(KeyType.KEK, kv.value.getType());
        assertEquals(0, (long)kv.value.getOffset());
    }

    @Test
    public void testGetDekSubjects() throws Exception {
        List<String> subjects = dekRegistry.getDekSubjects("kekName1", true);
        assertEquals(1, subjects.size());
        String subject = subjects.get(0);
        assertEquals("subject1", subject);
    }

    @Test
    public void testGetDekVersions() {
        List<Integer> versions = dekRegistry.getDekVersions("kekName1", "subject1", DekFormat.AES256_GCM, true);
        assertEquals(1, versions.size());
        assertEquals(2, (int)versions.get(0));
    }

    @Test
    public void testGetDek() throws SchemaRegistryException  {
        DataEncryptionKey dek = dekRegistry.getDek("kekName1", "subject1", 2, DekFormat.AES256_GCM, true);
        assertEquals(2, dek.getVersion());
        assertEquals(DekFormat.AES256_GCM, dek.getAlgorithm());
        assertEquals("subject1", dek.getSubject());
        assertEquals("kekName1", dek.getKekName());
        assertEquals(KeyType.DEK, dek.getType());
        // Key material should be populated since KEK is shared.
        assertNotNull(dek.getKeyMaterial());
    }

    @Test
    public void testGetDeks() throws SchemaRegistryException  {
        List<KeyValue<EncryptionKeyId, EncryptionKey>> deks = dekRegistry.getDeks(schemaRegistry.tenant(), true);
        assertEquals(1, deks.size());
        KeyValue<EncryptionKeyId, EncryptionKey> kv = deks.get(0);
        assertEquals(schemaRegistry.tenant(), kv.key.getTenant());
        assertEquals(KeyType.DEK, kv.key.getType());
        assertEquals(KeyType.DEK, kv.value.getType());
    }

    @Test
    public void testGetLatestDek() throws SchemaRegistryException  {
        DataEncryptionKey dek = dekRegistry.getLatestDek("kekName1", "subject1", DekFormat.AES256_GCM, true);
        assertEquals(DekFormat.AES256_GCM, dek.getAlgorithm());
        assertEquals("subject1", dek.getSubject());
        assertEquals("kekName1", dek.getKekName());
        assertEquals(KeyType.DEK, dek.getType());
        assertEquals(2, dek.getVersion());
        // Key material should be populated since KEK is shared.
        assertNotNull(dek.getKeyMaterial());
    }

    @Test
    public void testDeleteDek() throws SchemaRegistryException  {
        // First soft delete.
        dekRegistry.deleteDek("kekName1", "subject1", DekFormat.AES256_GCM, false);
        DataEncryptionKey dek = dekRegistry.getDek("kekName1", "subject1", 2, DekFormat.AES256_GCM, false);
        assertNull(dek);

        // Then undelete it.
        dekRegistry.undeleteDek("kekName1", "subject1", DekFormat.AES256_GCM);
        dek = dekRegistry.getDek("kekName1", "subject1", 2, DekFormat.AES256_GCM, false);
        assertNotNull(dek);
        assertFalse(dek.isDeleted());
        assertEquals(DekFormat.AES256_GCM, dek.getAlgorithm());
        assertEquals("subject1", dek.getSubject());
        assertEquals("kekName1", dek.getKekName());
        assertEquals(KeyType.DEK, dek.getType());
        assertEquals(2, dek.getVersion());

        // Hard delete.
        dekRegistry.deleteDek("kekName1", "subject1", DekFormat.AES256_GCM, false);
        dekRegistry.deleteDek("kekName1", "subject1", DekFormat.AES256_GCM, true);
        dek = dekRegistry.getDek("kekName1", "subject1", 2, DekFormat.AES256_GCM, false);
        assertNull(dek);
    }

    @Test
    public void testDeleteDekVersion() throws SchemaRegistryException  {
        // First soft delete the version.
        dekRegistry.deleteDekVersion("kekName1", "subject1", 2, DekFormat.AES256_GCM,false);
        DataEncryptionKey dek = dekRegistry.getDek("kekName1", "subject1", 2, DekFormat.AES256_GCM, false);
        assertNull(dek);

        // Then undelete it.
        dekRegistry.undeleteDekVersion("kekName1", "subject1", 2, DekFormat.AES256_GCM);
        dek = dekRegistry.getDek("kekName1", "subject1", 2, DekFormat.AES256_GCM, false);
        assertNotNull(dek);
        assertFalse(dek.isDeleted());
        assertEquals(DekFormat.AES256_GCM, dek.getAlgorithm());
        assertEquals("subject1", dek.getSubject());
        assertEquals("kekName1", dek.getKekName());
        assertEquals(KeyType.DEK, dek.getType());
        assertEquals(2, dek.getVersion());
    }
}
