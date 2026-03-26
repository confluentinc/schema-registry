/*
 * Copyright 2026 Confluent Inc.
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

package io.confluent.dekregistry.web.rest;

import com.google.common.testing.FakeTicker;
import com.google.crypto.tink.Aead;
import io.confluent.dekregistry.DekRegistryResourceExtension;
import io.confluent.dekregistry.client.CachedDekRegistryClient;
import io.confluent.dekregistry.client.rest.DekRegistryRestService;
import io.confluent.dekregistry.client.rest.entities.Dek;
import io.confluent.dekregistry.client.rest.entities.Kek;
import io.confluent.kafka.schemaregistry.ClusterTestHarness;
import io.confluent.kafka.schemaregistry.client.rest.Versions;
import io.confluent.kafka.schemaregistry.encryption.tink.Cryptor;
import io.confluent.kafka.schemaregistry.encryption.tink.DekFormat;
import io.confluent.kafka.schemaregistry.rest.SchemaRegistryConfig;
import org.junit.Before;
import org.junit.Test;

import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

/**
 * Integration tests to verify that X-Exporter-Name header is properly handled
 * across DEK Registry endpoints.
 */
public class RestApiExporterHeaderTest extends ClusterTestHarness {

  private CachedDekRegistryClient client;
  private Map<String, String> exporterHeaders;

  public RestApiExporterHeaderTest() {
    super(1, true);
  }

  @Override
  public Properties getSchemaRegistryProperties() throws Exception {
    Properties props = new Properties();
    props.put(
        SchemaRegistryConfig.RESOURCE_EXTENSION_CONFIG,
        DekRegistryResourceExtension.class.getName()
    );
    props.put(
        SchemaRegistryConfig.INTER_INSTANCE_HEADERS_WHITELIST_CONFIG,
        DekRegistryRestService.X_FORWARD_HEADER
    );
    return props;
  }

  @Before
  public void setUp() throws Exception {
    super.setUp();
    FakeTicker fakeTicker = new FakeTicker();
    client = new CachedDekRegistryClient(
        new DekRegistryRestService(restApp.restClient.getBaseUrls().urls()),
        1000,
        60,
        null,
        null,
        fakeTicker
    );

    exporterHeaders = new HashMap<>();
    exporterHeaders.put("Content-Type", Versions.SCHEMA_REGISTRY_V1_JSON_WEIGHTED);
    exporterHeaders.put("X-Exporter-Name", "test-exporter");
  }

  @Test
  public void testKekCreateWithExporterHeader() throws Exception {
    String kekName = "kek1";
    String kmsType = "test-kms";
    String kmsKeyId = "myid";

    Kek expectedKek = new Kek(kekName, kmsType, kmsKeyId, null, null, false, null, null);

    // Create KEK with exporter header
    Kek createdKek = client.createKek(
        exporterHeaders, kekName, kmsType, kmsKeyId, null, null, false, false);

    // Verify KEK was created successfully
    assertNotNull("Created KEK should not be null", createdKek);
    assertEquals(expectedKek, createdKek);
  }

  @Test
  public void testKekUpdateWithExporterHeader() throws Exception {
    String kekName = "kek1";
    String kmsType = "test-kms";
    String kmsKeyId = "myid";

    // Create KEK first
    Map<String, String> headers = new HashMap<>();
    headers.put("Content-Type", Versions.SCHEMA_REGISTRY_V1_JSON_WEIGHTED);
    client.createKek(headers, kekName, kmsType, kmsKeyId, null, null, false, false);

    // Update KEK with exporter header
    Kek updatedKek = client.updateKek(exporterHeaders, kekName, null, null, false);

    // Verify KEK was updated successfully
    assertNotNull("Updated KEK should not be null", updatedKek);
    assertEquals(kekName, updatedKek.getName());
  }

  @Test
  public void testKekDeleteWithExporterHeader() throws Exception {
    String kekName = "kek1";
    String kmsType = "test-kms";
    String kmsKeyId = "myid";

    // Create KEK first
    Map<String, String> headers = new HashMap<>();
    headers.put("Content-Type", Versions.SCHEMA_REGISTRY_V1_JSON_WEIGHTED);
    client.createKek(headers, kekName, kmsType, kmsKeyId, null, null, false, false);

    // Soft delete first, then hard delete with exporter header
    client.deleteKek(headers, kekName, false);  // Soft delete
    client.deleteKek(exporterHeaders, kekName, true); // Hard delete with header

    // Verify KEK was deleted
    List<String> keks = client.listKeks(true);  // Check deleted
    assertEquals("KEK list should be empty", 0, keks.size());
  }

  @Test
  public void testKekUndeleteWithExporterHeader() throws Exception {
    String kekName = "kek1";
    String kmsType = "test-kms";
    String kmsKeyId = "myid";

    // Create and soft-delete KEK first
    Map<String, String> headers = new HashMap<>();
    headers.put("Content-Type", Versions.SCHEMA_REGISTRY_V1_JSON_WEIGHTED);
    client.createKek(headers, kekName, kmsType, kmsKeyId, null, null, false, false);
    client.deleteKek(headers, kekName, false); // Soft delete

    // Undelete KEK with exporter header
    client.undeleteKek(exporterHeaders, kekName);

    // Verify KEK was undeleted by fetching it
    Kek undeletedKek = client.getKek(kekName, false);
    assertNotNull("Undeleted KEK should not be null", undeletedKek);
    assertEquals(kekName, undeletedKek.getName());
  }

  @Test
  public void testDekCreateWithExporterHeader() throws Exception {
    String kekName = "kek1";
    String kmsType = "test-kms";
    String kmsKeyId = "myid";
    String subject = "mysubject";
    DekFormat algorithm = DekFormat.AES256_GCM;

    // Create KEK first
    Map<String, String> headers = new HashMap<>();
    headers.put("Content-Type", Versions.SCHEMA_REGISTRY_V1_JSON_WEIGHTED);
    Kek kek = client.createKek(headers, kekName, kmsType, kmsKeyId, null, null, false, false);

    // Generate encrypted key material
    byte[] rawDek = new Cryptor(algorithm).generateKey();
    Aead aead = kek.toAead(Collections.emptyMap());
    byte[] encryptedDek = aead.encrypt(rawDek, new byte[0]);
    String encryptedDekStr =
        new String(Base64.getEncoder().encode(encryptedDek), StandardCharsets.UTF_8);

    // Create DEK with exporter header
    Dek createdDek = client.createDek(
        exporterHeaders, kekName, subject, null, algorithm, encryptedDekStr, false);

    // Verify DEK was created successfully
    assertNotNull("Created DEK should not be null", createdDek);
    assertEquals(kekName, createdDek.getKekName());
    assertEquals(subject, createdDek.getSubject());
    assertEquals(1, createdDek.getVersion());
  }

  @Test
  public void testDekDeleteWithExporterHeader() throws Exception {
    String kekName = "kek1";
    String kmsType = "test-kms";
    String kmsKeyId = "myid";
    String subject = "mysubject";
    DekFormat algorithm = DekFormat.AES256_GCM;

    // Create KEK and generate encrypted key material
    Map<String, String> headers = new HashMap<>();
    headers.put("Content-Type", Versions.SCHEMA_REGISTRY_V1_JSON_WEIGHTED);
    Kek kek = client.createKek(headers, kekName, kmsType, kmsKeyId, null, null, false, false);

    byte[] rawDek = new Cryptor(algorithm).generateKey();
    Aead aead = kek.toAead(Collections.emptyMap());
    byte[] encryptedDek = aead.encrypt(rawDek, new byte[0]);
    String encryptedDekStr =
        new String(Base64.getEncoder().encode(encryptedDek), StandardCharsets.UTF_8);

    client.createDek(headers, kekName, subject, null, algorithm, encryptedDekStr, false);

    // Soft delete first, then hard delete with exporter header
    client.deleteDek(headers, kekName, subject, algorithm, false);  // Soft delete
    client.deleteDek(exporterHeaders, kekName, subject, algorithm, true); // Hard delete with header

    // Verify DEK was deleted by listing
    List<String> deks = client.listDeks(kekName, true);  // Check deleted
    assertEquals("DEK list should be empty", 0, deks.size());
  }

  @Test
  public void testDekDeleteVersionWithExporterHeader() throws Exception {
    String kekName = "kek1";
    String kmsType = "test-kms";
    String kmsKeyId = "myid";
    String subject = "mysubject";
    int version = 1;
    DekFormat algorithm = DekFormat.AES256_GCM;

    // Create KEK and generate encrypted key material
    Map<String, String> headers = new HashMap<>();
    headers.put("Content-Type", Versions.SCHEMA_REGISTRY_V1_JSON_WEIGHTED);
    Kek kek = client.createKek(headers, kekName, kmsType, kmsKeyId, null, null, false, false);

    byte[] rawDek = new Cryptor(algorithm).generateKey();
    Aead aead = kek.toAead(Collections.emptyMap());
    byte[] encryptedDek = aead.encrypt(rawDek, new byte[0]);
    String encryptedDekStr =
        new String(Base64.getEncoder().encode(encryptedDek), StandardCharsets.UTF_8);

    client.createDek(headers, kekName, subject, version, algorithm, encryptedDekStr, false);

    // Soft delete first, then hard delete with exporter header
    client.deleteDekVersion(headers, kekName, subject, version, algorithm, false);  // Soft delete
    client.deleteDekVersion(exporterHeaders, kekName, subject, version, algorithm, true); // Hard delete with header

    // Verify DEK version was deleted by listing
    List<String> deks = client.listDeks(kekName, true);  // Check deleted
    assertEquals("DEK list should be empty", 0, deks.size());
  }

  @Test
  public void testDekUndeleteWithExporterHeader() throws Exception {
    String kekName = "kek1";
    String kmsType = "test-kms";
    String kmsKeyId = "myid";
    String subject = "mysubject";
    DekFormat algorithm = DekFormat.AES256_GCM;

    // Create KEK and generate encrypted key material
    Map<String, String> headers = new HashMap<>();
    headers.put("Content-Type", Versions.SCHEMA_REGISTRY_V1_JSON_WEIGHTED);
    Kek kek = client.createKek(headers, kekName, kmsType, kmsKeyId, null, null, false, false);

    byte[] rawDek = new Cryptor(algorithm).generateKey();
    Aead aead = kek.toAead(Collections.emptyMap());
    byte[] encryptedDek = aead.encrypt(rawDek, new byte[0]);
    String encryptedDekStr =
        new String(Base64.getEncoder().encode(encryptedDek), StandardCharsets.UTF_8);

    client.createDek(headers, kekName, subject, null, algorithm, encryptedDekStr, false);
    client.deleteDek(headers, kekName, subject, algorithm, false); // Soft delete

    // Undelete DEK with exporter header
    client.undeleteDek(exporterHeaders, kekName, subject, algorithm);

    // Verify DEK was undeleted by fetching it
    Dek undeletedDek = client.getDek(kekName, subject, algorithm, false);
    assertNotNull("Undeleted DEK should not be null", undeletedDek);
    assertEquals(kekName, undeletedDek.getKekName());
    assertEquals(subject, undeletedDek.getSubject());
  }

  @Test
  public void testDekUndeleteVersionWithExporterHeader() throws Exception {
    String kekName = "kek1";
    String kmsType = "test-kms";
    String kmsKeyId = "myid";
    String subject = "mysubject";
    int version = 1;
    DekFormat algorithm = DekFormat.AES256_GCM;

    // Create KEK and generate encrypted key material
    Map<String, String> headers = new HashMap<>();
    headers.put("Content-Type", Versions.SCHEMA_REGISTRY_V1_JSON_WEIGHTED);
    Kek kek = client.createKek(headers, kekName, kmsType, kmsKeyId, null, null, false, false);

    byte[] rawDek = new Cryptor(algorithm).generateKey();
    Aead aead = kek.toAead(Collections.emptyMap());
    byte[] encryptedDek = aead.encrypt(rawDek, new byte[0]);
    String encryptedDekStr =
        new String(Base64.getEncoder().encode(encryptedDek), StandardCharsets.UTF_8);

    client.createDek(headers, kekName, subject, version, algorithm, encryptedDekStr, false);
    client.deleteDekVersion(headers, kekName, subject, version, algorithm, false);

    // Undelete specific DEK version with exporter header
    client.undeleteDekVersion(exporterHeaders, kekName, subject, version, algorithm);

    // Verify DEK version was undeleted by fetching it
    Dek undeletedDek = client.getDek(kekName, subject, algorithm, false);
    assertNotNull("Undeleted DEK should not be null", undeletedDek);
    assertEquals(kekName, undeletedDek.getKekName());
    assertEquals(subject, undeletedDek.getSubject());
    assertEquals(version, undeletedDek.getVersion());
  }

  @Test
  public void testOperationsWithoutExporterHeader() throws Exception {
    String kekName = "kek1";
    String kmsType = "test-kms";
    String kmsKeyId = "myid";

    // Test without exporter header - should still work
    Map<String, String> headers = new HashMap<>();
    headers.put("Content-Type", Versions.SCHEMA_REGISTRY_V1_JSON_WEIGHTED);

    Kek expectedKek = new Kek(kekName, kmsType, kmsKeyId, null, null, false, null, null);

    // Create KEK without exporter header
    Kek createdKek = client.createKek(
        headers, kekName, kmsType, kmsKeyId, null, null, false, false);

    // Verify KEK was created successfully
    assertNotNull("Created KEK should not be null", createdKek);
    assertEquals(expectedKek, createdKek);
  }
}
