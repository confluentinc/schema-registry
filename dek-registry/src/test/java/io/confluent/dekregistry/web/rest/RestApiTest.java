/*
 * Copyright 2023 Confluent Inc.
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

import static io.confluent.dekregistry.storage.DekRegistry.X_FORWARD_HEADER;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.fail;

import com.google.common.collect.ImmutableList;
import com.google.common.testing.FakeTicker;
import com.google.crypto.tink.Aead;
import io.confluent.dekregistry.DekRegistryResourceExtension;
import io.confluent.dekregistry.client.CachedDekRegistryClient;
import io.confluent.dekregistry.client.rest.DekRegistryRestService;
import io.confluent.dekregistry.client.rest.entities.Dek;
import io.confluent.dekregistry.client.rest.entities.Kek;
import io.confluent.dekregistry.web.rest.exceptions.DekRegistryErrors;
import io.confluent.kafka.schemaregistry.ClusterTestHarness;
import io.confluent.kafka.schemaregistry.ParsedSchema;
import io.confluent.kafka.schemaregistry.avro.AvroUtils;
import io.confluent.kafka.schemaregistry.client.rest.Versions;
import io.confluent.kafka.schemaregistry.client.rest.entities.Rule;
import io.confluent.kafka.schemaregistry.client.rest.entities.RuleMode;
import io.confluent.kafka.schemaregistry.client.rest.entities.RuleSet;
import io.confluent.kafka.schemaregistry.client.rest.entities.SchemaString;
import io.confluent.kafka.schemaregistry.client.rest.entities.requests.ConfigUpdateRequest;
import io.confluent.kafka.schemaregistry.client.rest.entities.requests.RegisterSchemaRequest;
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;
import io.confluent.kafka.schemaregistry.encryption.tink.Cryptor;
import io.confluent.kafka.schemaregistry.encryption.tink.DekFormat;
import io.confluent.kafka.schemaregistry.rest.SchemaRegistryConfig;
import io.confluent.kafka.schemaregistry.storage.KafkaSchemaRegistry;
import io.confluent.kafka.schemaregistry.storage.Mode;
import io.confluent.kafka.schemaregistry.storage.RuleSetHandler;
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import org.junit.Before;
import org.junit.Test;

public class RestApiTest extends ClusterTestHarness {

  FakeTicker fakeTicker;
  CachedDekRegistryClient client;

  public RestApiTest() {
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
    fakeTicker = new FakeTicker();
    client = new CachedDekRegistryClient(
        new DekRegistryRestService(restApp.restClient.getBaseUrls().urls()),
        1000,
        60,
        null,
        null,
        fakeTicker
    );
    ((KafkaSchemaRegistry) restApp.schemaRegistry()).setRuleSetHandler(new RuleSetHandler() {
      public void handle(String subject, ConfigUpdateRequest request) {
      }

      public void handle(String subject, boolean normalize, RegisterSchemaRequest request) {
      }

      public io.confluent.kafka.schemaregistry.storage.RuleSet transform(RuleSet ruleSet) {
        return ruleSet != null
            ? new io.confluent.kafka.schemaregistry.storage.RuleSet(ruleSet)
            : null;
      }
    });
  }

  @Test
  public void testBasic() throws Exception {
    Map<String, String> headers = new HashMap<>();
    headers.put("Content-Type", Versions.SCHEMA_REGISTRY_V1_JSON_WEIGHTED);
    testBasic(headers, false);
  }

  @Test
  public void testForwarding() throws Exception {
    Map<String, String> headers = new HashMap<>();
    headers.put("Content-Type", Versions.SCHEMA_REGISTRY_V1_JSON_WEIGHTED);
    headers.put(X_FORWARD_HEADER, "false");
    testBasic(headers, false);
  }

  @Test
  public void testBasicImport() throws Exception {
    Map<String, String> headers = new HashMap<>();
    headers.put("Content-Type", Versions.SCHEMA_REGISTRY_V1_JSON_WEIGHTED);
    testBasic(headers, true);
  }

  private void testBasic(Map<String, String> headers, boolean isImport) throws Exception {
    String kekName = "kek1";
    String kmsType = "test-kms";
    String kmsKeyId = "myid";
    String subject = "mysubject";
    String badSubject = "badSubject";
    String subject2 = "mysubject2";
    DekFormat algorithm = DekFormat.AES256_GCM;
    Kek kek = new Kek(kekName, kmsType, kmsKeyId, null, null, false, null, null);

    if (isImport) {
      client.setMode("IMPORT");
    }

    // Create kek
    Kek newKek = client.createKek(headers, kekName, kmsType, kmsKeyId, null, null, false, false);
    assertEquals(kek, newKek);

    // Delete kek
    client.deleteKek(headers, kekName, false);

    // Allow create to act like undelete
    newKek = client.createKek(headers, kekName, kmsType, kmsKeyId, null, null, false, false);
    assertEquals(kek, newKek);

    newKek = client.getKek(kekName, false);
    assertEquals(kek, newKek);

    List<String> keks = client.listKeks(false);
    assertEquals(Collections.singletonList(kekName), keks);

    try {
      client.deleteKek(headers, kekName, true);
      fail();
    } catch (RestClientException e) {
      assertEquals(DekRegistryErrors.KEY_NOT_SOFT_DELETED_ERROR_CODE, e.getErrorCode());
    }

    // Delete kek
    client.deleteKek(headers, kekName, false);

    Map<String, String> kmsProps = Collections.singletonMap("hi", "there");
    String doc = "mydoc";
    try {
      client.updateKek(headers, kekName, kmsProps, doc, true);
      fail();
    } catch (RestClientException e) {
      assertEquals(DekRegistryErrors.KEY_NOT_FOUND_ERROR_CODE, e.getErrorCode());
    }

    keks = client.listKeks(false);
    assertEquals(Collections.emptyList(), keks);

    keks = client.listKeks(true);
    assertEquals(Collections.singletonList(kekName), keks);

    try {
      client.getKek(kekName, false);
      fail();
    } catch (RestClientException e) {
      assertEquals(DekRegistryErrors.KEY_NOT_FOUND_ERROR_CODE, e.getErrorCode());
    }

    newKek = client.getKek(kekName, true);
    assertEquals(kek, newKek);

    client.deleteKek(headers, kekName, true);

    try {
      client.getKek(kekName, false);
      fail();
    } catch (RestClientException e) {
      assertEquals(DekRegistryErrors.KEY_NOT_FOUND_ERROR_CODE, e.getErrorCode());
    }

    try {
      client.getKek(kekName, true);
      fail();
    } catch (RestClientException e) {
      assertEquals(DekRegistryErrors.KEY_NOT_FOUND_ERROR_CODE, e.getErrorCode());
    }

    keks = client.listKeks(false);
    assertEquals(Collections.emptyList(), keks);

    keks = client.listKeks(true);
    assertEquals(Collections.emptyList(), keks);

    // Recreate kek
    newKek = client.createKek(headers, kekName, kmsType, kmsKeyId, null, null, false, false);
    assertEquals(kek, newKek);

    newKek = client.getKek(kekName, false);
    assertEquals(kek, newKek);

    byte[] rawDek = new Cryptor(algorithm).generateKey();
    String rawDekStr =
        new String(Base64.getEncoder().encode(rawDek), StandardCharsets.UTF_8);
    Aead aead = kek.toAead(Collections.emptyMap());
    byte[] encryptedDek = aead.encrypt(rawDek, new byte[0]);
    String encryptedDekStr =
        new String(Base64.getEncoder().encode(encryptedDek), StandardCharsets.UTF_8);
    Dek dek = new Dek(kekName, subject, 1, algorithm, encryptedDekStr, null, null, null);

    // Create dek
    Dek newDek = client.createDek(headers, kekName, subject, null, algorithm, encryptedDekStr, false);
    assertEquals(dek, newDek);

    client.deleteDek(headers, kekName, subject, algorithm, false);

    // Allow create to act like undelete
    newDek = client.createDek(headers, kekName, subject, null, algorithm, encryptedDekStr, false);
    assertEquals(dek, newDek);

    newDek = client.getDek(kekName, subject, algorithm, false);
    assertEquals(dek, newDek);
    assertNotNull(newDek.getTimestamp());

    // Create dek w/o key material
    try {
      newDek = client.createDek(headers, kekName, badSubject, null, algorithm, null, false);
      fail();
    } catch (RestClientException e) {
      assertEquals(DekRegistryErrors.INVALID_KEY_ERROR_CODE, e.getErrorCode());
    }

    newDek = client.getDek(kekName, subject, algorithm, false);
    assertEquals(dek, newDek);
    assertNotNull(newDek.getTimestamp());

    Kek kek2 = new Kek(kekName, kmsType, kmsKeyId, kmsProps, doc, true, null, null);

    // Set shared flag to true
    newKek = client.updateKek(headers, kekName, kmsProps, doc, true);
    assertEquals(kek2, newKek);

    // Advance ticker
    fakeTicker.advance(61, TimeUnit.SECONDS);

    // Dek now has decrypted key material
    Dek dek2 = new Dek(kekName, subject, 1, algorithm, encryptedDekStr, rawDekStr, null, null);
    newDek = client.getDek(kekName, subject, algorithm, true);
    assertEquals(dek2, newDek);
    assertNotNull(newDek.getTimestamp());

    // Create dek w/o key material, receive both encrypted and decrypted key material
    newDek = client.createDek(headers, kekName, subject2, null, algorithm, null, false);
    assertNotNull(newDek.getEncryptedKeyMaterial());
    if (isImport) {
      assertNull(newDek.getKeyMaterial());
    } else {
      assertNotNull(newDek.getKeyMaterial());
    }
    assertNotNull(newDek.getTimestamp());

    // Create versioned dek
    newDek = client.createDek(headers, kekName, subject2, 2, algorithm, null, false);
    assertEquals(2, newDek.getVersion());

    List<String> deks = client.listDeks(kekName, false);
    assertEquals(ImmutableList.of(subject, subject2), deks);

    List<Integer> versions = client.listDekVersions(kekName, subject2, null, false);
    assertEquals(ImmutableList.of(1, 2), versions);

    List<String> kekNames = client.listKeks(subject, false);
    assertEquals(ImmutableList.of(kekName), kekNames);

    kekNames = client.listKeks(subject2, false);
    assertEquals(ImmutableList.of(kekName), kekNames);

    try {
      client.deleteKek(headers, kekName, false);
      fail();
    } catch (RestClientException e) {
      assertEquals(DekRegistryErrors.REFERENCE_EXISTS_ERROR_CODE, e.getErrorCode());
    }

    try {
      client.deleteDek(headers, kekName, subject, algorithm, true);
      fail();
    } catch (RestClientException e) {
      assertEquals(DekRegistryErrors.KEY_NOT_SOFT_DELETED_ERROR_CODE, e.getErrorCode());
    }

    client.deleteDek(headers, kekName, subject, algorithm, false);

    try {
      client.getDek(kekName, subject, algorithm, false);
      fail();
    } catch (RestClientException e) {
      assertEquals(DekRegistryErrors.KEY_NOT_FOUND_ERROR_CODE, e.getErrorCode());
    }

    newDek = client.getDek(kekName, subject, algorithm, true);
    assertEquals(dek2, newDek);
    assertNotNull(newDek.getTimestamp());

    deks = client.listDeks(kekName, false);
    assertEquals(ImmutableList.of(subject2), deks);

    deks = client.listDeks(kekName, true);
    assertEquals(ImmutableList.of(subject, subject2), deks);

    client.deleteDekVersion(headers, kekName, subject2, 2, null, false);

    versions = client.listDekVersions(kekName, subject2, null, false);
    assertEquals(ImmutableList.of(1), versions);

    client.undeleteDekVersion(headers, kekName, subject2, 2, null);

    versions = client.listDekVersions(kekName, subject2, null, false);
    assertEquals(ImmutableList.of(1, 2), versions);

    kekNames = client.listKeks(subject2, false);
    assertEquals(ImmutableList.of(kekName), kekNames);

    kekNames = client.listKeks(subject, true);
    assertEquals(ImmutableList.of(kekName), kekNames);

    client.deleteDek(headers, kekName, subject2, algorithm, false);
    client.deleteKek(headers, kekName, false);

    try {
      deks = client.listDeks(kekName, false);
      fail();
    } catch (RestClientException e) {
      assertEquals(DekRegistryErrors.KEY_NOT_FOUND_ERROR_CODE, e.getErrorCode());
    }

    deks = client.listDeks(kekName, true);
    assertEquals(ImmutableList.of(subject, subject2), deks);

    try {
      client.undeleteDek(headers, kekName, subject2, algorithm);
      fail();
    } catch (RestClientException e) {
      assertEquals(DekRegistryErrors.KEY_SOFT_DELETED_ERROR_CODE, e.getErrorCode());
    }
    client.undeleteKek(headers, kekName);

    newKek = client.getKek(kekName, false);
    assertEquals(kek2, newKek);

    client.undeleteDek(headers, kekName, subject2, algorithm);

    deks = client.listDeks(kekName, false);
    assertEquals(ImmutableList.of(subject2), deks);

    // Delete again
    client.deleteDek(headers, kekName, subject2, algorithm, false);
    client.deleteKek(headers, kekName, false);

    try {
      client.deleteKek(headers, kekName, true);
      fail();
    } catch (RestClientException e) {
      assertEquals(DekRegistryErrors.REFERENCE_EXISTS_ERROR_CODE, e.getErrorCode());
    }

    client.deleteDek(headers, kekName, subject, algorithm, true);
    try {
      client.deleteDek(headers, kekName, subject, algorithm, true);
      fail();
    } catch (RestClientException e) {
      assertEquals(DekRegistryErrors.KEY_NOT_FOUND_ERROR_CODE, e.getErrorCode());
    }

    client.deleteDek(headers, kekName, subject2, algorithm, true);

    try {
      deks = client.listDeks(kekName, false);
      fail();
    } catch (RestClientException e) {
      assertEquals(DekRegistryErrors.KEY_NOT_FOUND_ERROR_CODE, e.getErrorCode());
    }

    deks = client.listDeks(kekName, true);
    assertEquals(Collections.emptyList(), deks);

    client.deleteKek(headers, kekName, true);
    try {
      client.deleteKek(headers, kekName, true);
      fail();
    } catch (RestClientException e) {
      assertEquals(DekRegistryErrors.KEY_NOT_FOUND_ERROR_CODE, e.getErrorCode());
    }
  }

  @Test
  public void testUnknownKmsType() throws Exception {
    String kekName = "kek1";
    String kmsType = "unknown-kms";
    String kmsKeyId = "myid";
    String subject = "mysubject";
    String subject2 = "mysubject2";
    DekFormat algorithm = DekFormat.AES256_GCM;
    Kek kek = new Kek(kekName, kmsType, kmsKeyId, null, null, false, null, null);

    // Create kek
    Kek newKek = client.createKek(kekName, kmsType, kmsKeyId, null, null, false);
    assertEquals(kek, newKek);

    newKek = client.getKek(kekName, false);
    assertEquals(kek, newKek);

    List<String> keks = client.listKeks(false);
    assertEquals(Collections.singletonList(kekName), keks);

    // Use the test-kms type to generate a dummy dek locally
    Kek testKek = new Kek(kekName, "test-kms", kmsKeyId, null, null, false, null, null);
    byte[] rawDek = new Cryptor(algorithm).generateKey();
    Aead aead = testKek.toAead(Collections.emptyMap());
    byte[] encryptedDek = aead.encrypt(rawDek, new byte[0]);
    String encryptedDekStr =
        new String(Base64.getEncoder().encode(encryptedDek), StandardCharsets.UTF_8);
    Dek dek = new Dek(kekName, subject, 1, algorithm, encryptedDekStr, null, null, null);

    // Create dek
    Dek newDek = client.createDek(kekName, subject, algorithm, encryptedDekStr);
    assertEquals(dek, newDek);

    newDek = client.getDek(kekName, subject, algorithm, false);
    assertEquals(dek, newDek);
    assertNotNull(newDek.getTimestamp());

    // Create dek w/o key material, exception
    try {
      newDek = client.createDek(kekName, subject2, algorithm, null);
      fail();
    } catch (RestClientException e) {
      assertEquals(DekRegistryErrors.INVALID_KEY_ERROR_CODE, e.getErrorCode());
    }

    Map<String, String> kmsProps = Collections.singletonMap("hi", "there");
    String doc = "mydoc";
    Kek kek2 = new Kek(kekName, kmsType, kmsKeyId, kmsProps, doc, true, null, null);

    // Set shared flag to true
    newKek = client.updateKek(kekName, kmsProps, doc, true);
    assertEquals(kek2, newKek);

    // Advance ticker
    fakeTicker.advance(61, TimeUnit.SECONDS);

    // Dek still does not have decrypted key material because kms type is unknown
    Dek dek2 = new Dek(kekName, subject, 1, algorithm, encryptedDekStr, null, null, null);
    try {
      newDek = client.getDek(kekName, subject, algorithm, true);
      fail();
    } catch (RestClientException e) {
      assertEquals(DekRegistryErrors.DEK_GENERATION_ERROR_CODE, e.getErrorCode());
    }
  }

  @Test
  public void testRegisterCreatesKmsDefaults() throws Exception {
    String subject = "testSubject";

    ParsedSchema schema1 = AvroUtils.parseSchema("{\"type\":\"record\","
        + "\"name\":\"myrecord\","
        + "\"fields\":"
        + "[{\"type\":\"string\",\"name\":\"f1\"}]}");

    Map<String, String> params = Collections.singletonMap("encrypt.kms.key.id",
        "arn:aws:kms:us-west-2:111122223333:key/1234abcd-12ab-34cd-56ef-1234567890ab");
    Rule r1 = new Rule("foo", null, null, RuleMode.WRITEREAD, "ENCRYPT", null, params, null, null, null, false);
    List<Rule> rules = Collections.singletonList(r1);
    RuleSet ruleSet = new RuleSet(null, rules);
    RegisterSchemaRequest request1 = new RegisterSchemaRequest(schema1);
    request1.setRuleSet(ruleSet);
    int expectedIdSchema1 = 1;
    assertEquals("Registering should succeed",
        expectedIdSchema1,
        restApp.restClient.registerSchema(request1, subject, false).getId());

    SchemaString schemaString = restApp.restClient.getId(expectedIdSchema1, subject);
    Map<String, String> newParams = schemaString.getRuleSet().getDomainRules().get(0).getParams();
    assertEquals("aws-kms-us-west-2-111122223333-key-1234abcd-12ab-34cd-56ef-1234567890ab",
        newParams.get("encrypt.kek.name"));
    assertEquals("aws-kms", newParams.get("encrypt.kms.type"));
  }
}

