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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.fail;

import com.google.common.collect.ImmutableList;
import com.google.common.testing.FakeTicker;
import com.google.crypto.tink.Aead;
import io.confluent.dekregistry.DekRegistryResourceExtension;
import io.confluent.dekregistry.client.CachedDekRegistryClient;
import io.confluent.dekregistry.client.DekRegistryClient;
import io.confluent.dekregistry.client.rest.DekRegistryRestService;
import io.confluent.dekregistry.client.rest.entities.Dek;
import io.confluent.dekregistry.client.rest.entities.Kek;
import io.confluent.dekregistry.web.rest.exceptions.DekRegistryErrors;
import io.confluent.kafka.schemaregistry.ClusterTestHarness;
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;
import io.confluent.kafka.schemaregistry.encryption.tink.Cryptor;
import io.confluent.kafka.schemaregistry.encryption.tink.DekFormat;
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import org.junit.Before;
import org.junit.Test;

public class RestApiTest extends ClusterTestHarness {

  FakeTicker fakeTicker;
  DekRegistryClient client;

  public RestApiTest() {
    super(1, true);
  }

  @Override
  public Properties getSchemaRegistryProperties() throws Exception {
    Properties props = new Properties();
    props.setProperty("resource.extension.class", DekRegistryResourceExtension.class.getName());
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
  }

  @Test
  public void testBasic() throws Exception {
    String kekName = "kek1";
    String kmsType = "test-kms";
    String kmsKeyId = "myid";
    String subject = "mysubject";
    String subject2 = "mysubject2";
    String subject3 = "mysubject3";
    DekFormat algorithm = DekFormat.AES256_GCM;
    Kek kek = new Kek(kekName, kmsType, kmsKeyId, null, null, false, null);

    // Create kek
    Kek newKek = client.createKek(kekName, kmsType, kmsKeyId, null, null, false);
    assertEquals(kek, newKek);

    newKek = client.getKek(kekName, false);
    assertEquals(kek, newKek);

    List<String> keks = client.listKeks(false);
    assertEquals(Collections.singletonList(kekName), keks);

    try {
      client.deleteKek(kekName, true);
      fail();
    } catch (RestClientException e) {
      assertEquals(DekRegistryErrors.KEY_NOT_SOFT_DELETED_ERROR_CODE, e.getErrorCode());
    }

    // Delete kek
    client.deleteKek(kekName, false);

    Map<String, String> kmsProps = Collections.singletonMap("hi", "there");
    String doc = "mydoc";
    try {
      client.updateKek(kekName, kmsProps, doc, true);
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

    client.deleteKek(kekName, true);

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
    newKek = client.createKek(kekName, kmsType, kmsKeyId, null, null, false);
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
    Dek dek = new Dek(kekName, subject, 1, algorithm, encryptedDekStr, null, null);

    // Create dek
    Dek newDek = client.createDek(kekName, subject, algorithm, encryptedDekStr);
    assertEquals(dek, newDek);

    newDek = client.getDek(kekName, subject, algorithm, false);
    assertEquals(dek, newDek);

    // Create dek w/o key material, receive encrypted key material
    newDek = client.createDek(kekName, subject2, algorithm, null);
    assertNotNull(newDek.getEncryptedKeyMaterial());
    assertNull(newDek.getKeyMaterial());

    newDek = client.getDek(kekName, subject, algorithm, false);
    assertEquals(dek, newDek);

    Kek kek2 = new Kek(kekName, kmsType, kmsKeyId, kmsProps, doc, true, null);

    // Set shared flag to true
    newKek = client.updateKek(kekName, kmsProps, doc, true);
    assertEquals(kek2, newKek);

    // Advance ticker
    fakeTicker.advance(61, TimeUnit.SECONDS);

    // Dek now has decrypted key material
    Dek dek2 = new Dek(kekName, subject, 1, algorithm, encryptedDekStr, rawDekStr, null);
    newDek = client.getDek(kekName, subject, algorithm, true);
    assertEquals(dek2, newDek);

    // Create dek w/o key material, receive both encrypted and decrypted key material
    newDek = client.createDek(kekName, subject3, algorithm, null);
    assertNotNull(newDek.getEncryptedKeyMaterial());
    assertNotNull(newDek.getKeyMaterial());

    List<String> deks = client.listDeks(kekName, false);
    assertEquals(ImmutableList.of(subject, subject2, subject3), deks);

    try {
      client.deleteKek(kekName, false);
      fail();
    } catch (RestClientException e) {
      assertEquals(DekRegistryErrors.REFERENCE_EXISTS_ERROR_CODE, e.getErrorCode());
    }

    try {
      client.deleteDek(kekName, subject, algorithm, true);
      fail();
    } catch (RestClientException e) {
      assertEquals(DekRegistryErrors.KEY_NOT_SOFT_DELETED_ERROR_CODE, e.getErrorCode());
    }

    client.deleteDek(kekName, subject, algorithm, false);

    try {
      client.getDek(kekName, subject, algorithm, false);
      fail();
    } catch (RestClientException e) {
      assertEquals(DekRegistryErrors.KEY_NOT_FOUND_ERROR_CODE, e.getErrorCode());
    }

    newDek = client.getDek(kekName, subject, algorithm, true);
    assertEquals(dek2, newDek);

    deks = client.listDeks(kekName, false);
    assertEquals(ImmutableList.of(subject2, subject3), deks);

    deks = client.listDeks(kekName, true);
    assertEquals(ImmutableList.of(subject, subject2, subject3), deks);

    client.deleteDek(kekName, subject2, algorithm, false);
    client.deleteDek(kekName, subject3, algorithm, false);
    client.deleteKek(kekName, false);

    deks = client.listDeks(kekName, false);
    assertEquals(Collections.emptyList(), deks);

    deks = client.listDeks(kekName, true);
    assertEquals(ImmutableList.of(subject, subject2, subject3), deks);

    try {
      client.deleteKek(kekName, true);
      fail();
    } catch (RestClientException e) {
      assertEquals(DekRegistryErrors.REFERENCE_EXISTS_ERROR_CODE, e.getErrorCode());
    }

    client.deleteDek(kekName, subject, algorithm, true);
    client.deleteDek(kekName, subject2, algorithm, true);
    client.deleteDek(kekName, subject3, algorithm, true);

    deks = client.listDeks(kekName, false);
    assertEquals(Collections.emptyList(), deks);

    deks = client.listDeks(kekName, true);
    assertEquals(Collections.emptyList(), deks);

    client.deleteKek(kekName, true);
  }

  @Test
  public void testUnknownKmsType() throws Exception {
    String kekName = "kek1";
    String kmsType = "unknown-kms";
    String kmsKeyId = "myid";
    String subject = "mysubject";
    String subject2 = "mysubject2";
    DekFormat algorithm = DekFormat.AES256_GCM;
    Kek kek = new Kek(kekName, kmsType, kmsKeyId, null, null, false, null);

    // Create kek
    Kek newKek = client.createKek(kekName, kmsType, kmsKeyId, null, null, false);
    assertEquals(kek, newKek);

    newKek = client.getKek(kekName, false);
    assertEquals(kek, newKek);

    List<String> keks = client.listKeks(false);
    assertEquals(Collections.singletonList(kekName), keks);

    // Use the test-kms type to generate a dummy dek locally
    Kek testKek = new Kek(kekName, "test-kms", kmsKeyId, null, null, false, null);
    byte[] rawDek = new Cryptor(algorithm).generateKey();
    Aead aead = testKek.toAead(Collections.emptyMap());
    byte[] encryptedDek = aead.encrypt(rawDek, new byte[0]);
    String encryptedDekStr =
        new String(Base64.getEncoder().encode(encryptedDek), StandardCharsets.UTF_8);
    Dek dek = new Dek(kekName, subject, 1, algorithm, encryptedDekStr, null, null);

    // Create dek
    Dek newDek = client.createDek(kekName, subject, algorithm, encryptedDekStr);
    assertEquals(dek, newDek);

    newDek = client.getDek(kekName, subject, algorithm, false);
    assertEquals(dek, newDek);

    // Create dek w/o key material, exception
    try {
      newDek = client.createDek(kekName, subject2, algorithm, null);
      fail();
    } catch (RestClientException e) {
      assertEquals(DekRegistryErrors.DEK_GENERATION_ERROR_CODE, e.getErrorCode());
    }

    Map<String, String> kmsProps = Collections.singletonMap("hi", "there");
    String doc = "mydoc";
    Kek kek2 = new Kek(kekName, kmsType, kmsKeyId, kmsProps, doc, true, null);

    // Set shared flag to true
    newKek = client.updateKek(kekName, kmsProps, doc, true);
    assertEquals(kek2, newKek);

    // Advance ticker
    fakeTicker.advance(61, TimeUnit.SECONDS);

    // Dek still does not have decrypted key material because kms type is unknown
    Dek dek2 = new Dek(kekName, subject, 1, algorithm, encryptedDekStr, null, null);
    try {
      newDek = client.getDek(kekName, subject, algorithm, true);
      fail();
    } catch (RestClientException e) {
      assertEquals(DekRegistryErrors.DEK_GENERATION_ERROR_CODE, e.getErrorCode());
    }
  }
}

