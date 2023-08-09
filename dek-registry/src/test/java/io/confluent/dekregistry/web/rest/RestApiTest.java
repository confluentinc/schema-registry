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
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import com.google.crypto.tink.Aead;
import io.confluent.dekregistry.DekRegistryResourceExtension;
import io.confluent.dekregistry.client.DekRegistryClient;
import io.confluent.dekregistry.client.DekRegistryClientFactory;
import io.confluent.dekregistry.client.MockDekRegistryClient;
import io.confluent.dekregistry.client.rest.entities.Dek;
import io.confluent.dekregistry.client.rest.entities.Kek;
import io.confluent.dekregistry.web.rest.exceptions.DekRegistryErrors;
import io.confluent.kafka.schemaregistry.ClusterTestHarness;
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;
import io.confluent.kafka.schemaregistry.encryption.tink.Cryptor;
import io.confluent.kafka.schemaregistry.encryption.tink.DekFormat;
import io.confluent.kafka.schemaregistry.encryption.tink.KmsDriverManager;
import io.confluent.kafka.schemaregistry.storage.KafkaSchemaRegistry;
import io.confluent.kafka.schemaregistry.storage.RuleSetHandler;
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.Collections;
import java.util.Properties;
import org.checkerframework.checker.units.qual.K;
import org.junit.Before;
import org.junit.Test;

public class RestApiTest extends ClusterTestHarness {

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
    client = DekRegistryClientFactory.newClient(restApp.restClient.getBaseUrls().urls(),
        1000,
        100000,
        null,
        null
    );
  }

  @Test
  public void testBasic() throws Exception {
    String kekName = "kek1";
    String kmsType = "test-kms";
    String kmsKeyId = "myid";
    String scope = "mysubject";
    DekFormat algorithm = DekFormat.AES256_GCM;
    Kek kek = new Kek(kekName, kmsType, kmsKeyId, null, null, false);

    Kek newKek = client.createKek(kekName, kmsType, kmsKeyId, null, null, false);
    assertEquals(kek, newKek);

    newKek = client.getKek(kekName, false);
    assertEquals(kek, newKek);

    client.deleteKek(kekName, false);

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

    newKek = client.createKek(kekName, kmsType, kmsKeyId, null, null, false);
    assertEquals(kek, newKek);

    newKek = client.getKek(kekName, false);
    assertEquals(kek, newKek);

    byte[] rawDek = new Cryptor(algorithm).generateKey();
    Aead aead = kek.toAead(Collections.emptyMap());
    byte[] encryptedDek = aead.encrypt(rawDek, new byte[0]);
    String encryptedDekStr =
        new String(Base64.getEncoder().encode(encryptedDek), StandardCharsets.UTF_8);
    Dek dek = new Dek(kekName, scope, algorithm, encryptedDekStr, null);

    Dek newDek = client.createDek(kekName, scope, algorithm, encryptedDekStr);
    assertEquals(dek, newDek);

    newDek = client.getDek(kekName, scope, algorithm, false);
    assertEquals(dek, newDek);

    try {
      client.deleteKek(kekName, false);
      fail();
    } catch (RestClientException e) {
      assertEquals(DekRegistryErrors.REFERENCE_EXISTS_ERROR_CODE, e.getErrorCode());
    }

    client.deleteDek(kekName, scope, algorithm, false);

    try {
      client.getDek(kekName, scope, algorithm, false);
      fail();
    } catch (RestClientException e) {
      assertEquals(DekRegistryErrors.KEY_NOT_FOUND_ERROR_CODE, e.getErrorCode());
    }

    newDek = client.getDek(kekName, scope, algorithm, true);
    assertEquals(dek, newDek);

    client.deleteKek(kekName, false);

    try {
      client.deleteKek(kekName, true);
      fail();
    } catch (RestClientException e) {
      assertEquals(DekRegistryErrors.REFERENCE_EXISTS_ERROR_CODE, e.getErrorCode());
    }

    client.deleteDek(kekName, scope, algorithm, true);

    client.deleteKek(kekName, true);
  }
}

