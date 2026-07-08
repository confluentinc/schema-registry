/*
 * Copyright 2025 Confluent Inc.
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

package io.confluent.kafka.schemaregistry.encryption.tools;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

import com.google.common.collect.ImmutableMap;
import io.confluent.dekregistry.client.DekRegistryClient;
import io.confluent.dekregistry.client.DekRegistryClientFactory;
import io.confluent.dekregistry.client.MockDekRegistryClientFactory;
import io.confluent.dekregistry.client.rest.entities.Dek;
import io.confluent.kafka.schemaregistry.encryption.EncryptionExecutor;
import java.util.Base64;
import java.util.Collections;
import java.util.Map;
import org.junit.After;
import org.junit.Test;
import picocli.CommandLine;

public class RewrapDeksTest {

  private final DekRegistryClient dekRegistry;
  private final String topic1;
  private final String topic2;

  public RewrapDeksTest() throws Exception {
    topic1 = "test1";
    topic2 = "test2";
    dekRegistry = DekRegistryClientFactory.newClient(Collections.singletonList(
            "mock://"),
        1000,
        100000,
        Collections.emptyMap(),
        null
    );
  }

  @After
  public void tearDown() {
    MockDekRegistryClientFactory.clear();
  }

  @Test
  public void testRewrapDek() throws Exception {
    String subject1 = topic1 + "-value";
    String subject2 = topic2 + "-value";
    String kekName = "kek1";
    dekRegistry.createKek(kekName, "local-kms", "mykey", Collections.emptyMap(), null, false);
    String encryptedDek = "07V2ndh02DA73p+dTybwZFm7DKQSZN1tEwQh+FoX1DZLk4Yj2LLu4omYjp/84tAg3BYlkfGSz+zZacJHIE4=";
    dekRegistry.createDek(kekName, subject1, null, encryptedDek);
    dekRegistry.createDek(kekName, subject2, null, encryptedDek);

    RewrapDeks app = new RewrapDeks();
    CommandLine cmd = new CommandLine(app);

    int exitCode = cmd.execute("mock://", kekName,
        "--property", "rule.executors._default_.param.secret=mysecret");
    assertEquals(0, exitCode);

    Dek dek = dekRegistry.getDekVersion(kekName, subject1, -1, null, false);
    assertEquals(kekName, dek.getKekName());
    assertNotNull(dek.getEncryptedKeyMaterial());
    assertNull(dek.getKeyMaterial());

    dek = dekRegistry.getDekVersion(kekName, subject2, -1, null, false);
    assertEquals(kekName, dek.getKekName());
    assertNotNull(dek.getEncryptedKeyMaterial());
    assertNull(dek.getKeyMaterial());
  }

  @Test
  public void testRewrapDekSingleSubject() throws Exception {
    String subject1 = topic1 + "-value";
    String kekName = "kek1";
    dekRegistry.createKek(kekName, "local-kms", "mykey", Collections.emptyMap(), null, false);
    String encryptedDek = "07V2ndh02DA73p+dTybwZFm7DKQSZN1tEwQh+FoX1DZLk4Yj2LLu4omYjp/84tAg3BYlkfGSz+zZacJHIE4=";
    dekRegistry.createDek(kekName, subject1, null, encryptedDek);

    RewrapDeks app = new RewrapDeks();
    CommandLine cmd = new CommandLine(app);

    int exitCode = cmd.execute("mock://", kekName, subject1,
        "--property", "rule.executors._default_.param.secret=mysecret");
    assertEquals(0, exitCode);

    Dek dek = dekRegistry.getDekVersion(kekName, subject1, -1, null, false);
    assertEquals(kekName, dek.getKekName());
    assertNotNull(dek.getEncryptedKeyMaterial());
    assertNull(dek.getKeyMaterial());

    assertEquals(kekName, dek.getKekName());
    assertNotNull(dek.getEncryptedKeyMaterial());
    assertNull(dek.getKeyMaterial());
  }

  @Test
  public void testRewrapDekForSharedKek() throws Exception {
    String subject1 = topic1 + "-value";
    String subject2 = topic2 + "-value";
    String kekName = "kek1";
    dekRegistry.createKek(kekName, "local-kms", "mykey", ImmutableMap.of("secret", "mysecret"), null, true);
    String encryptedDek = null;
    dekRegistry.createDek(kekName, subject1, null, encryptedDek);
    dekRegistry.createDek(kekName, subject2, null, encryptedDek);

    RewrapDeks app = new RewrapDeks();
    CommandLine cmd = new CommandLine(app);

    int exitCode = cmd.execute("mock://", kekName);
    assertEquals(0, exitCode);

    Dek dek = dekRegistry.getDekVersion(kekName, subject1, -1, null, false);
    assertEquals(kekName, dek.getKekName());
    assertNotNull(dek.getEncryptedKeyMaterial());
    assertNotNull(dek.getKeyMaterial());

    dek = dekRegistry.getDekVersion(kekName, subject2, -1, null, false);
    assertEquals(kekName, dek.getKekName());
    assertNotNull(dek.getEncryptedKeyMaterial());
    assertNotNull(dek.getKeyMaterial());
  }

  @Test
  public void testRewrapDekForSharedKekSingleSubject() throws Exception {
    String subject1 = topic1 + "-value";
    String kekName = "kek1";
    dekRegistry.createKek(kekName, "local-kms", "mykey", ImmutableMap.of("secret", "mysecret"), null, true);
    String encryptedDek = null;
    dekRegistry.createDek(kekName, subject1, null, encryptedDek);

    RewrapDeks app = new RewrapDeks();
    CommandLine cmd = new CommandLine(app);

    int exitCode = cmd.execute("mock://", kekName, subject1);
    assertEquals(0, exitCode);

    Dek dek = dekRegistry.getDekVersion(kekName, subject1, -1, null, false);
    assertEquals(kekName, dek.getKekName());
    assertNotNull(dek.getEncryptedKeyMaterial());
    assertNotNull(dek.getKeyMaterial());
  }

  @Test
  public void testRewrapDekWithKmsKeyIdSave() throws Exception {
    String subject1 = topic1 + "-value";
    String kekName = "kek1";
    dekRegistry.createKek(kekName, "local-kms", "mykey",
        ImmutableMap.of(EncryptionExecutor.ENCRYPT_KMS_KEY_ID_SAVE, "true"), null, false);
    String encryptedDek = "07V2ndh02DA73p+dTybwZFm7DKQSZN1tEwQh+FoX1DZLk4Yj2LLu4omYjp/84tAg3BYlkfGSz+zZacJHIE4=";
    dekRegistry.createDek(kekName, subject1, null, encryptedDek);

    RewrapDeks app = new RewrapDeks();
    CommandLine cmd = new CommandLine(app);

    int exitCode = cmd.execute("mock://", kekName, subject1,
        "--property", "rule.executors._default_.param.secret=mysecret");
    assertEquals(0, exitCode);

    Dek dek = dekRegistry.getDekVersion(kekName, subject1, -1, null, false);
    assertNotNull(dek.getEncryptedKeyMaterial());
    Map.Entry<String, byte[]> parsed =
        EncryptionExecutor.extractKmsKeyId(dek.getEncryptedKeyMaterialBytes());
    assertNotNull("rewrapped edek should carry a kms key id prefix now that the toggle is on",
        parsed);
    assertEquals("mykey", parsed.getKey());
  }

  @Test
  public void testRewrapDekOfAlreadyPrefixedEdek() throws Exception {
    String subject1 = topic1 + "-value";
    String kekName = "kek1";
    dekRegistry.createKek(kekName, "local-kms", "mykey",
        ImmutableMap.of(EncryptionExecutor.ENCRYPT_KMS_KEY_ID_SAVE, "true"), null, false);
    byte[] wrapped = Base64.getDecoder().decode(
        "07V2ndh02DA73p+dTybwZFm7DKQSZN1tEwQh+FoX1DZLk4Yj2LLu4omYjp/84tAg3BYlkfGSz+zZacJHIE4=");
    byte[] prefixed = EncryptionExecutor.prefixKmsKeyId("mykey", wrapped);
    String encryptedDek = Base64.getEncoder().encodeToString(prefixed);
    dekRegistry.createDek(kekName, subject1, null, encryptedDek);

    RewrapDeks app = new RewrapDeks();
    CommandLine cmd = new CommandLine(app);

    // Rewrap must successfully decrypt using the id embedded in the existing prefix.
    int exitCode = cmd.execute("mock://", kekName, subject1,
        "--property", "rule.executors._default_.param.secret=mysecret");
    assertEquals(0, exitCode);

    Dek dek = dekRegistry.getDekVersion(kekName, subject1, -1, null, false);
    assertNotNull(dek.getEncryptedKeyMaterial());
    Map.Entry<String, byte[]> parsed =
        EncryptionExecutor.extractKmsKeyId(dek.getEncryptedKeyMaterialBytes());
    assertNotNull(parsed);
    assertEquals("mykey", parsed.getKey());
  }

  @Test
  public void testRewrapDekOfAlreadyPrefixedEdekWithToggleOff() throws Exception {
    // A dek that was prefixed while the toggle was on must still rewrap correctly after the
    // toggle is turned back off, and the output should revert to the legacy (unprefixed) format
    // since that is what the KEK's current configuration requests.
    String subject1 = topic1 + "-value";
    String kekName = "kek1";
    dekRegistry.createKek(kekName, "local-kms", "mykey", Collections.emptyMap(), null, false);
    byte[] wrapped = Base64.getDecoder().decode(
        "07V2ndh02DA73p+dTybwZFm7DKQSZN1tEwQh+FoX1DZLk4Yj2LLu4omYjp/84tAg3BYlkfGSz+zZacJHIE4=");
    byte[] prefixed = EncryptionExecutor.prefixKmsKeyId("mykey", wrapped);
    String encryptedDek = Base64.getEncoder().encodeToString(prefixed);
    dekRegistry.createDek(kekName, subject1, null, encryptedDek);

    RewrapDeks app = new RewrapDeks();
    CommandLine cmd = new CommandLine(app);

    int exitCode = cmd.execute("mock://", kekName, subject1,
        "--property", "rule.executors._default_.param.secret=mysecret");
    assertEquals(0, exitCode);

    Dek dek = dekRegistry.getDekVersion(kekName, subject1, -1, null, false);
    assertNotNull(dek.getEncryptedKeyMaterial());
    assertNull("toggle is off, so the rewrapped edek should not carry a prefix",
        EncryptionExecutor.extractKmsKeyId(dek.getEncryptedKeyMaterialBytes()));
  }
}

