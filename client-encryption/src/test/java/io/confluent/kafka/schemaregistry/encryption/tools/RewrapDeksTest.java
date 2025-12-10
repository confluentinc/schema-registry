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
import java.util.Collections;
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
}

