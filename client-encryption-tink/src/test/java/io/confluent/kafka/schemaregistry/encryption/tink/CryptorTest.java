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

package io.confluent.kafka.schemaregistry.encryption.tink;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;

import java.nio.charset.StandardCharsets;
import org.junit.Test;

public class CryptorTest {

  @Test
  public void testRandomCryptor() throws Exception {
    Cryptor cryptor = new Cryptor(DekFormat.AES128_GCM);
    byte[] dek = cryptor.generateKey();
    byte[] plaintext = "hello world".getBytes(StandardCharsets.UTF_8);
    byte[] ciphertext = cryptor.encrypt(dek, plaintext, new byte[0]);
    assertNotEquals(plaintext, ciphertext);

    plaintext = cryptor.decrypt(dek, ciphertext, new byte[0]);
    assertEquals("hello world", new String(plaintext, StandardCharsets.UTF_8));
  }

  @Test
  public void testRandomCryptor2() throws Exception {
    Cryptor cryptor = new Cryptor(DekFormat.AES256_GCM);
    byte[] dek = cryptor.generateKey();
    byte[] plaintext = "hello world".getBytes(StandardCharsets.UTF_8);
    byte[] ciphertext = cryptor.encrypt(dek, plaintext, new byte[0]);
    assertNotEquals(plaintext, ciphertext);

    plaintext = cryptor.decrypt(dek, ciphertext, new byte[0]);
    assertEquals("hello world", new String(plaintext, StandardCharsets.UTF_8));
  }

  @Test
  public void testDeterministicCryptor() throws Exception {
    Cryptor cryptor = new Cryptor(DekFormat.AES256_SIV);
    byte[] dek = cryptor.generateKey();
    byte[] plaintext = "hello world".getBytes(StandardCharsets.UTF_8);
    byte[] ciphertext = cryptor.encrypt(dek, plaintext, new byte[0]);
    assertNotEquals(plaintext, ciphertext);

    plaintext = cryptor.decrypt(dek, ciphertext, new byte[0]);
    assertEquals("hello world", new String(plaintext, StandardCharsets.UTF_8));

    // ciphertext is same given same key and plaintext
    byte[] ciphertext2 = cryptor.encrypt(dek, plaintext, new byte[0]);
    assertArrayEquals(ciphertext, ciphertext2);
  }
}

