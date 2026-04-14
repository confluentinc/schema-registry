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

package io.confluent.dekregistry.storage;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.confluent.kafka.schemaregistry.encryption.tink.DekFormat;
import java.util.TreeMap;
import org.junit.jupiter.api.Test;

class EncryptionKeyToStringTest {

  @Test
  void kekToStringShouldIncludeIdentifyingFields() {
    TreeMap<String, String> kmsProps = new TreeMap<>();
    kmsProps.put("prop1", "value1");
    KeyEncryptionKey kek = new KeyEncryptionKey(
        "myKek", "aws-kms", "arn:aws:kms:us-west-2:123:key/abc", kmsProps,
        "some doc", true, false);
    kek.setTimestamp(1000L);
    kek.setOffset(42L);

    String str = kek.toString();
    assertTrue(str.contains("name=myKek"));
    assertTrue(str.contains("kmsType=aws-kms"));
    assertTrue(str.contains("shared=true"));
    assertTrue(str.contains("deleted=false"));
    assertTrue(str.contains("offset=42"));
    assertTrue(str.contains("ts=1000"));
  }

  @Test
  void kekToStringShouldExcludeSensitiveFields() {
    TreeMap<String, String> kmsProps = new TreeMap<>();
    kmsProps.put("secretProp", "secretValue");
    KeyEncryptionKey kek = new KeyEncryptionKey(
        "myKek", "aws-kms", "arn:aws:kms:us-west-2:123:key/abc", kmsProps,
        "internal documentation", true, false);

    String str = kek.toString();
    assertFalse(str.contains("arn:aws:kms"), "toString must not contain kmsKeyId");
    assertFalse(str.contains("secretProp"), "toString must not contain kmsProps keys");
    assertFalse(str.contains("secretValue"), "toString must not contain kmsProps values");
    assertFalse(str.contains("internal documentation"), "toString must not contain doc");
  }

  @Test
  void dekToStringShouldIncludeIdentifyingFields() {
    DataEncryptionKey dek = new DataEncryptionKey(
        "myKek", "mySubject", DekFormat.AES256_GCM, 1,
        "encryptedMaterial123", false);
    dek.setTimestamp(2000L);
    dek.setOffset(43L);
    dek.setKeyMaterial("rawKeyMaterial");

    String str = dek.toString();
    assertTrue(str.contains("kekName=myKek"));
    assertTrue(str.contains("subject=mySubject"));
    assertTrue(str.contains("algorithm=AES256_GCM"));
    assertTrue(str.contains("version=1"));
    assertTrue(str.contains("deleted=false"));
    assertTrue(str.contains("offset=43"));
    assertTrue(str.contains("ts=2000"));
  }

  @Test
  void dekToStringShouldExcludeSensitiveFields() {
    DataEncryptionKey dek = new DataEncryptionKey(
        "myKek", "mySubject", DekFormat.AES256_GCM, 1,
        "encryptedMaterial123", false);
    dek.setKeyMaterial("rawKeyMaterial");

    String str = dek.toString();
    assertFalse(str.contains("encryptedMaterial123"),
        "toString must not contain encryptedKeyMaterial");
    assertFalse(str.contains("rawKeyMaterial"),
        "toString must not contain keyMaterial");
  }
}
