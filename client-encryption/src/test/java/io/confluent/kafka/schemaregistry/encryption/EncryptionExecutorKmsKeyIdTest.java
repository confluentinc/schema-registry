/*
 * Copyright 2026 Confluent Inc.
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

package io.confluent.kafka.schemaregistry.encryption;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

import java.nio.charset.StandardCharsets;
import java.util.Map;
import org.junit.Test;

/**
 * Unit tests for {@link EncryptionExecutor#prefixKmsKeyId} and
 * {@link EncryptionExecutor#extractKmsKeyId}, the encoding used to make a DEK's
 * encryptedKeyMaterial self-describing with respect to which kms key id wrapped it, independent
 * of the KEK's current ENCRYPT_KMS_KEY_ID_SAVE setting.
 */
public class EncryptionExecutorKmsKeyIdTest {

  @Test
  public void testPrefixAndExtractRoundTrip() {
    String kmsKeyId = "https://myvault.vault.azure.net/keys/mykey/abcdef1234567890";
    byte[] wrapped = {1, 2, 3, 4, 5, 6, 7, 8};
    byte[] prefixed = EncryptionExecutor.prefixKmsKeyId(kmsKeyId, wrapped);

    Map.Entry<String, byte[]> extracted = EncryptionExecutor.extractKmsKeyId(prefixed);

    assertEquals(kmsKeyId, extracted.getKey());
    assertArrayEquals(wrapped, extracted.getValue());
  }

  @Test
  public void testPrefixAndExtractRoundTripWithEmptyWrapped() {
    String kmsKeyId = "mykey";
    byte[] wrapped = {};
    byte[] prefixed = EncryptionExecutor.prefixKmsKeyId(kmsKeyId, wrapped);

    Map.Entry<String, byte[]> extracted = EncryptionExecutor.extractKmsKeyId(prefixed);

    assertEquals(kmsKeyId, extracted.getKey());
    assertArrayEquals(wrapped, extracted.getValue());
  }

  @Test
  public void testExtractReturnsNullForLegacyNonPrefixedBytes() {
    // Arbitrary bytes that do not start with the KMS_KEY_ID_MAGIC sequence, simulating a DEK
    // wrapped before ENCRYPT_KMS_KEY_ID_SAVE was enabled on its KEK.
    byte[] legacy = {10, 20, 30, 40, 50, 60, 70, 80, 90, 100};
    assertNull(EncryptionExecutor.extractKmsKeyId(legacy));
  }

  @Test
  public void testExtractReturnsNullForEmptyBuffer() {
    assertNull(EncryptionExecutor.extractKmsKeyId(new byte[0]));
  }

  @Test
  public void testExtractReturnsNullForBufferShorterThanHeader() {
    // Magic only, with no length varint byte at all (needs at least 1 more byte).
    byte[] tooShort = {0x0, 'e', 'd', 'e', 'k'};
    assertNull(EncryptionExecutor.extractKmsKeyId(tooShort));
  }

  @Test
  public void testExtractReturnsNullWhenDeclaredLengthExceedsRemainingBytes() {
    // Valid magic + a single-byte zig-zag varint declaring a length of 5 (zigzag(5) = 10 = 0x0A,
    // small enough to fit in one byte with no continuation bit), but nothing actually follows.
    byte[] magic = {0x0, 'e', 'd', 'e', 'k'};
    byte[] malformed = new byte[magic.length + 1];
    System.arraycopy(magic, 0, malformed, 0, magic.length);
    malformed[magic.length] = 0x0A;
    assertNull(EncryptionExecutor.extractKmsKeyId(malformed));
  }

  @Test
  public void testPrefixEncodesLengthLikeAvroForShortId() {
    // idBytes.length (46) zig-zags to 92, which fits in a single-byte varint (< 128), matching
    // how Avro encodes a "string" length of this size.
    String kmsKeyId = "https://myvault.vault.azure.net/keys/mykey/v1";
    byte[] wrapped = {42};
    byte[] prefixed = EncryptionExecutor.prefixKmsKeyId(kmsKeyId, wrapped);

    Map.Entry<String, byte[]> extracted = EncryptionExecutor.extractKmsKeyId(prefixed);

    assertEquals(kmsKeyId, extracted.getKey());
    int idByteLength = kmsKeyId.getBytes(StandardCharsets.UTF_8).length;
    assertEquals(5 /* magic */ + 1 /* single-byte varint length */ + idByteLength + wrapped.length,
        prefixed.length);
  }

  @Test
  public void testPrefixAndExtractRoundTripWithLongIdRequiringMultiByteVarint() {
    // idBytes.length (200) zig-zags to 400, which needs a 2-byte varint, matching how Avro would
    // encode a "string" length in this range.
    StringBuilder builder = new StringBuilder();
    for (int i = 0; i < 200; i++) {
      builder.append('x');
    }
    String kmsKeyId = builder.toString();
    byte[] wrapped = {1, 2, 3};
    byte[] prefixed = EncryptionExecutor.prefixKmsKeyId(kmsKeyId, wrapped);

    Map.Entry<String, byte[]> extracted = EncryptionExecutor.extractKmsKeyId(prefixed);

    assertEquals(kmsKeyId, extracted.getKey());
    assertArrayEquals(wrapped, extracted.getValue());
    assertEquals(5 /* magic */ + 2 /* two-byte varint length */ + 200 + wrapped.length,
        prefixed.length);
  }
}
