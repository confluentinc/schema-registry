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

package io.confluent.kafka.schemaregistry.encryption.alicloud;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

import java.nio.charset.StandardCharsets;
import java.security.GeneralSecurityException;
import java.util.Base64;
import java.util.Map;
import org.junit.Test;

public class AliCloudKmsAeadTest {

  @Test
  public void encryptSendsBase64PlaintextAndAssociatedData() throws Exception {
    RecordingOperations operations = new RecordingOperations();
    AliCloudKmsAead aead = new AliCloudKmsAead(keyUri(), operations);

    byte[] ciphertext = aead.encrypt(bytes("secret-dek"), bytes("subject:customer-value"));

    assertEquals("ciphertext-from-kms", new String(ciphertext, StandardCharsets.UTF_8));
    assertEquals("alias/csfle", operations.encryptKeyId);
    assertEquals(base64("secret-dek"), operations.encryptPlaintextBase64);
    assertEquals(
        base64("subject:customer-value"),
        operations.encryptContext.get(AliCloudKmsAead.ASSOCIATED_DATA_CONTEXT_KEY));
  }

  @Test
  public void encryptOmitsContextWhenAssociatedDataIsEmpty() throws Exception {
    RecordingOperations operations = new RecordingOperations();
    AliCloudKmsAead aead = new AliCloudKmsAead(keyUri(), operations);

    aead.encrypt(bytes("secret-dek"), new byte[0]);

    assertNull(operations.encryptContext);
  }

  @Test
  public void decryptSendsCiphertextAndReturnsDecodedPlaintext() throws Exception {
    RecordingOperations operations = new RecordingOperations();
    operations.decryptPlaintextBase64 = base64("secret-dek");
    AliCloudKmsAead aead = new AliCloudKmsAead(keyUri(), operations);

    byte[] plaintext = aead.decrypt(bytes("ciphertext-from-kms"), bytes("subject:customer-value"));

    assertArrayEquals(bytes("secret-dek"), plaintext);
    assertEquals("ciphertext-from-kms", operations.decryptCiphertextBlob);
    assertEquals(
        base64("subject:customer-value"),
        operations.decryptContext.get(AliCloudKmsAead.ASSOCIATED_DATA_CONTEXT_KEY));
  }

  @Test
  public void decryptRejectsWrongReturnedKeyIdForPlainKeyIdUris() throws Exception {
    RecordingOperations operations = new RecordingOperations();
    operations.decryptPlaintextBase64 = base64("secret-dek");
    operations.decryptKeyId = "key-different";
    AliCloudKmsAead aead = new AliCloudKmsAead(
        AliCloudKmsKeyUri.parse("alicloud-kms://cn-chengdu/key-expected"),
        operations);

    GeneralSecurityException error = assertThrows(
        GeneralSecurityException.class,
        () -> aead.decrypt(bytes("ciphertext-from-kms"), null));

    assertTrue(error.getMessage().contains("wrong key id"));
  }

  @Test
  public void decryptDoesNotCompareAliasAgainstReturnedKeyId() throws Exception {
    RecordingOperations operations = new RecordingOperations();
    operations.decryptPlaintextBase64 = base64("secret-dek");
    operations.decryptKeyId = "key-canonical";
    AliCloudKmsAead aead = new AliCloudKmsAead(keyUri(), operations);

    byte[] plaintext = aead.decrypt(bytes("ciphertext-from-kms"), null);

    assertArrayEquals(bytes("secret-dek"), plaintext);
  }

  @Test
  public void wrapsKmsEncryptFailure() throws Exception {
    RecordingOperations operations = new RecordingOperations();
    operations.encryptFailure = new IllegalStateException("boom");
    AliCloudKmsAead aead = new AliCloudKmsAead(keyUri(), operations);

    GeneralSecurityException error = assertThrows(
        GeneralSecurityException.class,
        () -> aead.encrypt(bytes("secret-dek"), null));

    assertTrue(error.getMessage().contains("Alibaba Cloud KMS encryption failed"));
  }

  @Test
  public void rejectsInvalidBase64PlaintextFromKms() throws Exception {
    RecordingOperations operations = new RecordingOperations();
    operations.decryptPlaintextBase64 = "not-base64";
    AliCloudKmsAead aead = new AliCloudKmsAead(keyUri(), operations);

    GeneralSecurityException error = assertThrows(
        GeneralSecurityException.class,
        () -> aead.decrypt(bytes("ciphertext-from-kms"), null));

    assertTrue(error.getMessage().contains("invalid base64 plaintext"));
  }

  private static AliCloudKmsKeyUri keyUri() throws Exception {
    return AliCloudKmsKeyUri.parse("alicloud-kms://cn-chengdu/alias%2Fcsfle");
  }

  private static byte[] bytes(String value) {
    return value.getBytes(StandardCharsets.UTF_8);
  }

  private static String base64(String value) {
    return Base64.getEncoder().encodeToString(bytes(value));
  }

  private static final class RecordingOperations implements AliCloudKmsOperations {

    private String encryptKeyId;
    private String encryptPlaintextBase64;
    private Map<String, ?> encryptContext;
    private RuntimeException encryptFailure;
    private String decryptCiphertextBlob;
    private Map<String, ?> decryptContext;
    private String decryptPlaintextBase64 = base64("secret-dek");
    private String decryptKeyId;

    @Override
    public String encrypt(String keyId, String plaintextBase64, Map<String, ?> encryptionContext) {
      if (encryptFailure != null) {
        throw encryptFailure;
      }
      this.encryptKeyId = keyId;
      this.encryptPlaintextBase64 = plaintextBase64;
      this.encryptContext = encryptionContext;
      return "ciphertext-from-kms";
    }

    @Override
    public DecryptResult decrypt(String ciphertextBlob, Map<String, ?> encryptionContext) {
      this.decryptCiphertextBlob = ciphertextBlob;
      this.decryptContext = encryptionContext;
      return new DecryptResult(decryptPlaintextBase64, decryptKeyId);
    }
  }
}
