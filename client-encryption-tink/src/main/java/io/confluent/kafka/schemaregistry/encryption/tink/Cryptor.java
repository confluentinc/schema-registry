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

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.crypto.tink.Aead;
import com.google.crypto.tink.DeterministicAead;
import com.google.crypto.tink.KeyTemplates;
import com.google.crypto.tink.Registry;
import com.google.crypto.tink.TinkProtoParametersFormat;
import com.google.crypto.tink.aead.AeadConfig;
import com.google.crypto.tink.daead.DeterministicAeadConfig;
import com.google.crypto.tink.proto.KeyTemplate;
import com.google.protobuf.ByteString;
import com.google.protobuf.ExtensionRegistryLite;
import com.google.protobuf.InvalidProtocolBufferException;
import java.nio.BufferUnderflowException;
import java.security.GeneralSecurityException;
import java.util.concurrent.ExecutionException;

public class Cryptor {

  static {
    try {
      AeadConfig.register();
      DeterministicAeadConfig.register();
    } catch (GeneralSecurityException e) {
      throw new IllegalArgumentException(e);
    }
  }

  private final DekFormat dekFormat;
  private final KeyTemplate dekTemplate;
  private final LoadingCache<ByteString, DeterministicAead> deterministicAeads;
  private final LoadingCache<ByteString, Aead> aeads;

  private static final int DEFAULT_CACHE_SIZE = 100;

  public Cryptor(DekFormat dekFormat) throws GeneralSecurityException {
    KeyTemplate keyTemplate;
    try {
      keyTemplate = KeyTemplate.parseFrom(
          TinkProtoParametersFormat.serialize(dekFormat.getParameters()),
          ExtensionRegistryLite.getEmptyRegistry());
    } catch (GeneralSecurityException e) {
      // Use deprecated Tink APIs to support older versions of Tink
      com.google.crypto.tink.KeyTemplate template = KeyTemplates.get(dekFormat.name());
      keyTemplate = com.google.crypto.tink.proto.KeyTemplate.newBuilder()
          .setTypeUrl(template.getTypeUrl())
          .setValue(ByteString.copyFrom(template.getValue()))
          .build();
    } catch (InvalidProtocolBufferException e) {
      throw new GeneralSecurityException(e);
    }
    this.dekFormat = dekFormat;
    this.dekTemplate = keyTemplate;
    this.deterministicAeads = CacheBuilder.newBuilder()
        .maximumSize(DEFAULT_CACHE_SIZE)
        .build(new CacheLoader<ByteString, DeterministicAead>() {
          @Override
          public DeterministicAead load(ByteString dek) throws GeneralSecurityException {
            return Registry.getPrimitive(dekTemplate.getTypeUrl(), dek, DeterministicAead.class);
          }
        });
    this.aeads = CacheBuilder.newBuilder()
        .maximumSize(DEFAULT_CACHE_SIZE)
        .build(new CacheLoader<ByteString, Aead>() {
          @Override
          public Aead load(ByteString dek) throws GeneralSecurityException {
            return Registry.getPrimitive(dekTemplate.getTypeUrl(), dek, Aead.class);
          }
        });
  }

  public DekFormat getDekFormat() {
    return dekFormat;
  }

  public byte[] generateKey() throws GeneralSecurityException {
    return Registry.newKeyData(dekTemplate).getValue().toByteArray();
  }

  public byte[] encrypt(byte[] dek, byte[] plaintext, byte[] associatedData)
      throws GeneralSecurityException {
    // Use DEK to encrypt plaintext.
    byte[] ciphertext;
    if (dekFormat.isDeterministic()) {
      DeterministicAead aead = getDeterministicAead(dek);
      ciphertext = aead.encryptDeterministically(plaintext, associatedData);
    } else {
      Aead aead = getAead(dek);
      ciphertext = aead.encrypt(plaintext, associatedData);
    }
    return ciphertext;
  }

  public byte[] decrypt(byte[] dek, byte[] ciphertext, byte[] associatedData)
      throws GeneralSecurityException {
    try {
      // Use DEK to decrypt ciphertext.
      if (dekFormat.isDeterministic()) {
        DeterministicAead aead = getDeterministicAead(dek);
        return aead.decryptDeterministically(ciphertext, associatedData);
      } else {
        Aead aead = getAead(dek);
        return aead.decrypt(ciphertext, associatedData);
      }
    } catch (IndexOutOfBoundsException
             | BufferUnderflowException
             | NegativeArraySizeException e) {
      throw new GeneralSecurityException("invalid ciphertext", e);
    }
  }

  private DeterministicAead getDeterministicAead(byte[] dek) throws GeneralSecurityException {
    try {
      return deterministicAeads.get(ByteString.copyFrom(dek));
    } catch (ExecutionException e) {
      if (e.getCause() instanceof GeneralSecurityException) {
        throw (GeneralSecurityException) e.getCause();
      }
      throw new RuntimeException(e.getCause());
    }
  }

  private Aead getAead(byte[] dek) throws GeneralSecurityException {
    try {
      return aeads.get(ByteString.copyFrom(dek));
    } catch (ExecutionException e) {
      if (e.getCause() instanceof GeneralSecurityException) {
        throw (GeneralSecurityException) e.getCause();
      }
      throw new RuntimeException(e.getCause());
    }
  }
}
