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

package io.confluent.kafka.schemaregistry.encryption;

import io.confluent.kafka.schemaregistry.rules.FieldRuleExecutor;
import io.confluent.kafka.schemaregistry.rules.FieldTransform;
import io.confluent.kafka.schemaregistry.rules.RuleContext;
import io.confluent.kafka.schemaregistry.rules.RuleContext.FieldContext;
import io.confluent.kafka.schemaregistry.rules.RuleException;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * In envelope encryption, a user generates a data encryption key (DEK) locally, encrypts data with
 * the DEK, sends the DEK to a KMS to be encrypted (with a key managed by KMS - KEK), and then
 * stores the encrypted DEK. At a later point, a user can retrieve the encrypted DEK for the
 * encrypted data, use the KEK from KMS to decrypt the DEK, and use the decrypted DEK to decrypt
 * the data.
 */
public class FieldEncryptionExecutor extends FieldRuleExecutor {

  private static final Logger log = LoggerFactory.getLogger(FieldEncryptionExecutor.class);

  public static final String TYPE = "ENCRYPT";

  public static final String ENCRYPT_KEK_NAME = EncryptionExecutor.ENCRYPT_KEK_NAME;
  public static final String ENCRYPT_KMS_KEY_ID = EncryptionExecutor.ENCRYPT_KMS_KEY_ID;
  public static final String ENCRYPT_KMS_TYPE = EncryptionExecutor.ENCRYPT_KMS_TYPE;
  public static final String ENCRYPT_DEK_ALGORITHM = EncryptionExecutor.ENCRYPT_DEK_ALGORITHM;
  public static final String ENCRYPT_DEK_EXPIRY_DAYS = EncryptionExecutor.ENCRYPT_DEK_EXPIRY_DAYS;

  public static final String KMS_TYPE_SUFFIX = EncryptionExecutor.KMS_TYPE_SUFFIX;
  public static final byte[] EMPTY_AAD = EncryptionExecutor.EMPTY_AAD;
  public static final String CACHE_EXPIRY_SECS = EncryptionExecutor.CACHE_EXPIRY_SECS;
  public static final String CACHE_SIZE = EncryptionExecutor.CACHE_SIZE;
  public static final String CLOCK = EncryptionExecutor.CLOCK;

  private EncryptionExecutor encryptionExecutor = new EncryptionExecutor();

  public FieldEncryptionExecutor() {
  }

  @Override
  public boolean addOriginalConfigs() {
    return true;
  }

  @Override
  public void configure(Map<String, ?> configs) {
    encryptionExecutor.configure(configs);
  }

  @Override
  public String type() {
    return TYPE;
  }

  // Visible for testing
  public EncryptionExecutor getEncryptionExecutor() {
    return encryptionExecutor;
  }

  @Override
  public FieldEncryptionExecutorTransform newTransform(RuleContext ctx) throws RuleException {
    FieldEncryptionExecutorTransform transform = new FieldEncryptionExecutorTransform();
    transform.init(ctx);
    return transform;
  }

  @Override
  public void close() throws RuleException {
    encryptionExecutor.close();
  }

  public class FieldEncryptionExecutorTransform implements FieldTransform {
    private EncryptionExecutor.EncryptionExecutorTransform transform;

    public void init(RuleContext ctx) throws RuleException {
      transform = encryptionExecutor.newTransform(ctx);
    }

    @Override
    public Object transform(RuleContext ctx, FieldContext fieldCtx, Object fieldValue)
        throws RuleException {
      return transform.transform(ctx, fieldCtx.getType(), fieldValue);
    }
  }
}

