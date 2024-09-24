/*
 * Copyright 2023 Confluent Inc.
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

package io.confluent.kafka.schemaregistry.encryption.local;

import static org.junit.Assert.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSortedSet;
import io.confluent.kafka.schemaregistry.avro.AvroSchema;
import io.confluent.kafka.schemaregistry.client.rest.entities.Metadata;
import io.confluent.kafka.schemaregistry.client.rest.entities.Rule;
import io.confluent.kafka.schemaregistry.client.rest.entities.RuleSet;
import io.confluent.kafka.schemaregistry.encryption.FieldEncryptionExecutor;
import io.confluent.kafka.schemaregistry.encryption.FieldEncryptionExecutorTest;
import io.confluent.kafka.schemaregistry.encryption.FieldEncryptionProperties;
import io.confluent.kafka.schemaregistry.encryption.tink.Cryptor;
import io.confluent.kafka.schemaregistry.encryption.tink.DekFormat;
import java.util.Collections;
import java.util.List;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.IndexedRecord;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.junit.Test;

public class LocalFieldEncryptionExecutorTest extends FieldEncryptionExecutorTest {

  public LocalFieldEncryptionExecutorTest() throws Exception {
    super();
  }

  @Override
  protected FieldEncryptionProperties getFieldEncryptionProperties(
      List<String> ruleNames, Class<?> ruleExecutor) {
    return new LocalFieldEncryptionProperties(ruleNames, ruleExecutor);
  }

  @Test
  public void testKafkaAvroSerializerF1Preserialized() throws Exception {
    AvroSchema avroSchema = new AvroSchema(createF1Schema());
    Rule rule = new Rule("rule1", null, null, null,
        FieldEncryptionExecutor.TYPE, ImmutableSortedSet.of("PII"), null, null, null, null, false);
    RuleSet ruleSet = new RuleSet(Collections.emptyList(), ImmutableList.of(rule));
    Metadata metadata = getMetadata("kek1");
    avroSchema = avroSchema.copy(metadata, ruleSet);
    schemaRegistry.register(topic + "-value", avroSchema);
    dekRegistry.createKek("kek1", fieldEncryptionProps.getKmsType(), fieldEncryptionProps.getKmsKeyId(), null, null, false);
    String encryptedDek = "07V2ndh02DA73p+dTybwZFm7DKQSZN1tEwQh+FoX1DZLk4Yj2LLu4omYjp/84tAg3BYlkfGSz+zZacJHIE4=";
    dekRegistry.createDek("kek1", topic + "-value", 1, DekFormat.AES256_GCM, encryptedDek);

    int expectedEncryptions = 1;
    RecordHeaders headers = new RecordHeaders();
    byte[] bytes = new byte[]{0, 0, 0, 0, 1, 104, 122, 103, 121, 47, 106, 70, 78, 77, 86, 47, 101, 70, 105, 108, 97, 72, 114, 77, 121, 101, 66, 103, 100, 97, 86, 122, 114, 82, 48, 117, 100, 71, 101, 111, 116, 87, 56, 99, 65, 47, 74, 97, 108, 55, 117, 107, 114, 43, 77, 47, 121, 122};

    Cryptor cryptor = addSpyToCryptor(avroDeserializer);
    GenericRecord record = (GenericRecord) avroDeserializer.deserialize(topic, headers, bytes);
    verify(cryptor, times(expectedEncryptions)).decrypt(any(), any(), any());
    assertEquals("hello world", record.get("f1"));
  }

  @Test
  public void testKafkaAvroSerializerDeterministicF1Preserialized() throws Exception {
    AvroSchema avroSchema = new AvroSchema(createF1Schema());
    Rule rule = new Rule("rule1", null, null, null,
        FieldEncryptionExecutor.TYPE, ImmutableSortedSet.of("PII"), null, null, null, null, false);
    RuleSet ruleSet = new RuleSet(Collections.emptyList(), ImmutableList.of(rule));
    Metadata metadata = getMetadata("kek1", DekFormat.AES256_SIV);
    avroSchema = avroSchema.copy(metadata, ruleSet);
    schemaRegistry.register(topic + "-value", avroSchema);
    dekRegistry.createKek("kek1", fieldEncryptionProps.getKmsType(), fieldEncryptionProps.getKmsKeyId(), null, null, false);
    String encryptedDek = "YSx3DTlAHrmpoDChquJMifmPntBzxgRVdMzgYL82rgWBKn7aUSnG+WIu9ozBNS3y2vXd++mBtK07w4/W/G6w0da39X9hfOVZsGnkSvry/QRht84V8yz3dqKxGMOK5A==";
    dekRegistry.createDek("kek1", topic + "-value", 1, DekFormat.AES256_SIV, encryptedDek);

    int expectedEncryptions = 1;
    RecordHeaders headers = new RecordHeaders();
    byte[] bytes = new byte[]{0, 0, 0, 0, 1, 72, 68, 54, 89, 116, 120, 114, 108, 66, 110, 107, 84, 87, 87, 57, 78, 54, 86, 98, 107, 51, 73, 73, 110, 106, 87, 72, 56, 49, 120, 109, 89, 104, 51, 107, 52, 100};

    Cryptor cryptor = addSpyToCryptor(avroDeserializer, DekFormat.AES256_SIV);
    GenericRecord record = (GenericRecord) avroDeserializer.deserialize(topic, headers, bytes);
    verify(cryptor, times(expectedEncryptions)).decrypt(any(), any(), any());
    assertEquals("hello world", record.get("f1"));
  }

  @Test
  public void testKafkaAvroDekRotationF1Preserialized() throws Exception {
    AvroSchema avroSchema = new AvroSchema(createF1Schema());
    Rule rule = new Rule("rule1", null, null, null,
        FieldEncryptionExecutor.TYPE, ImmutableSortedSet.of("PII"),
        ImmutableMap.of("encrypt.dek.expiry.days", "1", "preserve.source.fields", "true"),
        null, null, null, false);
    RuleSet ruleSet = new RuleSet(Collections.emptyList(), ImmutableList.of(rule));
    Metadata metadata = getMetadata("kek1");
    avroSchema = avroSchema.copy(metadata, ruleSet);
    schemaRegistry.register(topic + "-value", avroSchema);
    dekRegistry.createKek("kek1", fieldEncryptionProps.getKmsType(), fieldEncryptionProps.getKmsKeyId(), null, null, false);
    String encryptedDek = "W/v6hOQYq1idVAcs1pPWz9UUONMVZW4IrglTnG88TsWjeCjxmtRQ4VaNe/I5dCfm2zyY9Cu0nqdvqImtUk4=";
    dekRegistry.createDek("kek1", topic + "-value", 1, DekFormat.AES256_GCM, encryptedDek);

    int expectedEncryptions = 1;
    RecordHeaders headers = new RecordHeaders();
    byte[] bytes = new byte[]{0, 0, 0, 0, 1, 120, 65, 65, 65, 65, 65, 65, 71, 52, 72, 73, 54, 98, 49, 110, 88, 80, 88, 113, 76, 121, 71, 56, 99, 73, 73, 51, 53, 78, 72, 81, 115, 101, 113, 113, 85, 67, 100, 43, 73, 101, 76, 101, 70, 86, 65, 101, 78, 112, 83, 83, 51, 102, 120, 80, 110, 74, 51, 50, 65, 61};

    Cryptor cryptor = addSpyToCryptor(avroDeserializer);
    GenericRecord record = (GenericRecord) avroDeserializer.deserialize(topic, headers, bytes);
    verify(cryptor, times(expectedEncryptions)).decrypt(any(), any(), any());
    assertEquals("hello world", record.get("f1"));
  }
}

