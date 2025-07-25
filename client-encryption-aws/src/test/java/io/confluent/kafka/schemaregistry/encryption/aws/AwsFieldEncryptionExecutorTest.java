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

package io.confluent.kafka.schemaregistry.encryption.aws;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSortedSet;
import io.confluent.kafka.schemaregistry.avro.AvroSchema;
import io.confluent.kafka.schemaregistry.client.rest.entities.Metadata;
import io.confluent.kafka.schemaregistry.client.rest.entities.Rule;
import io.confluent.kafka.schemaregistry.client.rest.entities.RuleSet;
import io.confluent.kafka.schemaregistry.encryption.FieldEncryptionExecutor;
import io.confluent.kafka.schemaregistry.encryption.FieldEncryptionExecutorTest;
import io.confluent.kafka.schemaregistry.encryption.FieldEncryptionProperties;
import io.confluent.kafka.schemaregistry.encryption.tink.Cryptor;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.IndexedRecord;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.junit.Test;

public class AwsFieldEncryptionExecutorTest extends FieldEncryptionExecutorTest {

  public AwsFieldEncryptionExecutorTest() throws Exception {
    super();
  }

  @Override
  protected FieldEncryptionProperties getFieldEncryptionProperties(
      List<String> ruleNames, Class<?> ruleExecutor) {
    return new AwsFieldEncryptionProperties(ruleNames, ruleExecutor);
  }

  @Test
  public void testKafkaAvroSerializerWrongKmsKeyIdWithAlternate() throws Exception {
    // Create kek with alternate kms key id
    Map<String, String> alternateKmsProps = new HashMap<>();
    alternateKmsProps.put("encrypt.alternate.kms.key.ids",
        "arn:aws:kms:us-west-2:111122223333:key/1234abcd-12ab-34cd-56ef-1234567890ab");
    dekRegistry.createKek("kek1", fieldEncryptionProps.getKmsType(),
        "wrong", alternateKmsProps, null, false);

    IndexedRecord avroRecord = createUserRecord();
    AvroSchema avroSchema = new AvroSchema(createUserSchema());
    Rule rule = new Rule("rule1", null, null, null,
        FieldEncryptionExecutor.TYPE, ImmutableSortedSet.of("PII"), null, null, null, null, false);
    RuleSet ruleSet = new RuleSet(Collections.emptyList(), ImmutableList.of(rule));
    Map<String, String> properties = new HashMap<>();
    properties.put(FieldEncryptionExecutor.ENCRYPT_KEK_NAME, "kek1");
    Metadata metadata = getMetadata(properties);
    avroSchema = avroSchema.copy(metadata, ruleSet);
    schemaRegistry.register(topic + "-value", avroSchema);

    int expectedEncryptions = 1;
    RecordHeaders headers = new RecordHeaders();
    Cryptor cryptor = addSpyToCryptor(avroSerializer);
    byte[] bytes = avroSerializer.serialize(topic, headers, avroRecord);
    verify(cryptor, times(expectedEncryptions)).encrypt(any(), any(), any());
    cryptor = addSpyToCryptor(avroDeserializer);
    GenericRecord record = (GenericRecord) avroDeserializer.deserialize(topic, headers, bytes);
    verify(cryptor, times(expectedEncryptions)).decrypt(any(), any(), any());
    assertEquals("testUser", record.get("name"));
  }

  @Test
  public void testKafkaAvroSerializerWrongKmsKeyIdWithMultipleAlternate() throws Exception {
    // Create kek with alternate kms key id
    Map<String, String> alternateKmsProps = new HashMap<>();
    alternateKmsProps.put("encrypt.alternate.kms.key.ids",
        // comma separated list of alternate key ids
        "wrong2,arn:aws:kms:us-west-2:111122223333:key/1234abcd-12ab-34cd-56ef-1234567890ab");
    dekRegistry.createKek("kek1", fieldEncryptionProps.getKmsType(),
        "wrong", alternateKmsProps, null, false);

    IndexedRecord avroRecord = createUserRecord();
    AvroSchema avroSchema = new AvroSchema(createUserSchema());
    Rule rule = new Rule("rule1", null, null, null,
        FieldEncryptionExecutor.TYPE, ImmutableSortedSet.of("PII"), null, null, null, null, false);
    RuleSet ruleSet = new RuleSet(Collections.emptyList(), ImmutableList.of(rule));
    Map<String, String> properties = new HashMap<>();
    properties.put(FieldEncryptionExecutor.ENCRYPT_KEK_NAME, "kek1");
    Metadata metadata = getMetadata(properties);
    avroSchema = avroSchema.copy(metadata, ruleSet);
    schemaRegistry.register(topic + "-value", avroSchema);

    int expectedEncryptions = 1;
    RecordHeaders headers = new RecordHeaders();
    Cryptor cryptor = addSpyToCryptor(avroSerializer);
    byte[] bytes = avroSerializer.serialize(topic, headers, avroRecord);
    verify(cryptor, times(expectedEncryptions)).encrypt(any(), any(), any());
    cryptor = addSpyToCryptor(avroDeserializer);
    GenericRecord record = (GenericRecord) avroDeserializer.deserialize(topic, headers, bytes);
    verify(cryptor, times(expectedEncryptions)).decrypt(any(), any(), any());
    assertEquals("testUser", record.get("name"));
  }

}