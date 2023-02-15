/*
 * Copyright 2023 Confluent Inc.
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
package io.confluent.kafka.schemaregistry.encryption;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSortedSet;
import com.google.common.testing.FakeTicker;
import io.confluent.kafka.schemaregistry.ClusterTestHarness;
import io.confluent.kafka.schemaregistry.ParsedSchema;
import io.confluent.kafka.schemaregistry.avro.AvroSchema;
import io.confluent.kafka.schemaregistry.avro.AvroSchemaProvider;
import io.confluent.kafka.schemaregistry.client.CachedSchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.rest.entities.Metadata;
import io.confluent.kafka.schemaregistry.client.rest.entities.Rule;
import io.confluent.kafka.schemaregistry.client.rest.entities.RuleSet;
import io.confluent.kafka.schemaregistry.client.rest.entities.requests.ConfigUpdateRequest;
import io.confluent.kafka.schemaregistry.client.rest.entities.requests.RegisterSchemaRequest;
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;
import io.confluent.kafka.schemaregistry.json.JsonSchemaProvider;
import io.confluent.kafka.schemaregistry.protobuf.ProtobufSchemaProvider;
import io.confluent.kafka.schemaregistry.storage.KafkaSchemaRegistry;
import io.confluent.kafka.schemaregistry.storage.RuleSetHandler;
import io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig;
import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.IndexedRecord;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.junit.Before;
import org.junit.Test;

public abstract class RestApiFieldEncryptionTest extends ClusterTestHarness {

  public RestApiFieldEncryptionTest() {
    super(1, true);
  }

  protected abstract FieldEncryptionProperties getFieldEncryptionProperties();

  @Before
  public void setUp() throws Exception {
    super.setUp();
    ((KafkaSchemaRegistry) restApp.schemaRegistry()).setRuleSetHandler(new RuleSetHandler() {
      public void handle(ConfigUpdateRequest request) {
      }

      public void handle(RegisterSchemaRequest request) {
      }

      public io.confluent.kafka.schemaregistry.storage.RuleSet transform(RuleSet ruleSet) {
        return ruleSet != null
            ? new io.confluent.kafka.schemaregistry.storage.RuleSet(ruleSet)
            : null;
      }
    });
  }

  @Test
  public void testFieldEncryption() throws Exception {
    String topic = "test";
    FieldEncryptionProperties fieldEncryptionProps = getFieldEncryptionProperties();
    List<String> ruleNames = ImmutableList.of("myRule");
    Map<String, Object> clientProps = fieldEncryptionProps.getClientProperties(ruleNames);
    FakeTicker fakeTicker = new FakeTicker();
    SchemaRegistryClient schemaRegistry = new CachedSchemaRegistryClient(
        restApp.restClient,
        10,
        ImmutableList.of(
            new AvroSchemaProvider(), new ProtobufSchemaProvider(), new JsonSchemaProvider()),
        Collections.emptyMap(),
        Collections.emptyMap(),
        fakeTicker
    );

    KafkaAvroSerializer avroSerializer = new KafkaAvroSerializer(schemaRegistry, clientProps);
    KafkaAvroDeserializer avroDeserializer = new KafkaAvroDeserializer(schemaRegistry, clientProps);

    Map<String, Object> badClientProps = new HashMap<>(clientProps);
    badClientProps.remove(AbstractKafkaSchemaSerDeConfig.RULE_EXECUTORS);
    KafkaAvroDeserializer badDeserializer = new KafkaAvroDeserializer(schemaRegistry, badClientProps);

    String subject = "test-value";
    AvroSchema schema = createUserSchema();
    registerAndVerifySchema(schemaRegistry, schema, 1, subject);

    IndexedRecord avroRecord = createUserRecord();
    RecordHeaders headers = new RecordHeaders();
    byte[] bytes = avroSerializer.serialize(topic, headers, avroRecord);
    GenericRecord record = (GenericRecord) avroDeserializer.deserialize(topic, headers, bytes);
    assertEquals("testUser", record.get("name").toString());

    Rule rule = new Rule("myRule", null, null, null,
        FieldEncryptionExecutor.TYPE, ImmutableSortedSet.of("PII"), null, null, "NONE,NONE", false);
    RuleSet ruleSet = new RuleSet(Collections.emptyList(), ImmutableList.of(rule));
    Metadata metadata = new Metadata(
        Collections.emptyMap(), Collections.emptyMap(), Collections.emptySet());
    AvroSchema ruleSchema = new AvroSchema(
        null, Collections.emptyList(), Collections.emptyMap(), metadata, ruleSet, null, true);
    registerAndVerifySchema(schemaRegistry, ruleSchema, 2, subject);

    fakeTicker.advance(61, TimeUnit.SECONDS);
    bytes = avroSerializer.serialize(topic, headers, avroRecord);
    record = (GenericRecord) avroDeserializer.deserialize(topic, headers, bytes);
    assertEquals("testUser", record.get("name").toString());
    record = (GenericRecord) badDeserializer.deserialize(topic, headers, bytes);
    assertNotEquals("testUser", record.get("name").toString());  // still encrypted
  }

  private AvroSchema createUserSchema() {
    String userSchema = "{\"namespace\": \"example.avro\", \"type\": \"record\", "
        + "\"name\": \"User\","
        + "\"fields\": [{\"name\": \"name\", \"type\": [\"null\", \"string\"], "
        + "\"confluent:tags\": [\"PII\"]}]}";
    org.apache.avro.Schema.Parser parser = new org.apache.avro.Schema.Parser();
    org.apache.avro.Schema schema = parser.parse(userSchema);
    return new AvroSchema(schema);
  }

  private IndexedRecord createUserRecord() {
    org.apache.avro.Schema schema = createUserSchema().rawSchema();
    GenericRecord avroRecord = new GenericData.Record(schema);
    avroRecord.put("name", "testUser");
    return avroRecord;
  }

  static void registerAndVerifySchema(SchemaRegistryClient schemaRegistry, ParsedSchema schema,
      int expectedId, String subject)
      throws IOException, RestClientException {
    int registeredId = schemaRegistry.register(subject, schema);
    assertEquals("Registering a new schema should succeed", expectedId, registeredId);

    ParsedSchema newSchema = schemaRegistry.getSchemaBySubjectAndId(subject, expectedId);
    assertNotNull("Registered schema should be found", newSchema);
  }
}

