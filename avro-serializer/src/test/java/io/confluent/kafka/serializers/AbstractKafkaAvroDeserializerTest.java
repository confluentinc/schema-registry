/*
 * Copyright 2018-2020 Confluent Inc.
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

package io.confluent.kafka.serializers;

import static java.util.Collections.singletonMap;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.sameInstance;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;

import io.confluent.kafka.schemaregistry.ParsedSchema;
import io.confluent.kafka.schemaregistry.avro.AvroSchema;
import io.confluent.kafka.schemaregistry.client.MockSchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;
import io.confluent.kafka.schemaregistry.testutil.MockSchemaRegistry;
import io.confluent.kafka.serializers.subject.RecordNameStrategy;
import io.confluent.kafka.serializers.subject.TopicNameStrategy;
import io.confluent.kafka.serializers.subject.TopicRecordNameStrategy;
import io.confluent.kafka.serializers.subject.strategy.SubjectNameStrategy;
import java.io.IOException;
import java.lang.reflect.Field;
import java.util.Map;

import com.google.common.collect.ImmutableMap;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.IndexedRecord;
import org.apache.kafka.common.config.ConfigException;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class AbstractKafkaAvroDeserializerTest {
  private Map<String, ?> defaultConfigs;
  private SchemaRegistryClient schemaRegistry;
  private KafkaAvroSerializer avroSerializer;
  private Deserializer deserializer;

  @Before
  public void setUp() {
    defaultConfigs = ImmutableMap.of(
        KafkaAvroDeserializerConfig.SCHEMA_REGISTRY_URL_CONFIG, "bogus");
    schemaRegistry = new MockSchemaRegistryClient();
    avroSerializer = new KafkaAvroSerializer(schemaRegistry, defaultConfigs);
    deserializer = new Deserializer(schemaRegistry);
  }

  private static class Deserializer extends AbstractKafkaAvroDeserializer {
    Deserializer(SchemaRegistryClient schemaRegistry) {
      this.schemaRegistry = schemaRegistry;
    }
  }

  private IndexedRecord createAvroRecord() {
    String userSchema = "{\"namespace\": \"example.avro\", \"type\": \"record\", " +
        "\"name\": \"User\"," +
        "\"fields\": [{\"name\": \"name\", \"type\": \"string\"}]}";
    Schema.Parser parser = new Schema.Parser();
    Schema schema = parser.parse(userSchema);
    GenericRecord avroRecord = new GenericData.Record(schema);
    avroRecord.put("name", "testUser");
    return avroRecord;
  }

  public void assertSchemaNotCopiedWhenDeserializedWithVersion(
      String topic,
      SubjectNameStrategy subjectNameStrategy) throws IOException,
      RestClientException {
    Map configs = ImmutableMap.builder()
        .putAll(defaultConfigs)
        .put(KafkaAvroSerializerConfig.AUTO_REGISTER_SCHEMAS, false)
        .put(
            AbstractKafkaSchemaSerDeConfig.VALUE_SUBJECT_NAME_STRATEGY,
            subjectNameStrategy.getClass())
        .build();
    IndexedRecord avroRecord = createAvroRecord();
    String subject = subjectNameStrategy.subjectName(topic, false,
        new AvroSchema(avroRecord.getSchema()));
    avroSerializer.configure(configs, false);
    deserializer.configure(new KafkaAvroDeserializerConfig(configs));
    schemaRegistry.register(subject, new AvroSchema(avroRecord.getSchema()));
    byte[] bytes = avroSerializer.serialize(topic, avroRecord);
    IndexedRecord deserialized
        = (IndexedRecord) deserializer.deserializeWithSchemaAndVersion(topic, false, bytes).container();

    assertThat(deserialized.getSchema(), sameInstance(avroRecord.getSchema()));
  }

  @Test
  public void testSchemaNotCopiedForTopicNameStrategy() throws IOException, RestClientException {
    assertSchemaNotCopiedWhenDeserializedWithVersion(
        "test-topic",
        new TopicNameStrategy()
    );
  }

  @Test
  public void testSchemaNotCopiedForRecordNameStrategy()
      throws IOException, RestClientException {
    assertSchemaNotCopiedWhenDeserializedWithVersion(
        "test-topic",
        new RecordNameStrategy()
    );
  }

  @Test
  public void testSchemaNotCopiedForTopicRecordNameStrategy()
      throws IOException, RestClientException {
    assertSchemaNotCopiedWhenDeserializedWithVersion(
        "test-topic",
        new TopicRecordNameStrategy()
    );
  }

  private int getSchemaInternalHashCode(org.apache.avro.Schema avroSchema)
      throws NoSuchFieldException, IllegalAccessException {
    Field hashCodeField = org.apache.avro.Schema.class.getDeclaredField("hashCode");
    boolean accessible = hashCodeField.isAccessible();
    hashCodeField.setAccessible(true);
    try {
      return (int) hashCodeField.get(avroSchema);
    } finally {
      hashCodeField.setAccessible(accessible);
    }
  }

  @Test
  public void testSchemaVersionSet() throws IOException, RestClientException {
    IndexedRecord avroRecord = createAvroRecord();
    int version = schemaRegistry.register("topic", new AvroSchema(avroRecord.getSchema()));
    byte[] bytes = avroSerializer.serialize("topic", avroRecord);

    GenericContainerWithVersion genericContainerWithVersion
        = (GenericContainerWithVersion) deserializer.deserializeWithSchemaAndVersion(
            "topic", false, bytes);

    org.apache.avro.Schema avroSchema = genericContainerWithVersion.container().getSchema();
    Integer schemaVersion = genericContainerWithVersion.version();
    assertThat(schemaVersion, equalTo(version));
  }

  @Test
  public void testHashCodeNotReset() throws NoSuchFieldException, IllegalAccessException {
    IndexedRecord avroRecord = createAvroRecord();
    byte[] bytes = avroSerializer.serialize("topic", avroRecord);
    IndexedRecord deserialized1
        = (IndexedRecord) deserializer.deserializeWithSchemaAndVersion(
            "topic", false, bytes).container();
    int hashCode = deserialized1.getSchema().hashCode();

    IndexedRecord deserialized2
        = (IndexedRecord) deserializer.deserializeWithSchemaAndVersion(
        "topic", false, bytes).container();

    assertThat(deserialized1.getSchema(), sameInstance(deserialized2.getSchema()));
    org.apache.avro.Schema avroSchema = deserialized2.getSchema();
    assertThat(getSchemaInternalHashCode(avroSchema), equalTo(hashCode));
  }

  @Test
  public void testMockUrl() {
    final KafkaAvroSerializer kafkaAvroSerializer = new KafkaAvroSerializer();
    kafkaAvroSerializer.configure(
            singletonMap(AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, "mock://asdf"),
            false
    );

    Assert.assertSame(MockSchemaRegistry.getClientForScope("asdf"), kafkaAvroSerializer.schemaRegistry);
    Assert.assertNotSame(MockSchemaRegistry.getClientForScope("qwer"), kafkaAvroSerializer.schemaRegistry);
  }

  @Test
  public void testMockUrlsAreRejected() {
    final KafkaAvroSerializer kafkaAvroSerializer = new KafkaAvroSerializer();
    try {
        kafkaAvroSerializer.configure(
                singletonMap(AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, "mock://asdf,mock://qwer"),
                false
        );
        fail();
    } catch (final ConfigException e) {
        Assert.assertEquals(
                "Only one mock scope is permitted for 'schema.registry.url'. Got: [mock://asdf, mock://qwer]",
                e.getMessage()
        );
    }

    Assert.assertNull(kafkaAvroSerializer.schemaRegistry);
  }

  @Test
  public void testMixedUrlsAreRejected() {
    final KafkaAvroSerializer kafkaAvroSerializer = new KafkaAvroSerializer();
    try {
        kafkaAvroSerializer.configure(
                singletonMap(AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, "mock://asdf,http://qwer"),
                false
        );
        fail();
    } catch (final ConfigException e) {
        Assert.assertEquals(
                "Cannot mix mock and real urls for 'schema.registry.url'. Got: [mock://asdf, http://qwer]",
                e.getMessage()
        );
    }

    try {
        kafkaAvroSerializer.configure(
                singletonMap(AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, "http://qwer,mock://asdf"),
                false
        );
        fail();
    } catch (final ConfigException e) {
        Assert.assertEquals(
                "Cannot mix mock and real urls for 'schema.registry.url'. Got: [http://qwer, mock://asdf]",
                e.getMessage()
        );
    }

    Assert.assertNull(kafkaAvroSerializer.schemaRegistry);
  }
}
