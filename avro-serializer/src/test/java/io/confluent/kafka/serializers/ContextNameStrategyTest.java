/*
 * Copyright 2021 Confluent Inc.
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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

import io.confluent.kafka.schemaregistry.avro.AvroSchema;
import io.confluent.kafka.schemaregistry.client.MockSchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.SchemaMetadata;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.rest.entities.SchemaReference;
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;
import io.confluent.kafka.serializers.context.strategy.ContextNameStrategy;
import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Properties;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.IndexedRecord;
import org.junit.Test;

public class ContextNameStrategyTest {

  private final String topic;
  private final String topic2;
  private final SchemaRegistryClient schemaRegistry;
  private final KafkaAvroSerializer avroSerializer1;
  private final KafkaAvroDeserializer avroDeserializer1;
  private final KafkaAvroSerializer avroSerializer2;
  private final KafkaAvroSerializer avroSerializer3;
  private final KafkaAvroSerializer avroSerializer4;

  public ContextNameStrategyTest() {
    topic = "test";
    topic2 = "test2";
    schemaRegistry = new MockSchemaRegistryClient();
    Properties config1 = new Properties();
    config1.put(KafkaAvroDeserializerConfig.SCHEMA_REGISTRY_URL_CONFIG, "bogus");
    config1.put(AbstractKafkaSchemaSerDeConfig.CONTEXT_NAME_STRATEGY,
        CustomContextNameStrategy1.class.getName());
    avroSerializer1 = new KafkaAvroSerializer(schemaRegistry, new HashMap(config1));
    avroDeserializer1 = new KafkaAvroDeserializer(schemaRegistry, new HashMap(config1));

    Properties config2 = new Properties();
    config2.put(KafkaAvroDeserializerConfig.SCHEMA_REGISTRY_URL_CONFIG, "bogus");
    config2.put(AbstractKafkaSchemaSerDeConfig.CONTEXT_NAME_STRATEGY,
        CustomContextNameStrategy2.class.getName());
    avroSerializer2 = new KafkaAvroSerializer(schemaRegistry, new HashMap(config2));

    Properties config3 = new Properties();
    config3.put(KafkaAvroDeserializerConfig.SCHEMA_REGISTRY_URL_CONFIG, "bogus");
    config3.put(AbstractKafkaSchemaSerDeConfig.CONTEXT_NAME_STRATEGY,
        CustomContextNameStrategy3.class.getName());
    avroSerializer3 = new KafkaAvroSerializer(schemaRegistry, new HashMap(config3));

    Properties config4 = new Properties();
    config4.put(KafkaAvroDeserializerConfig.SCHEMA_REGISTRY_URL_CONFIG, "bogus");
    config4.put(AbstractKafkaSchemaSerDeConfig.CONTEXT_NAME_STRATEGY,
        CustomContextNameStrategy4.class.getName());
    avroSerializer4 = new KafkaAvroSerializer(schemaRegistry, new HashMap(config4));
  }

  private Schema createUserSchema() {
    String userSchema = "{\"namespace\": \"example.avro\", \"type\": \"record\", " +
        "\"name\": \"User\"," +
        "\"fields\": [{\"name\": \"name\", \"type\": \"string\"}]}";
    Schema.Parser parser = new Schema.Parser();
    Schema schema = parser.parse(userSchema);
    return schema;
  }

  private IndexedRecord createUserRecord() {
    Schema schema = createUserSchema();
    GenericRecord avroRecord = new GenericData.Record(schema);
    avroRecord.put("name", "testUser");
    return avroRecord;
  }

  @Test
  public void testCustomContextNameStrategy() {
    assertEquals(":.customContext_test:", avroSerializer1.getContextName(topic));
    assertEquals(":.customContext_test:subject1", avroSerializer1.getContextName(topic, "subject1"));

    assertEquals(":.customContext_test:", avroSerializer2.getContextName(topic));
    assertEquals(":.customContext_test:subject1", avroSerializer2.getContextName(topic, "subject1"));

    assertEquals(":.customContext_test:", avroSerializer3.getContextName(topic));
    assertEquals(":.customContext_test:subject1", avroSerializer3.getContextName(topic, "subject1"));

    assertEquals(":.customContext_test:", avroSerializer4.getContextName(topic));
    assertEquals(":.customContext_test:subject1", avroSerializer4.getContextName(topic, "subject1"));
  }

  @Test
  public void testSerialization() {
    byte[] bytes;
    IndexedRecord avroRecord = createUserRecord();
    bytes = avroSerializer1.serialize(topic, avroRecord);
    assertEquals(avroRecord, avroDeserializer1.deserialize(topic, bytes));

    bytes = avroSerializer1.serialize(topic2, avroRecord);
    assertEquals(avroRecord, avroDeserializer1.deserialize(topic2, bytes));
  }

  public static class CustomContextNameStrategy1 implements ContextNameStrategy {
    @Override
    public String contextName(String topic) {
      return "customContext_" + topic;
    }
  }


  public static class CustomContextNameStrategy2 implements ContextNameStrategy {
    @Override
    public String contextName(String topic) {
      return ".customContext_" + topic;
    }
  }

  public static class CustomContextNameStrategy3 implements ContextNameStrategy {
    @Override
    public String contextName(String topic) {
      return ":.customContext_" + topic + ":";
    }
  }

  public static class CustomContextNameStrategy4 implements ContextNameStrategy {
    @Override
    public String contextName(String topic) {
      return ":customContext_" + topic + ":";
    }
  }

  // A latest version's references are read in its subject's context, even once the same body
  // with the same references has been parsed in the default context.
  @Test
  public void testLatestVersionReadsReferencesInItsSubjectsContext() throws Exception {
    SchemaRegistryClient client = new MockSchemaRegistryClient();
    String inDefault = "{\"type\": \"record\", \"name\": \"Y\", "
        + "\"fields\": [{\"name\": \"g\", \"type\": \"string\"}]}";
    String inContext = "{\"type\": \"record\", \"name\": \"Y\", "
        + "\"fields\": [{\"name\": \"f\", \"type\": \"int\"}]}";
    String body = "{\"type\": \"record\", \"name\": \"X\", "
        + "\"fields\": [{\"name\": \"a\", \"type\": \"Y\"}]}";
    List<SchemaReference> refs = Collections.singletonList(new SchemaReference("Y", "y", 1));
    client.register("y", new AvroSchema(inDefault));
    client.register(":.ctx:y", new AvroSchema(inContext));
    client.register("x",
        new AvroSchema(body, refs, Collections.singletonMap("Y", inDefault), null));
    client.register(":.ctx:x",
        new AvroSchema(body, refs, Collections.singletonMap("Y", inContext), null));
    AbstractKafkaSchemaSerDe.lookupLatestVersion(client, "x", null, null, false);
    Schema latest = ((AvroSchema) AbstractKafkaSchemaSerDe.lookupLatestVersion(
        client, ":.ctx:x", null, null, false).getSchema()).rawSchema();
    assertNotNull(latest.getField("a").schema().getField("f"));
  }

  // A latest version found through an alias reads its references in its target's context, the
  // subject the registry names, not the alias's.
  @Test
  public void testLatestVersionOfAnAliasReadsReferencesInItsTargetsContext() throws Exception {
    // The registry answers a lookup of the alias with the target's version, naming the target.
    SchemaRegistryClient client = new MockSchemaRegistryClient() {
      @Override
      public SchemaMetadata getLatestSchemaMetadata(String subject)
          throws IOException, RestClientException {
        return super.getLatestSchemaMetadata(subject.equals(":.a:alias") ? ":.b:x" : subject);
      }
    };
    String inA = "{\"type\": \"record\", \"name\": \"Y\", "
        + "\"fields\": [{\"name\": \"g\", \"type\": \"string\"}]}";
    String inB = "{\"type\": \"record\", \"name\": \"Y\", "
        + "\"fields\": [{\"name\": \"f\", \"type\": \"int\"}]}";
    String body = "{\"type\": \"record\", \"name\": \"X\", "
        + "\"fields\": [{\"name\": \"a\", \"type\": \"Y\"}]}";
    List<SchemaReference> refs = Collections.singletonList(new SchemaReference("Y", "y", 1));
    client.register(":.a:y", new AvroSchema(inA));
    client.register(":.b:y", new AvroSchema(inB));
    client.register(":.b:x", new AvroSchema(body, refs, Collections.singletonMap("Y", inB), null));
    Schema latest = ((AvroSchema) AbstractKafkaSchemaSerDe.lookupLatestVersion(
        client, ":.a:alias", null, null, false).getSchema()).rawSchema();
    assertNotNull(latest.getField("a").schema().getField("f"));
  }
}
