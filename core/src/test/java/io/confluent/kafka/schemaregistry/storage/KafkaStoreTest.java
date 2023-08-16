/*
 * Copyright 2018 Confluent Inc.
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
package io.confluent.kafka.schemaregistry.storage;

import io.confluent.kafka.schemaregistry.avro.AvroSchema;
import io.confluent.kafka.schemaregistry.id.IncrementalIdGenerator;
import io.confluent.kafka.schemaregistry.rest.SchemaRegistryConfig;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.Config;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.common.config.ConfigResource;
import org.apache.kafka.common.config.TopicConfig;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.confluent.kafka.schemaregistry.ClusterTestHarness;
import io.confluent.kafka.schemaregistry.storage.exceptions.StoreException;
import io.confluent.kafka.schemaregistry.storage.exceptions.StoreInitializationException;
import io.confluent.kafka.schemaregistry.storage.serialization.SchemaRegistrySerializer;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Iterator;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.assertNull;

public class KafkaStoreTest extends ClusterTestHarness {

  private static final Logger log = LoggerFactory.getLogger(KafkaStoreTest.class);

  private static final int ADMIN_TIMEOUT_SEC = 60;

  @Before
  public void setup() {
  }

  @After
  public void teardown() {
    log.debug("Shutting down");
  }

  @Test
  public void testInitialization() throws Exception {
    KafkaStore<String, String> kafkaStore = StoreUtils.createAndInitKafkaStoreInstance(bootstrapServers);
    kafkaStore.close();
  }

  @Test(expected = StoreInitializationException.class)
  public void testDoubleInitialization() throws Exception {
    KafkaStore<String, String> kafkaStore = StoreUtils.createAndInitKafkaStoreInstance(bootstrapServers);
    try {
      kafkaStore.init();
    } finally {
      kafkaStore.close();
    }
  }

  @Test
  public void testSimplePut() throws Exception {
    KafkaStore<String, String> kafkaStore = StoreUtils.createAndInitKafkaStoreInstance(bootstrapServers);
    String key = "Kafka";
    String value = "Rocks";
    try {
      kafkaStore.put(key, value);
      String retrievedValue = kafkaStore.get(key);
      assertEquals("Retrieved value should match entered value", value, retrievedValue);
    } finally {
      kafkaStore.close();
    }
  }

  // TODO: This requires fix for https://issues.apache.org/jira/browse/KAFKA-1788
//  @Test
//  public void testPutRetries() throws InterruptedException {
//    KafkaStore<String, String> kafkaStore = StoreUtils.createAndInitKafkaStoreInstance(bootstrapServers,
//                                                                                       zkClient);
//    String key = "Kafka";
//    String value = "Rocks";
//    try {
//      kafkaStore.put(key, value);
//    } catch (StoreException e) {
//      fail("Kafka store put(Kafka, Rocks) operation failed");
//    }
//    String retrievedValue = null;
//    try {
//      retrievedValue = kafkaStore.get(key);
//    } catch (StoreException e) {
//      fail("Kafka store get(Kafka) operation failed");
//    }
//    assertEquals("Retrieved value should match entered value", value, retrievedValue);
//    // stop the Kafka servers
//    for (KafkaServer server : servers) {
//      server.shutdown();
//    }
//    try {
//      kafkaStore.put(key, value);
//      fail("Kafka store put(Kafka, Rocks) operation should fail");
//    } catch (StoreException e) {
//      // expected since the Kafka producer will run out of retries
//    }
//    kafkaStore.close();
//  }

  @Test
  public void testSimpleGetAfterFailure() throws Exception {
    Store<String, String> inMemoryStore = new InMemoryCache<>(StringSerializer.INSTANCE);
    KafkaStore<String, String> kafkaStore = StoreUtils.createAndInitKafkaStoreInstance(
        bootstrapServers,
        inMemoryStore
    );
    String key = "Kafka";
    String value = "Rocks";
    String retrievedValue = null;
    try {
      try {
        kafkaStore.put(key, value);
      } catch (StoreException e) {
        throw new RuntimeException("Kafka store put(Kafka, Rocks) operation failed", e);
      }
      try {
        retrievedValue = kafkaStore.get(key);
      } catch (StoreException e) {
        throw new RuntimeException("Kafka store get(Kafka) operation failed", e);
      }
      assertEquals("Retrieved value should match entered value", value, retrievedValue);
    } finally {
      kafkaStore.close();
    }

    // recreate kafka store
    kafkaStore = StoreUtils.createAndInitKafkaStoreInstance(bootstrapServers, inMemoryStore);
    try {
      try {
        retrievedValue = kafkaStore.get(key);
      } catch (StoreException e) {
        throw new RuntimeException("Kafka store get(Kafka) operation failed", e);
      }
      assertEquals("Retrieved value should match entered value", value, retrievedValue);
    } finally {
      kafkaStore.close();
    }
  }

  @Test
  public void testSimpleDelete() throws Exception {
    KafkaStore<String, String> kafkaStore = StoreUtils.createAndInitKafkaStoreInstance(bootstrapServers);
    String key = "Kafka";
    String value = "Rocks";
    try {
      try {
        kafkaStore.put(key, value);
      } catch (StoreException e) {
        throw new RuntimeException("Kafka store put(Kafka, Rocks) operation failed", e);
      }
      String retrievedValue = null;
      try {
        retrievedValue = kafkaStore.get(key);
      } catch (StoreException e) {
        throw new RuntimeException("Kafka store get(Kafka) operation failed", e);
      }
      assertEquals("Retrieved value should match entered value", value, retrievedValue);
      try {
        kafkaStore.delete(key);
      } catch (StoreException e) {
        throw new RuntimeException("Kafka store delete(Kafka) operation failed", e);
      }
      // verify that value is deleted
      try {
        retrievedValue = kafkaStore.get(key);
      } catch (StoreException e) {
        throw new RuntimeException("Kafka store get(Kafka) operation failed", e);
      }
      assertNull("Value should have been deleted", retrievedValue);
    } finally {
      kafkaStore.close();
    }
  }

  @Test
  public void testDeleteAfterRestart() throws Exception {
    Store<String, String> inMemoryStore = new InMemoryCache<>(StringSerializer.INSTANCE);
    KafkaStore<String, String> kafkaStore = StoreUtils.createAndInitKafkaStoreInstance(
        bootstrapServers,
        inMemoryStore
    );
    String key = "Kafka";
    String value = "Rocks";
    try {
      try {
        kafkaStore.put(key, value);
      } catch (StoreException e) {
        throw new RuntimeException("Kafka store put(Kafka, Rocks) operation failed", e);
      }
      String retrievedValue = null;
      try {
        retrievedValue = kafkaStore.get(key);
      } catch (StoreException e) {
        throw new RuntimeException("Kafka store get(Kafka) operation failed", e);
      }
      assertEquals("Retrieved value should match entered value", value, retrievedValue);
      // delete the key
      try {
        kafkaStore.delete(key);
      } catch (StoreException e) {
        throw new RuntimeException("Kafka store delete(Kafka) operation failed", e);
      }
      // verify that key is deleted
      try {
        retrievedValue = kafkaStore.get(key);
      } catch (StoreException e) {
        throw new RuntimeException("Kafka store get(Kafka) operation failed", e);
      }
      assertNull("Value should have been deleted", retrievedValue);
      kafkaStore.close();
      // recreate kafka store
      kafkaStore = StoreUtils.createAndInitKafkaStoreInstance(bootstrapServers, inMemoryStore);
      // verify that key still doesn't exist in the store
      retrievedValue = value;
      try {
        retrievedValue = kafkaStore.get(key);
      } catch (StoreException e) {
        throw new RuntimeException("Kafka store get(Kafka) operation failed", e);
      }
      assertNull("Value should have been deleted", retrievedValue);
    } finally {
      kafkaStore.close();
    }
  }



  @Test
  public void testCustomGroupIdConfig() throws Exception {
    Store<String, String> inMemoryStore = new InMemoryCache<>(StringSerializer.INSTANCE);
    String groupId = "test-group-id";
    Properties props = new Properties();
    props.put(SchemaRegistryConfig.KAFKASTORE_GROUP_ID_CONFIG, groupId);
    KafkaStore kafkaStore = StoreUtils.createAndInitKafkaStoreInstance(bootstrapServers, inMemoryStore, props);

    assertEquals(kafkaStore.getKafkaStoreReaderThread().getConsumerProperty(org.apache.kafka.clients.consumer.ConsumerConfig.GROUP_ID_CONFIG), groupId);
  }


  @Test
  public void testDefaultGroupIdConfig() throws Exception {
    Store<String, String> inMemoryStore = new InMemoryCache<>(StringSerializer.INSTANCE);
    Properties props = new Properties();
    KafkaStore kafkaStore = StoreUtils.createAndInitKafkaStoreInstance(bootstrapServers, inMemoryStore, props);

    assertTrue(kafkaStore.getKafkaStoreReaderThread().getConsumerProperty(org.apache.kafka.clients.consumer.ConsumerConfig.GROUP_ID_CONFIG).startsWith("schema-registry-"));
  }

  @Test(expected=StoreInitializationException.class)
  public void testMandatoryCompactionPolicy() throws Exception {
    Properties kafkaProps = new Properties();
    Map<String, String> topicProps = new HashMap<>();
    topicProps.put(TopicConfig.CLEANUP_POLICY_CONFIG, "delete");

    NewTopic topic = new NewTopic(SchemaRegistryConfig.DEFAULT_KAFKASTORE_TOPIC, 1, (short) 1);
    topic.configs(topicProps);

    Properties props = new Properties();
    props.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
    try (AdminClient admin = AdminClient.create(props)) {
      admin.createTopics(Collections.singletonList(topic)).all().get(ADMIN_TIMEOUT_SEC, TimeUnit.SECONDS);
    }

    Store<String, String> inMemoryStore = new InMemoryCache<>(StringSerializer.INSTANCE);

    KafkaStore kafkaStore = StoreUtils.createAndInitKafkaStoreInstance(bootstrapServers, inMemoryStore, kafkaProps);
  }

  @Test(expected=StoreInitializationException.class)
  public void testTooManyPartitions() throws Exception {
    Properties kafkaProps = new Properties();
    Map<String, String> topicProps = new HashMap<>();
    topicProps.put(TopicConfig.CLEANUP_POLICY_CONFIG, "compact");

    NewTopic topic = new NewTopic(SchemaRegistryConfig.DEFAULT_KAFKASTORE_TOPIC, 3, (short) 1);
    topic.configs(topicProps);

    Properties props = new Properties();
    props.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
    try (AdminClient admin = AdminClient.create(props)) {
      admin.createTopics(Collections.singletonList(topic)).all().get(ADMIN_TIMEOUT_SEC, TimeUnit.SECONDS);
    }

    Store<String, String> inMemoryStore = new InMemoryCache<>(StringSerializer.INSTANCE);

    StoreUtils.createAndInitKafkaStoreInstance(bootstrapServers, inMemoryStore, kafkaProps);
  }

  @Test
  public void testTopicAdditionalConfigs() throws Exception {
    Properties kafkaProps = new Properties();
    kafkaProps.put("kafkastore.topic.config.delete.retention.ms", "10000");
    kafkaProps.put("kafkastore.topic.config.segment.ms", "10000");
    Store<String, String> inMemoryStore = new InMemoryCache<>(StringSerializer.INSTANCE);
    StoreUtils.createAndInitKafkaStoreInstance(bootstrapServers, inMemoryStore, kafkaProps);

    Properties props = new Properties();
    props.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);

    ConfigResource configResource = new ConfigResource(
        ConfigResource.Type.TOPIC,
        SchemaRegistryConfig.DEFAULT_KAFKASTORE_TOPIC
    );
    Map<org.apache.kafka.common.config.ConfigResource, Config> topicConfigs;
    try (AdminClient admin = AdminClient.create(props)) {
      topicConfigs = admin.describeConfigs(Collections.singleton(configResource))
          .all().get(ADMIN_TIMEOUT_SEC, TimeUnit.SECONDS);
    }

    Config config = topicConfigs.get(configResource);
    assertNotNull(config.get("delete.retention.ms"));
    assertEquals("10000",config.get("delete.retention.ms").value());
    assertNotNull(config.get("segment.ms"));
    assertEquals("10000",config.get("segment.ms").value());
  }

  @Test
  public void testGetAlwaysTrueHostnameVerifierWhenSslEndpointIdentificationAlgorithmIsNotSet() throws Exception {
    Properties props = new Properties();
    props.put(SchemaRegistryConfig.KAFKASTORE_BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
    props.put(SchemaRegistryConfig.KAFKASTORE_TOPIC_CONFIG, ClusterTestHarness.KAFKASTORE_TOPIC);

    SchemaRegistryConfig config = new SchemaRegistryConfig(props);
    KafkaSchemaRegistry schemaRegistry = new KafkaSchemaRegistry(
            config,
            new SchemaRegistrySerializer()
    );

    assertTrue(schemaRegistry.getHostnameVerifier().verify("", null));
  }

  @Test
  public void testGetAlwaysTrueHostnameVerifierWhenSslEndpointIdentificationAlgorithmIsNone() throws Exception {
    Properties props = new Properties();
    props.put(SchemaRegistryConfig.KAFKASTORE_BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
    props.put(SchemaRegistryConfig.KAFKASTORE_TOPIC_CONFIG, ClusterTestHarness.KAFKASTORE_TOPIC);
    props.put(SchemaRegistryConfig.SSL_ENDPOINT_IDENTIFICATION_ALGORITHM_CONFIG, "none");

    SchemaRegistryConfig config = new SchemaRegistryConfig(props);
    KafkaSchemaRegistry schemaRegistry = new KafkaSchemaRegistry(
            config,
            new SchemaRegistrySerializer()
    );

    assertTrue(schemaRegistry.getHostnameVerifier().verify("", null));
  }

  @Test
  public void testGetAlwaysTrueHostnameVerifierWhenSslEndpointIdentificationAlgorithmIsEmptyString() throws Exception {
    Properties props = new Properties();
    props.put(SchemaRegistryConfig.KAFKASTORE_BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
    props.put(SchemaRegistryConfig.KAFKASTORE_TOPIC_CONFIG, ClusterTestHarness.KAFKASTORE_TOPIC);
    props.put(SchemaRegistryConfig.SSL_ENDPOINT_IDENTIFICATION_ALGORITHM_CONFIG, "");

    SchemaRegistryConfig config = new SchemaRegistryConfig(props);
    KafkaSchemaRegistry schemaRegistry = new KafkaSchemaRegistry(
            config,
            new SchemaRegistrySerializer()
    );

    assertTrue(schemaRegistry.getHostnameVerifier().verify("", null));
  }

  @Test
  public void testGetNullHostnameVerifierWhenSslEndpointIdentificationAlgorithmIsHttps() throws Exception {
    Properties props = new Properties();
    props.put(SchemaRegistryConfig.KAFKASTORE_BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
    props.put(SchemaRegistryConfig.KAFKASTORE_TOPIC_CONFIG, ClusterTestHarness.KAFKASTORE_TOPIC);
    props.put(SchemaRegistryConfig.SSL_ENDPOINT_IDENTIFICATION_ALGORITHM_CONFIG, "https");

    SchemaRegistryConfig config = new SchemaRegistryConfig(props);
    KafkaSchemaRegistry schemaRegistry = new KafkaSchemaRegistry(
            config,
            new SchemaRegistrySerializer()
    );

    assertNull(schemaRegistry.getHostnameVerifier());
  }

  @Test
  public void testKafkaStoreMessageHandlerSameIdDifferentSchema() throws Exception {
    Properties props = new Properties();
    props.put(SchemaRegistryConfig.KAFKASTORE_BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
    props.put(SchemaRegistryConfig.KAFKASTORE_TOPIC_CONFIG, ClusterTestHarness.KAFKASTORE_TOPIC);

    SchemaRegistryConfig config = new SchemaRegistryConfig(props);
    KafkaSchemaRegistry schemaRegistry = new KafkaSchemaRegistry(
        config,
        new SchemaRegistrySerializer()
    );

    KafkaStore<SchemaRegistryKey, SchemaRegistryValue> kafkaStore = schemaRegistry.kafkaStore;
    kafkaStore.init();
    int id = 100;
    kafkaStore.put(new SchemaKey("subject", 1),
        new SchemaValue("subject", 1, id, "schemaString", false)
    );
    kafkaStore.put(new SchemaKey("subject2", 1),
        new SchemaValue("subject2", 1, id, "schemaString2", false)
    );
    int size = 0;
    try (CloseableIterator<SchemaRegistryKey> keys = kafkaStore.getAllKeys()) {
      for (Iterator<SchemaRegistryKey> iter = keys; iter.hasNext(); ) {
        size++;
        iter.next();
      }
    }
    assertEquals(1, size);
  }

  @Test
  public void testKafkaStoreMessageHandlerSameIdSameSchema() throws Exception {
    Properties props = new Properties();
    props.put(SchemaRegistryConfig.KAFKASTORE_BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
    props.put(SchemaRegistryConfig.KAFKASTORE_TOPIC_CONFIG, ClusterTestHarness.KAFKASTORE_TOPIC);

    SchemaRegistryConfig config = new SchemaRegistryConfig(props);
    KafkaSchemaRegistry schemaRegistry = new KafkaSchemaRegistry(
        config,
        new SchemaRegistrySerializer()
    );

    KafkaStore<SchemaRegistryKey, SchemaRegistryValue> kafkaStore = schemaRegistry.kafkaStore;
    kafkaStore.init();
    int id = 100;
    kafkaStore.put(new SchemaKey("subject", 1),
        new SchemaValue("subject", 1, id, "schemaString", false)
    );
    kafkaStore.put(new SchemaKey("subject2", 1),
        new SchemaValue("subject2", 1, id, "schemaString", false)
    );
    int size = 0;
    try (CloseableIterator<SchemaRegistryKey> keys = kafkaStore.getAllKeys()) {
      for (Iterator<SchemaRegistryKey> iter = keys; iter.hasNext(); ) {
        size++;
        iter.next();
      }
    }
    assertEquals(2, size);
  }

  @Test
  public void testKafkaStoreMessageHandlerSameIdDifferentDeletedSchema() throws Exception {
    Properties props = new Properties();
    props.put(SchemaRegistryConfig.KAFKASTORE_BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
    props.put(SchemaRegistryConfig.KAFKASTORE_TOPIC_CONFIG, ClusterTestHarness.KAFKASTORE_TOPIC);

    SchemaRegistryConfig config = new SchemaRegistryConfig(props);
    KafkaSchemaRegistry schemaRegistry = new KafkaSchemaRegistry(
        config,
        new SchemaRegistrySerializer()
    );

    KafkaStore<SchemaRegistryKey, SchemaRegistryValue> kafkaStore = schemaRegistry.kafkaStore;
    kafkaStore.init();
    int id = 100;
    kafkaStore.put(new SchemaKey("subject", 1),
        new SchemaValue("subject", 1, id, "schemaString", false)
    );
    kafkaStore.put(new SchemaKey("subject", 1),
        new SchemaValue("subject", 1, id, "schemaString", true)
    );
    kafkaStore.put(new SchemaKey("subject2", 1),
        new SchemaValue("subject2", 1, id, "schemaString2", false)
    );
    int size = 0;
    try (CloseableIterator<SchemaRegistryKey> keys = kafkaStore.getAllKeys()) {
      for (Iterator<SchemaRegistryKey> iter = keys; iter.hasNext(); ) {
        size++;
        iter.next();
      }
    }
    assertEquals(1, size);
  }

  @Test
  public void testKafkaStoreMessageHandlerSameIdSameDeletedSchema() throws Exception {
    Properties props = new Properties();
    props.put(SchemaRegistryConfig.KAFKASTORE_BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
    props.put(SchemaRegistryConfig.KAFKASTORE_TOPIC_CONFIG, ClusterTestHarness.KAFKASTORE_TOPIC);

    SchemaRegistryConfig config = new SchemaRegistryConfig(props);
    KafkaSchemaRegistry schemaRegistry = new KafkaSchemaRegistry(
        config,
        new SchemaRegistrySerializer()
    );

    KafkaStore<SchemaRegistryKey, SchemaRegistryValue> kafkaStore = schemaRegistry.kafkaStore;
    kafkaStore.init();
    int id = 100;
    kafkaStore.put(new SchemaKey("subject", 1),
        new SchemaValue("subject", 1, id, "schemaString", false)
    );
    kafkaStore.put(new SchemaKey("subject", 1),
        new SchemaValue("subject", 1, id, "schemaString", true)
    );
    kafkaStore.put(new SchemaKey("subject2", 1),
        new SchemaValue("subject2", 1, id, "schemaString", false)
    );
    int size = 0;
    try (CloseableIterator<SchemaRegistryKey> keys = kafkaStore.getAllKeys()) {
      for (Iterator<SchemaRegistryKey> iter = keys; iter.hasNext(); ) {
        size++;
        iter.next();
      }
    }
    assertEquals(2, size);
  }

  // Test no NPE happens when handling DeleteSubjectKey with null value
  @Test
  public void testKafkaStoreMessageHandlerDeleteSubjectKeyNullValue() throws Exception {
    Properties props = new Properties();
    props.put(SchemaRegistryConfig.KAFKASTORE_BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
    props.put(SchemaRegistryConfig.KAFKASTORE_TOPIC_CONFIG, ClusterTestHarness.KAFKASTORE_TOPIC);

    SchemaRegistryConfig config = new SchemaRegistryConfig(props);
    KafkaSchemaRegistry schemaRegistry = new KafkaSchemaRegistry(
            config,
            new SchemaRegistrySerializer()
    );

    InMemoryCache<SchemaRegistryKey, SchemaRegistryValue> store =
            new InMemoryCache<>(new SchemaRegistrySerializer());
    store.init();
    KafkaStoreMessageHandler storeMessageHandler = new KafkaStoreMessageHandler(schemaRegistry,
            store, new IncrementalIdGenerator(schemaRegistry));

    storeMessageHandler.handleUpdate(new DeleteSubjectKey("test"), null, null, null, 0L, 0L);
  }

  // Test no NPE happens when handling ClearSubjectKey with null value
  @Test
  public void testKafkaStoreMessageHandlerClearSubjectKeyNullValue() throws Exception {
    Properties props = new Properties();
    props.put(SchemaRegistryConfig.KAFKASTORE_BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
    props.put(SchemaRegistryConfig.KAFKASTORE_TOPIC_CONFIG, ClusterTestHarness.KAFKASTORE_TOPIC);

    SchemaRegistryConfig config = new SchemaRegistryConfig(props);
    KafkaSchemaRegistry schemaRegistry = new KafkaSchemaRegistry(
            config,
            new SchemaRegistrySerializer()
    );

    InMemoryCache<SchemaRegistryKey, SchemaRegistryValue> store =
            new InMemoryCache<>(new SchemaRegistrySerializer());
    store.init();
    KafkaStoreMessageHandler storeMessageHandler = new KafkaStoreMessageHandler(schemaRegistry,
          store, new IncrementalIdGenerator(schemaRegistry));

    storeMessageHandler.handleUpdate(new ClearSubjectKey("test"), null, null, null, 0L, 0L);
  }
}
