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

import io.confluent.kafka.schemaregistry.rest.SchemaRegistryConfig;
import kafka.admin.AdminUtils;
import kafka.admin.RackAwareMode;
import kafka.log.LogConfig;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.Config;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.common.config.ConfigResource;
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
import java.util.Set;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.assertNull;

public class KafkaStoreTest extends ClusterTestHarness {

  private static final Logger log = LoggerFactory.getLogger(KafkaStoreTest.class);

  @Before
  public void setup() {
    log.debug("Zk conn url = " + zkConnect);
  }

  @After
  public void teardown() {
    log.debug("Shutting down");
  }

  @Test
  public void testInitialization() throws Exception {
    KafkaStore<String, String> kafkaStore = StoreUtils.createAndInitKafkaStoreInstance(zkConnect);
    kafkaStore.close();
  }

  @Test(expected = StoreInitializationException.class)
  public void testDoubleInitialization() throws Exception {
    KafkaStore<String, String> kafkaStore = StoreUtils.createAndInitKafkaStoreInstance(zkConnect);
    try {
      kafkaStore.init();
    } finally {
      kafkaStore.close();
    }
  }

  @Test
  public void testSimplePut() throws Exception {
    KafkaStore<String, String> kafkaStore = StoreUtils.createAndInitKafkaStoreInstance(zkConnect);
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
//    KafkaStore<String, String> kafkaStore = StoreUtils.createAndInitKafkaStoreInstance(zkConnect,
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
    Store<String, String> inMemoryStore = new InMemoryCache<String, String>();
    KafkaStore<String, String> kafkaStore = StoreUtils.createAndInitKafkaStoreInstance(
        zkConnect,
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
    kafkaStore = StoreUtils.createAndInitKafkaStoreInstance(zkConnect, inMemoryStore);
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
    KafkaStore<String, String> kafkaStore = StoreUtils.createAndInitKafkaStoreInstance(zkConnect);
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
    Store<String, String> inMemoryStore = new InMemoryCache<String, String>();
    KafkaStore<String, String> kafkaStore = StoreUtils.createAndInitKafkaStoreInstance(
        zkConnect,
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
      kafkaStore = StoreUtils.createAndInitKafkaStoreInstance(zkConnect, inMemoryStore);
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
    Store<String, String> inMemoryStore = new InMemoryCache<String, String>();
    String groupId = "test-group-id";
    Properties props = new Properties();
    props.put(SchemaRegistryConfig.KAFKASTORE_GROUP_ID_CONFIG, groupId);
    KafkaStore kafkaStore = StoreUtils.createAndInitKafkaStoreInstance(zkConnect, inMemoryStore, props);

    assertEquals(kafkaStore.getKafkaStoreReaderThread().getConsumerProperty(org.apache.kafka.clients.consumer.ConsumerConfig.GROUP_ID_CONFIG), groupId);
  }


  @Test
  public void testDefaultGroupIdConfig() throws Exception {
    Store<String, String> inMemoryStore = new InMemoryCache<String, String>();
    Properties props = new Properties();
    KafkaStore kafkaStore = StoreUtils.createAndInitKafkaStoreInstance(zkConnect, inMemoryStore, props);

    assertTrue(kafkaStore.getKafkaStoreReaderThread().getConsumerProperty(org.apache.kafka.clients.consumer.ConsumerConfig.GROUP_ID_CONFIG).startsWith("schema-registry-"));
  }

  @Test(expected=StoreInitializationException.class)
  public void testMandatoryCompationPolicy() throws Exception {
    Properties kafkaProps = new Properties();
    Map<String, String> topicProps = new HashMap<>();
    topicProps.put(LogConfig.CleanupPolicyProp(), "delete");

    NewTopic topic = new NewTopic(SchemaRegistryConfig.DEFAULT_KAFKASTORE_TOPIC, 1, (short) 1);
    topic.configs(topicProps);

    Properties props = new Properties();
    props.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
    AdminClient admin = AdminClient.create(props);
    admin.createTopics(Collections.singletonList(topic));

    Store<String, String> inMemoryStore = new InMemoryCache<String, String>();

    KafkaStore kafkaStore = StoreUtils.createAndInitKafkaStoreInstance(zkConnect, inMemoryStore, kafkaProps);
  }

  @Test(expected=StoreInitializationException.class)
  public void testTooManyPartitions() throws Exception {
    Properties kafkaProps = new Properties();
    Map<String, String> topicProps = new HashMap<>();
    topicProps.put(LogConfig.CleanupPolicyProp(), "compact");

    NewTopic topic = new NewTopic(SchemaRegistryConfig.DEFAULT_KAFKASTORE_TOPIC, 3, (short) 1);
    topic.configs(topicProps);

    Properties props = new Properties();
    props.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
    AdminClient admin = AdminClient.create(props);
    admin.createTopics(Collections.singletonList(topic));

    Store<String, String> inMemoryStore = new InMemoryCache<String, String>();

    StoreUtils.createAndInitKafkaStoreInstance(zkConnect, inMemoryStore, kafkaProps);
  }

  @Test
  public void testTopicAdditionalConfigs() throws Exception {
    Properties kafkaProps = new Properties();
    kafkaProps.put("kafkastore.topic.config.delete.retention.ms", "10000");
    kafkaProps.put("kafkastore.topic.config.segment.ms", "10000");
    Store<String, String> inMemoryStore = new InMemoryCache<String, String>();
    StoreUtils.createAndInitKafkaStoreInstance(zkConnect, inMemoryStore, kafkaProps);

    Properties props = new Properties();
    props.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);

    AdminClient admin = AdminClient.create(props);
    ConfigResource configResource = new ConfigResource(
            ConfigResource.Type.TOPIC,
            SchemaRegistryConfig.DEFAULT_KAFKASTORE_TOPIC );
    Map<org.apache.kafka.common.config.ConfigResource, Config> topicConfigs =
        admin.describeConfigs(Collections.singleton(configResource)).all()
            .get(60000, TimeUnit.MILLISECONDS);

    Config config = topicConfigs.get(configResource);
    assertNotNull(config.get("delete.retention.ms"));
    assertEquals("10000",config.get("delete.retention.ms").value());
    assertNotNull(config.get("segment.ms"));
    assertEquals("10000",config.get("segment.ms").value());
  }

  @Test
  public void testKafkaStoreMessageHandlerSameIdDifferentSchema() throws Exception {
    Properties props = new Properties();
    props.put(SchemaRegistryConfig.KAFKASTORE_CONNECTION_URL_CONFIG, zkConnect);
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
    for (Iterator<SchemaRegistryKey> iter = kafkaStore.getAllKeys(); iter.hasNext(); ) {
      size++;
      iter.next();
    }
    assertEquals(1, size);
  }

  @Test
  public void testKafkaStoreMessageHandlerSameIdSameSchema() throws Exception {
    Properties props = new Properties();
    props.put(SchemaRegistryConfig.KAFKASTORE_CONNECTION_URL_CONFIG, zkConnect);
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
    for (Iterator<SchemaRegistryKey> iter = kafkaStore.getAllKeys(); iter.hasNext(); ) {
      size++;
      iter.next();
    }
    assertEquals(2, size);
  }

  @Test
  public void testReplaceDeletedWithNonDeleted() throws Exception {
    InMemoryCache<SchemaKey, SchemaValue> inMemoryStore = new InMemoryCache<>();

    int id = 100;
    SchemaKey schemaKey = new SchemaKey("subject", 1);
    SchemaValue schemaValue = new SchemaValue("subject", 1, id, "schemaString", false);

    SchemaKey schemaKey2 = new SchemaKey("subject2", 1);
    SchemaValue schemaValue2 = new SchemaValue("subject2", 1, id, "schemaString", false);

    inMemoryStore.put(schemaKey, schemaValue);
    inMemoryStore.schemaRegistered(schemaKey, schemaValue);

    inMemoryStore.put(schemaKey2, schemaValue2);
    inMemoryStore.schemaRegistered(schemaKey2, schemaValue2);

    schemaValue2.setDeleted(true);
    inMemoryStore.schemaDeleted(schemaKey2, schemaValue2);

    assertTrue(inMemoryStore.get(inMemoryStore.schemaKeyById(id)).isDeleted());

    inMemoryStore.replaceMatchingDeletedWithNonDeletedOrRemove(s -> s.equals("subject2"));

    SchemaValue newValue = inMemoryStore.get(inMemoryStore.schemaKeyById(id));
    assertEquals("subject", newValue.getSubject());
    assertFalse(newValue.isDeleted());
  }

  @Test
  public void testReplaceDeletedWithNonDeletedAfterCompaction() throws Exception {
    InMemoryCache<SchemaKey, SchemaValue> inMemoryStore = new InMemoryCache<>();

    int id = 100;
    SchemaKey schemaKey = new SchemaKey("subject", 1);
    SchemaValue schemaValue = new SchemaValue("subject", 1, id, "schemaString", true);

    // After a compaction, the schema will not be registered but only deleted
    inMemoryStore.put(schemaKey, schemaValue);
    inMemoryStore.schemaDeleted(schemaKey, schemaValue);

    inMemoryStore.replaceMatchingDeletedWithNonDeletedOrRemove(s -> s.equals("subject"));
  }
}
