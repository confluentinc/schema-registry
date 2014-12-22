package io.confluent.kafka.schemaregistry.zookeeper;

import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;
import java.util.concurrent.Callable;

import io.confluent.kafka.schemaregistry.ClusterTestHarness;
import io.confluent.kafka.schemaregistry.storage.KafkaSchemaRegistry;
import io.confluent.kafka.schemaregistry.storage.KafkaStoreConfig;
import io.confluent.kafka.schemaregistry.storage.SchemaRegistryConfig;
import io.confluent.kafka.schemaregistry.utils.TestUtils;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class MasterElectorTest extends ClusterTestHarness {

  private static final Logger log = LoggerFactory.getLogger(MasterElectorTest.class);

  @Test
  public void testAutoFailover() {

    // create schema registry instance 1
    Properties props1 = new Properties();
    props1.put(KafkaStoreConfig.KAFKASTORE_CONNECTION_URL_CONFIG, zkConnect);
    props1.put(KafkaStoreConfig.KAFKASTORE_TOPIC_CONFIG, KAFKASTORE_TOPIC);
    props1.put(SchemaRegistryConfig.PORT_CONFIG, "1234");

    final KafkaSchemaRegistry schemaRegistry1 = TestUtils.createAndInitSchemaRegistryInstance(
        props1);
    assertTrue("Schema registry instance 1 should be the master", schemaRegistry1.isMaster());

    // create schema registry instance 2
    Properties props2 = new Properties();
    props2.put(KafkaStoreConfig.KAFKASTORE_CONNECTION_URL_CONFIG, zkConnect);
    props2.put(KafkaStoreConfig.KAFKASTORE_TOPIC_CONFIG, KAFKASTORE_TOPIC);
    props2.put(SchemaRegistryConfig.PORT_CONFIG, "4321");

    final KafkaSchemaRegistry schemaRegistry2 = TestUtils.createAndInitSchemaRegistryInstance(
        props2);
    assertFalse("Schema registry instance 2 shouldn't be the master", schemaRegistry2.isMaster());
    assertEquals("Instance 2's master should be instance 1",
                 schemaRegistry2.masterIdentity(), schemaRegistry1.myIdentity());

    // stop schema registry instance 1
    schemaRegistry1.close();

    Callable<Boolean> condition = new Callable<Boolean>() {
      @Override
      public Boolean call() throws Exception {
        return schemaRegistry2.isMaster();
      }
    };

    TestUtils.waitUntilTrue(condition, 5000, "Schema registry instance 2 should become the master");

    schemaRegistry2.close();
  }
}