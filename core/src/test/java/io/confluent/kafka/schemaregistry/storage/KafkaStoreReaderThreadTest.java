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

import io.confluent.kafka.schemaregistry.ClusterTestHarness;
import io.confluent.kafka.schemaregistry.rest.SchemaRegistryConfig;
import io.confluent.kafka.schemaregistry.storage.exceptions.StoreTimeoutException;
import io.confluent.kafka.schemaregistry.utils.TestUtils;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.MockConsumer;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.AuthenticationException;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

public class KafkaStoreReaderThreadTest extends ClusterTestHarness {

  public KafkaStoreReaderThreadTest() {
    super(1, true);
  }

  private static final Logger log = LoggerFactory.getLogger(KafkaStoreReaderThreadTest.class);

  @Before
  public void setup() {
  }

  @After
  public void teardown() {
    log.debug("Shutting down");
  }


  @Test
  public void testWaitUntilOffset() throws Exception {
    String schema = TestUtils.getRandomCanonicalAvroString(1).get(0);
    int id1 = restApp.restClient.registerSchema(schema, "subject1");

    KafkaSchemaRegistry sr = (KafkaSchemaRegistry) restApp.schemaRegistry();
    KafkaStoreReaderThread readerThread = sr.getKafkaStore().getKafkaStoreReaderThread();
    try {
      readerThread.waitUntilOffset(50L, 500L, TimeUnit.MILLISECONDS);
      fail("Should have timed out waiting to reach non-existent offset.");
    } catch (StoreTimeoutException e) {
      // This is expected
    }
    
    try {
      readerThread.waitUntilOffset(0L, 5000L, TimeUnit.MILLISECONDS);
    } catch (StoreTimeoutException e) {
      fail("5 seconds should be more than enough time to reach offset 0 in the log.");
    }
  }

  @Test
  public void testPollFailureIsRetried() throws Exception {
    Properties props = new Properties();
    props.put(SchemaRegistryConfig.KAFKASTORE_BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
    props.put(SchemaRegistryConfig.KAFKASTORE_TOPIC_CONFIG, KAFKASTORE_TOPIC);
    SchemaRegistryConfig config = new SchemaRegistryConfig(props);
    Store<String, String> localStore = new InMemoryCache<>(StringSerializer.INSTANCE);
    KafkaStoreReaderThread<String, String> readerThread = new KafkaStoreReaderThread<>(
        bootstrapServers, KAFKASTORE_TOPIC, "test-group", new StringMessageHandler(),
        StringSerializer.INSTANCE, localStore, null, "noop", new AtomicBoolean(true), config);

    TopicPartition tp = new TopicPartition(KAFKASTORE_TOPIC, 0);
    MockConsumer<byte[], byte[]> consumer = new MockConsumer<>(OffsetResetStrategy.EARLIEST);
    consumer.assign(Collections.singletonList(tp));
    consumer.updateBeginningOffsets(Collections.singletonMap(tp, 0L));
    readerThread.setConsumer(consumer);
    try {
      // Used to rethrow and end the reader thread for good
      consumer.setPollException(new AuthenticationException("auth outage"));
      readerThread.doWork();

      consumer.addRecord(new ConsumerRecord<>(KAFKASTORE_TOPIC, 0, 0L,
          StringSerializer.INSTANCE.serializeKey("key"),
          StringSerializer.INSTANCE.serializeValue("value")));
      readerThread.doWork();
      assertEquals("value", localStore.get("key"));
    } finally {
      readerThread.shutdown();
    }
  }
}
