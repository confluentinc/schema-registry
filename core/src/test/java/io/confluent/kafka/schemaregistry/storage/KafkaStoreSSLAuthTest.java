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

import io.confluent.kafka.schemaregistry.SSLClusterTestHarness;
import io.confluent.kafka.schemaregistry.storage.exceptions.StoreInitializationException;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.junit.Assert.assertEquals;

public class KafkaStoreSSLAuthTest extends SSLClusterTestHarness {
  private static final Logger log = LoggerFactory.getLogger(KafkaStoreSSLAuthTest.class);

  @Before
  public void setup() {
  }

  @After
  public void teardown() {
    log.debug("Shutting down");
  }

  @Test
  public void testInitialization() throws Exception {
    KafkaStore<String, String> kafkaStore = StoreUtils.createAndInitSSLKafkaStoreInstance(bootstrapServers,
            clientSslConfigs, requireSSLClientAuth());
    kafkaStore.close();
  }

  @Test(expected = StoreInitializationException.class)
  public void testInitializationWithoutClientAuth() throws Exception {
    KafkaStore<String, String> kafkaStore = StoreUtils.createAndInitSSLKafkaStoreInstance(bootstrapServers,
            clientSslConfigs, false);
    kafkaStore.close();

    // TODO: make the timeout shorter so the test fails quicker.
  }

  @Test(expected = StoreInitializationException.class)
  public void testDoubleInitialization() throws Exception {
    KafkaStore<String, String> kafkaStore = StoreUtils.createAndInitSSLKafkaStoreInstance(bootstrapServers,
            clientSslConfigs, requireSSLClientAuth());
    try {
      kafkaStore.init();
    } finally {
      kafkaStore.close();
    }
  }

  @Test
  public void testSimplePut() throws Exception {
    KafkaStore<String, String> kafkaStore = StoreUtils.createAndInitSSLKafkaStoreInstance(bootstrapServers,
            clientSslConfigs, requireSSLClientAuth());
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
}
