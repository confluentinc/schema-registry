/**
 * Copyright 2015 Confluent Inc.
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
package io.confluent.kafka.schemaregistry.storage;

import io.confluent.kafka.schemaregistry.ClusterTestHarness;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.junit.Assert.assertEquals;

public class KafkaStoreReaderThreadConfigTest extends ClusterTestHarness {

  public KafkaStoreReaderThreadConfigTest() {
    super(1, true);
  }

  private static final Logger log = LoggerFactory.getLogger(KafkaStoreReaderThreadConfigTest.class);

  @Before
  @Override
  public void setUp() throws Exception {
    this.groupId = "test-group-id";
    super.setUp();
    log.debug("Zk conn url = " + zkConnect);
  }

  @After
  public void teardown() {
    log.debug("Shutting down");
  }

  @Test
  public void testGroupIdConfig() throws Exception {

    KafkaSchemaRegistry sr = (KafkaSchemaRegistry) restApp.schemaRegistry();
    KafkaStoreReaderThread readerThread = sr.getKafkaStore().getKafkaStoreReaderThread();
    assertEquals(groupId, readerThread.getGroupId());

  }
}
