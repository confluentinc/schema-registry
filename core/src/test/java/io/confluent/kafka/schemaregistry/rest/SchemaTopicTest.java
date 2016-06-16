/**
 * Copyright 2014 Confluent Inc.
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
package io.confluent.kafka.schemaregistry.rest;

import kafka.server.ConfigType;
import org.junit.Test;

import java.util.HashSet;
import java.util.Properties;
import java.util.Set;

import io.confluent.kafka.schemaregistry.ClusterTestHarness;
import kafka.admin.AdminUtils;
import kafka.log.LogConfig;
import scala.collection.JavaConversions;
import scala.collection.Map;
import scala.collection.Seq;

import static org.junit.Assert.assertEquals;

public class SchemaTopicTest extends ClusterTestHarness {

  public SchemaTopicTest() {
    super(3, true);
  }

  @Test
  public void testSchemaTopicProperty() throws Exception {
    Set<String> topics = new HashSet<String>();
    topics.add(KAFKASTORE_TOPIC);

    // check # partition and the replication factor
    Map partitionAssignment = zkUtils.getPartitionAssignmentForTopics(
        JavaConversions.asScalaSet(topics).toSeq())
        .get(KAFKASTORE_TOPIC).get();
    assertEquals("There should be only 1 partition in the schema topic",
                 1,
                 partitionAssignment.size());
    Seq replicas = (Seq) partitionAssignment.get(0).get();
    assertEquals("There should be 3 replicas in partition 0 in the schema topic",
                 3,
                 replicas.size());

    // check the retention policy
    Properties prop = AdminUtils.fetchEntityConfig(zkUtils, ConfigType.Topic(), KAFKASTORE_TOPIC);
    assertEquals("The schema topic should have the compact retention policy",
                 "compact",
                 prop.getProperty(LogConfig.CleanupPolicyProp()));
  }
}