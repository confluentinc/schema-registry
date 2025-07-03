/*
 * Copyright 2019 Confluent Inc.
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

package io.confluent.kafka.schemaregistry.client.rest;

import com.google.common.collect.ImmutableMap;
import io.confluent.kafka.schemaregistry.client.rest.entities.ServerClusterId;
import org.junit.Test;

import java.util.Collections;
import java.util.Map;

import static org.junit.Assert.assertEquals;

public class ServerClusterIdTest {

  @Test
  public void buildServerClusterId() {
    ServerClusterId serverClusterId = ServerClusterId.of("kafka1", "sr1");

    final String id = serverClusterId.getId();
    final Map<String, Object> scope = serverClusterId.getScope();

    assertEquals("", id);
    assertEquals(
        ImmutableMap.of(
          "path", Collections.emptyList(),
          "clusters", ImmutableMap.of(
                  "kafka-cluster", "kafka1",
                  "schema-registry-cluster", "sr1")
    ), scope);
  }
}
