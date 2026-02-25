/*
 * Copyright 2026 Confluent Inc.
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

package io.confluent.kafka.serializers.subject;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

import io.confluent.kafka.schemaregistry.avro.AvroSchema;
import io.confluent.kafka.schemaregistry.client.MockSchemaRegistryClient;
import java.util.HashMap;
import java.util.Map;
import org.junit.Before;
import org.junit.Test;

public class AssociatedNameStrategyTest {

  private AssociatedNameStrategy strategy;

  @Before
  public void setUp() {
    strategy = new AssociatedNameStrategy();
    strategy.setSchemaRegistryClient(new MockSchemaRegistryClient());
    Map<String, Object> configs = new HashMap<>();
    configs.put(AssociatedNameStrategy.FALLBACK_SUBJECT_NAME_STRATEGY_TYPE, "RECORD");
    strategy.configure(configs);
  }

  @Test
  public void testNullSchemaReturnsNull() {
    assertNull(strategy.subjectName("my-topic", false, null));
  }

  @Test
  public void testNonNullSchemaReturnsFallbackSubjectName() {
    AvroSchema schema = new AvroSchema(
        "{\"type\":\"record\",\"name\":\"com.example.MyRecord\",\"fields\":[{\"type\":\"string\",\"name\":\"f1\"}]}");
    assertEquals("com.example.MyRecord", strategy.subjectName("my-topic", false, schema));
  }
}
