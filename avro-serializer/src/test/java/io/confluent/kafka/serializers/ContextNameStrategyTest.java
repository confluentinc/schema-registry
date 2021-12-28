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

import io.confluent.kafka.schemaregistry.client.MockSchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.serializers.context.strategy.ContextNameStrategy;
import java.util.HashMap;
import java.util.Properties;
import org.junit.Test;

public class ContextNameStrategyTest {

  private final SchemaRegistryClient schemaRegistry;
  private final KafkaAvroSerializer avroSerializer;

  public ContextNameStrategyTest() {
    Properties defaultConfig = new Properties();
    defaultConfig.put(KafkaAvroDeserializerConfig.SCHEMA_REGISTRY_URL_CONFIG, "bogus");
    defaultConfig.put(AbstractKafkaSchemaSerDeConfig.CONTEXT_NAME_STRATEGY,
        CustomContextNameStrategy.class.getName());
    schemaRegistry = new MockSchemaRegistryClient();
    avroSerializer = new KafkaAvroSerializer(schemaRegistry, new HashMap(defaultConfig));
  }

  @Test
  public void testCustomContextNameStrategy() {
    assertEquals(":.customContext:", avroSerializer.getContextName("topic1"));
    assertEquals(":.customContext:subject1", avroSerializer.getContextName("topic1", "subject1"));
  }

  public static class CustomContextNameStrategy implements ContextNameStrategy {
    @Override
    public String contextName(String topic) {
      return "customContext";
    }
  }

}
