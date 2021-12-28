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
  private final KafkaAvroSerializer avroSerializer1;
  private final KafkaAvroSerializer avroSerializer2;
  private final KafkaAvroSerializer avroSerializer3;
  private final KafkaAvroSerializer avroSerializer4;

  public ContextNameStrategyTest() {
    schemaRegistry = new MockSchemaRegistryClient();
    Properties config1 = new Properties();
    config1.put(KafkaAvroDeserializerConfig.SCHEMA_REGISTRY_URL_CONFIG, "bogus");
    config1.put(AbstractKafkaSchemaSerDeConfig.CONTEXT_NAME_STRATEGY,
        CustomContextNameStrategy1.class.getName());
    avroSerializer1 = new KafkaAvroSerializer(schemaRegistry, new HashMap(config1));

    Properties config2 = new Properties();
    config2.put(KafkaAvroDeserializerConfig.SCHEMA_REGISTRY_URL_CONFIG, "bogus");
    config2.put(AbstractKafkaSchemaSerDeConfig.CONTEXT_NAME_STRATEGY,
        CustomContextNameStrategy2.class.getName());
    avroSerializer2 = new KafkaAvroSerializer(schemaRegistry, new HashMap(config2));

    Properties config3 = new Properties();
    config3.put(KafkaAvroDeserializerConfig.SCHEMA_REGISTRY_URL_CONFIG, "bogus");
    config3.put(AbstractKafkaSchemaSerDeConfig.CONTEXT_NAME_STRATEGY,
        CustomContextNameStrategy3.class.getName());
    avroSerializer3 = new KafkaAvroSerializer(schemaRegistry, new HashMap(config3));

    Properties config4 = new Properties();
    config4.put(KafkaAvroDeserializerConfig.SCHEMA_REGISTRY_URL_CONFIG, "bogus");
    config4.put(AbstractKafkaSchemaSerDeConfig.CONTEXT_NAME_STRATEGY,
        CustomContextNameStrategy4.class.getName());
    avroSerializer4 = new KafkaAvroSerializer(schemaRegistry, new HashMap(config4));
  }

  @Test
  public void testCustomContextNameStrategy() {
    assertEquals(":.customContext:", avroSerializer1.getContextName("topic1"));
    assertEquals(":.customContext:subject1", avroSerializer1.getContextName("topic1", "subject1"));

    assertEquals(":.customContext:", avroSerializer2.getContextName("topic1"));
    assertEquals(":.customContext:subject1", avroSerializer2.getContextName("topic1", "subject1"));

    assertEquals(":.customContext:", avroSerializer3.getContextName("topic1"));
    assertEquals(":.customContext:subject1", avroSerializer3.getContextName("topic1", "subject1"));

    assertEquals(":.customContext:", avroSerializer4.getContextName("topic1"));
    assertEquals(":.customContext:subject1", avroSerializer4.getContextName("topic1", "subject1"));
  }

  public static class CustomContextNameStrategy1 implements ContextNameStrategy {
    @Override
    public String contextName(String topic) {
      return "customContext";
    }
  }


  public static class CustomContextNameStrategy2 implements ContextNameStrategy {
    @Override
    public String contextName(String topic) {
      return ".customContext";
    }
  }

  public static class CustomContextNameStrategy3 implements ContextNameStrategy {
    @Override
    public String contextName(String topic) {
      return ":.customContext:";
    }
  }

  public static class CustomContextNameStrategy4 implements ContextNameStrategy {
    @Override
    public String contextName(String topic) {
      return ":customContext:";
    }
  }
}
