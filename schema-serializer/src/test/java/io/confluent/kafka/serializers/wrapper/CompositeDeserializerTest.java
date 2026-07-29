/*
 * Copyright 2025 Confluent Inc.
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

package io.confluent.kafka.serializers.wrapper;

import static org.junit.Assert.assertEquals;

import io.confluent.kafka.serializers.AbstractKafkaSchemaSerDe;
import java.util.HashMap;
import java.util.Map;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.serialization.Deserializer;
import org.junit.Test;

public class CompositeDeserializerTest {

  @Test
  public void testCompositeDeserializerWithCustomConfig() throws Exception {
    Map<String, Object> configs = new HashMap<>();
    configs.put("composite.confluent.deserializer", TestSerializer.class.getName());
    configs.put("composite.old.deserializer", TestSerializer.class.getName());
    configs.put("composite.old.deserializer.use.schema.id", "10111");

    CompositeDeserializer compositeDeserializer = new CompositeDeserializer();
    compositeDeserializer.configure(configs, false);

    assertEquals("10111",
        ((TestSerializer) compositeDeserializer.getOldDeserializer()).configs.get("use.schema.id"));
    assertEquals(TestSerializer.class, compositeDeserializer.getConfluentDeserializer().getClass());
    assertEquals(TestSerializer.class, compositeDeserializer.getOldDeserializer().getClass());
  }

  public static class TestSerializer
      extends AbstractKafkaSchemaSerDe implements Deserializer<Object> {
    Map<String, Object> configs;

    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {
      this.configs = new HashMap<>(configs);
    }

    @Override
    public Object deserialize(String topic, byte[] data) {
      return null;
    }

    @Override
    public Object deserialize(String topic, Headers headers, byte[] data) {
      return null;
    }

    @Override
    public void close() {
    }
  }
}
