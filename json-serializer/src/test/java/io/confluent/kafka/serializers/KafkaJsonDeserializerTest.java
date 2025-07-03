/*
 * Copyright 2015-2025 Confluent Inc.
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

import org.junit.Test;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

public class KafkaJsonDeserializerTest {

  @Test
  public void deserializeNullOrEmpty() {
    KafkaJsonDeserializer<Object> deserializer = new KafkaJsonDeserializer<>();

    Map<String, Object> props = new HashMap<>();
    props.put(KafkaJsonDeserializerConfig.JSON_KEY_TYPE, Object.class.getName());
    deserializer.configure(props, true);

    assertNull(deserializer.deserialize("topic", null));
    assertNull(deserializer.deserialize("topic", new byte[0]));
  }

  @Test
  public void deserializePojoKey() {
    KafkaJsonDeserializer<Foo> deserializer = new KafkaJsonDeserializer<Foo>();

    Map<String, Object> props = new HashMap<String, Object>();
    props.put(KafkaJsonDeserializerConfig.JSON_KEY_TYPE, Foo.class.getName());
    deserializer.configure(props, true);

    Foo foo = deserializer.deserialize(null, "{\"bar\":\"baz\"}".getBytes());
    assertNotNull(foo);
    assertEquals("baz", foo.getBar());
  }

  @Test
  public void deserializePojoValue() {
    KafkaJsonDeserializer<Foo> deserializer = new KafkaJsonDeserializer<Foo>();

    HashMap<String, Object> props = new HashMap<String, Object>();
    props.put(KafkaJsonDeserializerConfig.JSON_VALUE_TYPE, Foo.class.getName());
    deserializer.configure(props, false);

    Foo foo = deserializer.deserialize(null, "{\"bar\":\"baz\"}".getBytes());
    assertNotNull(foo);
    assertEquals("baz", foo.getBar());
  }

  @Test
  public void deserializeObject() {
    KafkaJsonDeserializer<Object> deserializer = new KafkaJsonDeserializer<Object>();

    HashMap<String, Object> props = new HashMap<String, Object>();
    props.put(KafkaJsonDeserializerConfig.JSON_VALUE_TYPE, Object.class.getName());
    deserializer.configure(props, false);

    assertEquals(45.3, deserializer.deserialize(null, "45.3".getBytes()));
    assertEquals(799, deserializer.deserialize(null, "799".getBytes()));
    assertEquals("hello", deserializer.deserialize(null, "\"hello\"".getBytes()));
    assertEquals(null, deserializer.deserialize(null, "null".getBytes()));
    assertEquals(Arrays.asList("foo", "bar"), deserializer.deserialize(null, "[\"foo\",\"bar\"]".getBytes()));

    Map<String, String> map = new HashMap<String, String>();
    map.put("foo", "bar");
    assertEquals(map, deserializer.deserialize(null, "{\"foo\":\"bar\"}".getBytes()));
  }

  public static class Foo {
    private String bar;

    public String getBar() {
      return bar;
    }

    public void setBar(String bar) {
      this.bar = bar;
    }
  }

}
