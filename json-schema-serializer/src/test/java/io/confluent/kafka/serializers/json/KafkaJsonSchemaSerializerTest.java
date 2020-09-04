/*
 * Copyright 2020 Confluent Inc.
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

package io.confluent.kafka.serializers.json;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.kjetland.jackson.jsonSchema.annotations.JsonSchemaInject;
import com.kjetland.jackson.jsonSchema.annotations.JsonSchemaString;
import org.apache.kafka.common.errors.SerializationException;
import org.junit.Test;

import javax.validation.constraints.Min;
import java.math.BigDecimal;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Properties;

import io.confluent.kafka.schemaregistry.client.MockSchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

public class KafkaJsonSchemaSerializerTest {

  private final Properties config;
  private final SchemaRegistryClient schemaRegistry;
  private KafkaJsonSchemaSerializer<Object> serializer;
  private KafkaJsonSchemaDeserializer<Object> deserializer;
  private final String topic;

  public KafkaJsonSchemaSerializerTest() {
    config = new Properties();
    config.put(KafkaJsonSchemaSerializerConfig.AUTO_REGISTER_SCHEMAS, true);
    config.put(KafkaJsonSchemaSerializerConfig.SCHEMA_REGISTRY_URL_CONFIG, "bogus");
    config.put(KafkaJsonSchemaSerializerConfig.FAIL_INVALID_SCHEMA, true);
    schemaRegistry = new MockSchemaRegistryClient();
    serializer = new KafkaJsonSchemaSerializer<>(schemaRegistry, new HashMap(config));
    deserializer = getDeserializer(Object.class);
    topic = "test";
  }

  private <T> KafkaJsonSchemaDeserializer<T> getDeserializer(Class<T> cls) {
    return new KafkaJsonSchemaDeserializer<>(schemaRegistry, new HashMap(config), cls);
  }

  @Test
  public void testKafkaJsonSchemaSerializer() {
    byte[] bytes;

    bytes = serializer.serialize(topic, null);
    assertEquals(null, deserializer.deserialize(topic, bytes));

    bytes = serializer.serialize(topic, true);
    assertEquals(true, deserializer.deserialize(topic, bytes));

    bytes = serializer.serialize(topic, 123);
    assertEquals(123, deserializer.deserialize(topic, bytes));

    bytes = serializer.serialize(topic, 345L);
    // JSON can't distinguish longs
    assertEquals(345, deserializer.deserialize(topic, bytes));

    bytes = serializer.serialize(topic, 1.23f);
    // JSON can't distinguish doubles
    assertEquals(new BigDecimal("1.23"), deserializer.deserialize(topic, bytes));

    bytes = serializer.serialize(topic, 2.34d);
    assertEquals(new BigDecimal("2.34"), deserializer.deserialize(topic, bytes));

    bytes = serializer.serialize(topic, "abc");
    assertEquals("abc", deserializer.deserialize(topic, bytes));
  }

  @Test
  public void serializeNull() {
    assertNull(serializer.serialize("foo", null));
  }

  @Test
  public void serializeMap() throws Exception {
    Map<String, Object> message = new HashMap<>();
    message.put("foo", "bar");
    message.put("baz", new BigDecimal("354.99"));

    byte[] bytes = serializer.serialize("foo", message);
    Object deserialized = deserializer.deserialize(topic, bytes);
    assertEquals(message, deserialized);
  }

  @Test
  public void serializeUser() throws Exception {
    User user = new User("john", "doe", (short) 50);

    byte[] bytes = serializer.serialize("foo", user);
    Object deserialized = getDeserializer(User.class).deserialize(topic, bytes);
    assertEquals(user, deserialized);

    // Test for javaType property
    deserialized = getDeserializer(null).deserialize(topic, bytes);
    assertEquals(user, deserialized);

    // Test javaType overrides the default Object.class
    deserialized = getDeserializer(Object.class).deserialize(topic, bytes);
    assertEquals(user, deserialized);
  }

  @Test(expected = SerializationException.class)
  public void serializeInvalidUser() throws Exception {
    User user = new User("john", "doe", (short) -1, "jack");

    byte[] bytes = serializer.serialize("foo", user);
    Object deserialized = getDeserializer(User.class).deserialize(topic, bytes);
    assertEquals(user, deserialized);
  }

  // Generate javaType property
  @JsonSchemaInject(strings = {@JsonSchemaString(path="javaType",
      value="io.confluent.kafka.serializers.json.KafkaJsonSchemaSerializerTest$User")})
  public static class User {
    @JsonProperty
    public String firstName;
    @JsonProperty
    public String lastName;
    @JsonProperty
    @Min(0)
    public short age;
    @JsonProperty
    public Optional<String> nickName;

    public User() {}

    public User(String firstName, String lastName, short age) {
      this(firstName, lastName, age, null);
    }

    public User(String firstName, String lastName, short age, String nickName) {
      this.firstName = firstName;
      this.lastName = lastName;
      this.age = age;
      this.nickName = Optional.ofNullable(nickName);
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }
      User user = (User) o;
      return age == user.age
          && Objects.equals(firstName, user.firstName)
          && Objects.equals(lastName, user.lastName)
          && Objects.equals(nickName, user.nickName);
    }

    @Override
    public int hashCode() {
      return Objects.hash(firstName, lastName, age, nickName);
    }
  }
}
