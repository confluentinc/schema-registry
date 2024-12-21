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
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.BooleanNode;
import com.fasterxml.jackson.databind.node.DoubleNode;
import com.fasterxml.jackson.databind.node.FloatNode;
import com.fasterxml.jackson.databind.node.IntNode;
import com.fasterxml.jackson.databind.node.LongNode;
import com.fasterxml.jackson.databind.node.NullNode;
import com.fasterxml.jackson.databind.node.TextNode;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.kjetland.jackson.jsonSchema.annotations.JsonSchemaInject;
import com.kjetland.jackson.jsonSchema.annotations.JsonSchemaString;
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;
import io.confluent.kafka.schemaregistry.json.JsonSchema;
import io.confluent.kafka.schemaregistry.json.JsonSchemaProvider;
import io.confluent.kafka.schemaregistry.json.JsonSchemaUtils;
import io.confluent.kafka.serializers.subject.RecordNameStrategy;
import java.io.IOException;
import java.time.LocalDate;
import java.util.Collections;
import java.util.List;
import org.apache.kafka.common.errors.InvalidConfigurationException;
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

  private static final String recordWithDefaultsSchemaString = "{\"properties\": {\n"
      + "     \"null\": {\"type\": \"null\", \"default\": null},\n"
      + "     \"boolean\": {\"type\": \"boolean\", \"default\": true},\n"
      + "     \"number\": {\"type\": \"number\", \"default\": 123},\n"
      + "     \"string\": {\"type\": \"string\", \"default\": \"abc\"}\n"
      + "  },\n"
      + "  \"additionalProperties\": false\n"
      + "}";

  private static final JsonSchema recordWithDefaultsSchema =
      new JsonSchema(recordWithDefaultsSchemaString);

  private final Properties config;
  private final SchemaRegistryClient schemaRegistry;
  private KafkaJsonSchemaSerializer<Object> serializer;
  private KafkaJsonSchemaSerializer<Object> latestSerializer;
  private KafkaJsonSchemaDeserializer<Object> deserializer;
  private final String topic;

  public KafkaJsonSchemaSerializerTest() {
    config = new Properties();
    config.put(KafkaJsonSchemaSerializerConfig.AUTO_REGISTER_SCHEMAS, true);
    config.put(KafkaJsonSchemaSerializerConfig.SCHEMA_REGISTRY_URL_CONFIG, "bogus");
    config.put(KafkaJsonSchemaSerializerConfig.FAIL_INVALID_SCHEMA, true);
    config.put(KafkaJsonSchemaSerializerConfig.WRITE_DATES_AS_ISO8601, true);
    schemaRegistry = new MockSchemaRegistryClient(
        Collections.singletonList(new JsonSchemaProvider()));
    serializer = new KafkaJsonSchemaSerializer<>(schemaRegistry, new HashMap(config));
    deserializer = getDeserializer(Object.class);
    Properties latestConfig = new Properties(config);
    latestConfig.put(KafkaJsonSchemaSerializerConfig.AUTO_REGISTER_SCHEMAS, false);
    latestConfig.put(KafkaJsonSchemaSerializerConfig.SCHEMA_REGISTRY_URL_CONFIG, "bogus");
    latestConfig.put(KafkaJsonSchemaSerializerConfig.USE_LATEST_VERSION, true);
    latestConfig.put(KafkaJsonSchemaSerializerConfig.LATEST_COMPATIBILITY_STRICT, false);
    latestConfig.put(KafkaJsonSchemaSerializerConfig.DEFAULT_PROPERTY_INCLUSION, "NON_NULL");
    latestSerializer = new KafkaJsonSchemaSerializer<>(schemaRegistry, new HashMap(latestConfig));
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

    bytes = serializer.serialize(topic, NullNode.getInstance());
    assertEquals(null, deserializer.deserialize(topic, bytes));

    bytes = serializer.serialize(topic, true);
    assertEquals(true, deserializer.deserialize(topic, bytes));

    bytes = serializer.serialize(topic, BooleanNode.getTrue());
    assertEquals(true, deserializer.deserialize(topic, bytes));

    bytes = serializer.serialize(topic, 123);
    assertEquals(123, deserializer.deserialize(topic, bytes));

    bytes = serializer.serialize(topic, IntNode.valueOf(123));
    assertEquals(123, deserializer.deserialize(topic, bytes));

    bytes = serializer.serialize(topic, 345L);
    // JSON can't distinguish longs
    assertEquals(345, deserializer.deserialize(topic, bytes));

    bytes = serializer.serialize(topic, LongNode.valueOf(345L));
    // JSON can't distinguish longs
    assertEquals(345, deserializer.deserialize(topic, bytes));

    bytes = serializer.serialize(topic, 1.23f);
    // JSON can't distinguish doubles
    assertEquals(new BigDecimal("1.23"), deserializer.deserialize(topic, bytes));

    bytes = serializer.serialize(topic, FloatNode.valueOf(1.23f));
    // JSON can't distinguish doubles
    assertEquals(new BigDecimal("1.23"), deserializer.deserialize(topic, bytes));

    bytes = serializer.serialize(topic, 2.34d);
    assertEquals(new BigDecimal("2.34"), deserializer.deserialize(topic, bytes));

    bytes = serializer.serialize(topic, DoubleNode.valueOf(2.34d));
    assertEquals(new BigDecimal("2.34"), deserializer.deserialize(topic, bytes));

    bytes = serializer.serialize(topic, "abc");
    assertEquals("abc", deserializer.deserialize(topic, bytes));

    bytes = serializer.serialize(topic, TextNode.valueOf("abc"));
    assertEquals("abc", deserializer.deserialize(topic, bytes));
  }

  @Test(expected = InvalidConfigurationException.class)
  public void testKafkaJsonSchemaSerializerWithoutConfigure() {
    KafkaJsonSchemaSerializer unconfiguredSerializer = new KafkaJsonSchemaSerializer();
    User user = new User();
    unconfiguredSerializer.serialize("foo", user);
  }

  @Test(expected = InvalidConfigurationException.class)
  public void testKafkaJsonSchemaDeserializerWithoutConfigure() {
    KafkaJsonSchemaDeserializer unconfiguredSerializer = new KafkaJsonSchemaDeserializer();
    byte[] randomBytes = "foo".getBytes();
    unconfiguredSerializer.deserialize("foo", randomBytes);
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
    User user = new User("john", "doe", (short) 50, "jack", LocalDate.parse("2018-12-27"));

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
    User user = new User("john", "doe", (short) -1, "jack", LocalDate.parse("2018-12-27"));

    byte[] bytes = serializer.serialize("foo", user);
    Object deserialized = getDeserializer(User.class).deserialize(topic, bytes);
    assertEquals(user, deserialized);
  }

  @Test
  public void serializeUserIgnoreNulls() throws Exception {
    User user = new User("john", "doe", (short) 50, "jack", null);
    JsonSchema userSchema = JsonSchemaUtils.getSchema(user, null, false, null);
    schemaRegistry.register(topic + "-value", userSchema);

    byte[] bytes = latestSerializer.serialize(topic, user);
    Object deserialized = getDeserializer(User.class).deserialize(topic, bytes);
    assertEquals(user, deserialized);
  }

  @Test
  public void serializeUserRef() throws Exception {
    String schema = "{\n"
        + "  \"$schema\": \"http://json-schema.org/draft-07/schema#\",\n"
        + "  \"title\": \"Schema references\",\n"
        + "  \"description\": \"List of schema references for multiple types in a single topic\",\n"
        + "  \"oneOf\": [\n"
        + "    { \"$ref\": \"customer.json\"},\n"
        + "    { \"$ref\": \"user.json\"}\n"
        + "  ]\n"
        + "}";

    Customer customer = new Customer("acme", null);
    User user = new User("john", "doe", (short) 50, "jack", null);
    JsonSchema userSchema = JsonSchemaUtils.getSchema(user);
    JsonSchema customerSchema = JsonSchemaUtils.getSchema(customer);
    schemaRegistry.register("user", userSchema);
    schemaRegistry.register("customer", customerSchema);
    List<io.confluent.kafka.schemaregistry.client.rest.entities.SchemaReference> refs =
        ImmutableList.of(
            new io.confluent.kafka.schemaregistry.client.rest.entities.SchemaReference(
                "user.json", "user", 1),
            new io.confluent.kafka.schemaregistry.client.rest.entities.SchemaReference(
                "customer.json", "customer", 1));
    Map<String, String> resolvedRefs = ImmutableMap.of(
        "user.json", userSchema.canonicalString(),
        "customer.json", customerSchema.canonicalString());
    JsonSchema jsonSchema = new JsonSchema(schema, refs, resolvedRefs, null);
    schemaRegistry.register(topic + "-value", jsonSchema);

    byte[] bytes = latestSerializer.serialize(topic, user);

    // Test for javaType property
    Object deserialized = getDeserializer(null).deserialize(topic, bytes);
    assertEquals(user, deserialized);

    bytes = latestSerializer.serialize(topic, customer);

    // Test for javaType property
    deserialized = getDeserializer(null).deserialize(topic, bytes);
    assertEquals(customer, deserialized);
  }

  @Test
  public void serializeRecordWithDefaults() throws Exception {
    schemaRegistry.register(topic + "-value", recordWithDefaultsSchema);

    String json = "{}";
    JsonNode record = new ObjectMapper().readTree(json);
    byte[] bytes = latestSerializer.serialize(topic, record);

    String expectedJson = "{\n"
        + "    \"null\": null,\n"
        + "    \"boolean\": true,\n"
        + "    \"number\": 123,\n"
        + "    \"string\": \"abc\"\n"
        + "}";
    JsonNode expectedRecord = new ObjectMapper().readTree(expectedJson);
    Object deserialized = getDeserializer(null).deserialize(topic, bytes);
    assertEquals(expectedRecord, deserialized);
  }
  
  @Test
  public void testKafkaJsonSchemaDeserializerWithPreRegisteredUseLatestRecordNameStrategy()
      throws IOException, RestClientException {
    Map configs = ImmutableMap.of(
        KafkaJsonSchemaDeserializerConfig.SCHEMA_REGISTRY_URL_CONFIG,
        "bogus",
        KafkaJsonSchemaSerializerConfig.AUTO_REGISTER_SCHEMAS,
        false,
        KafkaJsonSchemaSerializerConfig.USE_LATEST_VERSION,
        true,
        KafkaJsonSchemaSerializerConfig.VALUE_SUBJECT_NAME_STRATEGY,
        RecordNameStrategy.class.getName()
    );
    serializer.configure(configs, false);
    deserializer.configure(configs, false);
    User user = new User("john", "doe", (short) 50, "jack", null);
    JsonSchema schema = JsonSchemaUtils.getSchema(user);
    schemaRegistry.register("com.acme.User", schema);
    byte[] bytes = serializer.serialize(topic, user);
    assertEquals(user, deserializer.deserialize(topic, bytes));

    // restore configs
    serializer.configure(new HashMap(config), false);
    serializer.configure(new HashMap(config), false);
  }

  // Generate javaType property
  @JsonSchemaInject(strings = {@JsonSchemaString(path="javaType",
      value="io.confluent.kafka.serializers.json.KafkaJsonSchemaSerializerTest$Customer")})
  public static class Customer {
    @JsonProperty
    public String customerName;
    @JsonProperty
    public LocalDate acquireDate;

    public Customer() {}

    public Customer(String customerName, LocalDate acquireDate) {
      this.customerName = customerName;
      this.acquireDate = acquireDate;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }
      Customer customer = (Customer) o;
      return Objects.equals(customerName, customer.customerName)
          && Objects.equals(acquireDate, customer.acquireDate);
    }

    @Override
    public int hashCode() {
      return Objects.hash(customerName, acquireDate);
    }
  }

  // Generate javaType property
  @JsonSchemaInject(strings = {@JsonSchemaString(path="javaType",
      value="io.confluent.kafka.serializers.json.KafkaJsonSchemaSerializerTest$User"),
      @JsonSchemaString(path="title", value="com.acme.User")})
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
    @JsonProperty
    public LocalDate birthdate;

    public User() {}

    public User(String firstName, String lastName, short age, String nickName, LocalDate birthdate) {
      this.firstName = firstName;
      this.lastName = lastName;
      this.age = age;
      this.nickName = Optional.ofNullable(nickName);
      this.birthdate = birthdate;
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
          && Objects.equals(nickName, user.nickName)
          && Objects.equals(birthdate, user.birthdate);
    }

    @Override
    public int hashCode() {
      return Objects.hash(firstName, lastName, age, nickName, birthdate);
    }
  }
}
