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

package io.confluent.connect.json;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.collect.ImmutableMap;
import java.math.BigDecimal;
import java.math.BigInteger;
import org.apache.kafka.common.errors.NetworkException;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.connect.data.Date;
import org.apache.kafka.connect.data.Decimal;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.data.Time;
import org.apache.kafka.connect.data.Timestamp;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.fail;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import io.confluent.kafka.schemaregistry.client.MockSchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;
import io.confluent.kafka.schemaregistry.json.JsonSchema;
import io.confluent.kafka.schemaregistry.json.JsonSchemaUtils;
import io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig;
import io.confluent.kafka.serializers.json.KafkaJsonSchemaSerializer;

import static org.junit.Assert.assertEquals;

public class JsonSchemaConverterTest {

  private final ResourceLoader loader = ResourceLoader.DEFAULT;

  private static final String TOPIC = "topic";

  private static final Map<String, ?> SR_CONFIG =
      Collections.singletonMap(AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG,
      "localhost"
  );

  private final SchemaRegistryClient schemaRegistry;
  private final JsonSchemaConverter converter;

  public JsonSchemaConverterTest() {
    schemaRegistry = new MockSchemaRegistryClient();
    converter = new JsonSchemaConverter(schemaRegistry);
  }

  @Before
  public void setUp() {
    Map<String, String> config = new HashMap<>();
    config.put("schema.registry.url", "http://fake-url");
    converter.configure(config, false);
  }

  @Test
  public void testPrimitive() {
    SchemaAndValue original = new SchemaAndValue(Schema.BOOLEAN_SCHEMA, true);
    byte[] converted = converter.fromConnectData(TOPIC, original.schema(), original.value());
    SchemaAndValue schemaAndValue = converter.toConnectData(TOPIC, converted);
    // Because of registration in schema registry and lookup, we'll have added a version number
    SchemaAndValue expected = new SchemaAndValue(SchemaBuilder.bool().version(1).build(), true);
    assertEquals(expected, schemaAndValue);
  }

  @Test
  public void testComplex() {
    SchemaBuilder builder = SchemaBuilder.struct()
        .field("int8", Schema.INT8_SCHEMA)
        .field("int16", Schema.INT16_SCHEMA)
        .field("int32", Schema.INT32_SCHEMA)
        .field("int64", Schema.INT64_SCHEMA)
        .field("float32", Schema.FLOAT32_SCHEMA)
        .field("float64", Schema.FLOAT64_SCHEMA)
        .field("boolean", Schema.BOOLEAN_SCHEMA)
        .field("string", Schema.STRING_SCHEMA)
        .field("bytes", Schema.BYTES_SCHEMA)
        .field("array", SchemaBuilder.array(Schema.STRING_SCHEMA).build())
        .field("map", SchemaBuilder.map(Schema.STRING_SCHEMA, Schema.INT32_SCHEMA).build());
    Schema schema = builder.build();
    Struct original = new Struct(schema).put("int8", (byte) 12)
        .put("int16", (short) 12)
        .put("int32", 12)
        .put("int64", 12L)
        .put("float32", 12.2f)
        .put("float64", 12.2)
        .put("boolean", true)
        .put("string", "foo")
        .put("bytes", "foo".getBytes())
        .put("array", Arrays.asList("a", "b", "c"))
        .put("map", Collections.singletonMap("field", 1));
    // Because of registration in schema registry and lookup, we'll have added a version number
    Schema expectedSchema = builder.version(1).build();
    Struct expected = new Struct(expectedSchema).put("int8", (byte) 12)
        .put("int16", (short) 12)
        .put("int32", 12)
        .put("int64", 12L)
        .put("float32", 12.2f)
        .put("float64", 12.2)
        .put("boolean", true)
        .put("string", "foo")
        .put("bytes", "foo".getBytes())
        .put("array", Arrays.asList("a", "b", "c"))
        .put("map", Collections.singletonMap("field", 1));

    byte[] converted = converter.fromConnectData(TOPIC, original.schema(), original);
    SchemaAndValue schemaAndValue = converter.toConnectData(TOPIC, converted);
    assertEquals(expected, schemaAndValue.value());
  }

  @Test
  public void testComplexWithDefaults() {
    int dateDefVal = 100;
    int timeDefVal = 1000 * 60 * 60 * 2;
    long tsDefVal = 1000 * 60 * 60 * 24 * 365 + 100;
    java.util.Date dateDef = Date.toLogical(Date.SCHEMA, dateDefVal);
    java.util.Date timeDef = Time.toLogical(Time.SCHEMA, timeDefVal);
    java.util.Date tsDef = Timestamp.toLogical(Timestamp.SCHEMA, tsDefVal);
    BigDecimal decimalDef = new BigDecimal(BigInteger.valueOf(314159L), 5);
    SchemaBuilder builder = SchemaBuilder.struct()
        .field("int8", SchemaBuilder.int8().defaultValue((byte) 2).doc("int8 field").build())
        .field("int16", SchemaBuilder.int16().defaultValue((short)12).doc("int16 field").build())
        .field("int32", SchemaBuilder.int32().defaultValue(12).doc("int32 field").build())
        .field("int64", SchemaBuilder.int64().defaultValue(12L).doc("int64 field").build())
        .field("float32", SchemaBuilder.float32().defaultValue(12.2f).doc("float32 field").build())
        .field("float64", SchemaBuilder.float64().defaultValue(12.2).doc("float64 field").build())
        .field("boolean", SchemaBuilder.bool().defaultValue(true).doc("bool field").build())
        .field("string", SchemaBuilder.string().defaultValue("foo").doc("string field").build())
        .field("bytes", SchemaBuilder.bytes().defaultValue("foo".getBytes()).doc("bytes field").build())
        .field("array", SchemaBuilder.array(Schema.STRING_SCHEMA).defaultValue(Arrays.asList("a", "b", "c")).build())
        .field("map", SchemaBuilder.map(Schema.STRING_SCHEMA, Schema.INT32_SCHEMA).defaultValue(Collections.singletonMap("field", 1)).build())
        .field("date", Date.builder().defaultValue(dateDef).doc("date field").build())
        .field("time", Time.builder().defaultValue(timeDef).doc("time field").build())
        .field("ts", Timestamp.builder().defaultValue(tsDef).doc("ts field").build())
        .field("decimal", Decimal.builder(5).defaultValue(decimalDef).doc("decimal field").build());
    Schema schema = builder.build();
    Struct original = new Struct(schema).put("int8", (byte) 12)
        .put("int16", (short) 12)
        .put("int32", 12)
        .put("int64", 12L)
        .put("float32", 12.2f)
        .put("float64", 12.2)
        .put("boolean", true)
        .put("string", "foo")
        .put("bytes", "foo".getBytes())
        .put("array", Arrays.asList("a", "b", "c"))
        .put("map", Collections.singletonMap("field", 1))
        .put("date", dateDef)
        .put("time", timeDef)
        .put("ts", tsDef)
        .put("decimal", decimalDef);
    // Because of registration in schema registry and lookup, we'll have added a version number
    Schema expectedSchema = builder.version(1).build();
    Struct expected = new Struct(expectedSchema).put("int8", (byte) 12)
        .put("int16", (short) 12)
        .put("int32", 12)
        .put("int64", 12L)
        .put("float32", 12.2f)
        .put("float64", 12.2)
        .put("boolean", true)
        .put("string", "foo")
        .put("bytes", "foo".getBytes())
        .put("array", Arrays.asList("a", "b", "c"))
        .put("map", Collections.singletonMap("field", 1))
        .put("date", dateDef)
        .put("time", timeDef)
        .put("ts", tsDef)
        .put("decimal", decimalDef);

    byte[] converted = converter.fromConnectData(TOPIC, original.schema(), original);
    SchemaAndValue schemaAndValue = converter.toConnectData(TOPIC, converted);
    assertEquals(expected, schemaAndValue.value());
  }

  @Test
  public void testNull() {
    byte[] converted = converter.fromConnectData(TOPIC, Schema.OPTIONAL_BOOLEAN_SCHEMA, null);
    SchemaAndValue schemaAndValue = converter.toConnectData(TOPIC, converted);
    assertEquals(new SchemaAndValue(SchemaBuilder.bool().version(1).optional().build(), null),
        schemaAndValue
    );
  }

  @Test
  public void testVersionExtractedForDefaultSubjectNameStrategy() throws Exception {
    // Version info should be extracted even if the data was not created with Copycat. Manually
    // register a few compatible schemas and validate that data serialized with our normal
    // serializer can be read and gets version info inserted
    String subject = TOPIC + "-value";
    KafkaJsonSchemaSerializer serializer = new KafkaJsonSchemaSerializer(schemaRegistry,
        ImmutableMap.of("schema.registry.url", "http://fake-url")
    );
    JsonSchemaConverter jsonConverter = new JsonSchemaConverter(schemaRegistry);
    jsonConverter.configure(Collections.singletonMap("schema.registry.url", "http://fake-url"),
        false
    );
    testVersionExtracted(subject, serializer, jsonConverter);
  }

  private void testVersionExtracted(
      String subject, KafkaJsonSchemaSerializer serializer, JsonSchemaConverter jsonConverter
  ) throws IOException, RestClientException {
    JsonNode rawSchemaJson1 = loader.readJsonNode("key.json");
    JsonNode rawSchemaJson2 = loader.readJsonNode("keyvalue.json");
    schemaRegistry.register(subject, new JsonSchema(rawSchemaJson1));
    schemaRegistry.register(subject, new JsonSchema(rawSchemaJson2));

    ObjectMapper mapper = new ObjectMapper();

    ObjectNode objectNode1 = mapper.createObjectNode();
    objectNode1.put("key", 15);

    ObjectNode objectNode2 = mapper.createObjectNode();
    objectNode2.put("key", 15);
    objectNode2.put("value", "bar");

    // Get serialized data
    byte[] serializedRecord1 = serializer.serialize(TOPIC,
        JsonSchemaUtils.envelope(rawSchemaJson1, objectNode1)
    );
    byte[] serializedRecord2 = serializer.serialize(TOPIC,
        JsonSchemaUtils.envelope(rawSchemaJson2, objectNode2)
    );

    SchemaAndValue converted1 = jsonConverter.toConnectData(TOPIC, serializedRecord1);
    assertEquals(1L, (long) converted1.schema().version());

    SchemaAndValue converted2 = jsonConverter.toConnectData(TOPIC, serializedRecord2);
    assertEquals(2L, (long) converted2.schema().version());
  }

  @Test
  public void testVersionMaintained() {
    // Version info provided from the Copycat schema should be maintained. This should be true
    // regardless of any underlying schema registry versioning since the versions are explicitly
    // specified by the connector.

    // Use newer schema first
    Schema newerSchema = SchemaBuilder.struct()
        .version(2)
        .field("orig", Schema.OPTIONAL_INT16_SCHEMA)
        .field("new", Schema.OPTIONAL_INT16_SCHEMA)
        .build();
    SchemaAndValue newer = new SchemaAndValue(newerSchema,
        new Struct(newerSchema).put("orig", (short) 1).put("new", (short) 2)
    );
    byte[] newerSerialized = converter.fromConnectData(TOPIC, newer.schema(), newer.value());

    Schema olderSchema = SchemaBuilder.struct()
        .version(1)
        .field("orig", Schema.OPTIONAL_INT16_SCHEMA)
        .build();
    SchemaAndValue older = new SchemaAndValue(olderSchema,
        new Struct(olderSchema).put("orig", (short) 1)
    );
    byte[] olderSerialized = converter.fromConnectData(TOPIC, older.schema(), older.value());

    assertEquals(2L, (long) converter.toConnectData(TOPIC, newerSerialized).schema().version());
    assertEquals(1L, (long) converter.toConnectData(TOPIC, olderSerialized).schema().version());
  }

  @Test
  public void testSameSchemaMultipleTopicForValue() throws IOException, RestClientException {
    SchemaRegistryClient schemaRegistry = new MockSchemaRegistryClient();
    JsonSchemaConverter jsonConverter = new JsonSchemaConverter(schemaRegistry);
    jsonConverter.configure(SR_CONFIG, false);
    assertSameSchemaMultipleTopic(jsonConverter, schemaRegistry, false);
  }

  @Test
  public void testSameSchemaMultipleTopicForKey() throws IOException, RestClientException {
    SchemaRegistryClient schemaRegistry = new MockSchemaRegistryClient();
    JsonSchemaConverter jsonConverter = new JsonSchemaConverter(schemaRegistry);
    jsonConverter.configure(SR_CONFIG, true);
    assertSameSchemaMultipleTopic(jsonConverter, schemaRegistry, true);
  }

  @Test
  public void testExplicitlyNamedNestedMapsWithNonStringKeys() {
    final Schema schema = SchemaBuilder.map(Schema.OPTIONAL_STRING_SCHEMA,
        SchemaBuilder.map(Schema.OPTIONAL_STRING_SCHEMA, Schema.OPTIONAL_INT32_SCHEMA)
            .name("foo.bar")
            .build()
    ).name("biz.baz").version(1).build();
    final JsonSchemaConverter jsonConverter =
        new JsonSchemaConverter(new MockSchemaRegistryClient());
    jsonConverter.configure(Collections.singletonMap(AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG,
        "localhost"
    ), false);
    final Object value = Collections.singletonMap("foo", Collections.singletonMap("bar", 1));

    final byte[] bytes = jsonConverter.fromConnectData("topic", schema, value);
    final SchemaAndValue schemaAndValue = jsonConverter.toConnectData("topic", bytes);

    assertEquals(schemaAndValue.schema(), schema);
    assertEquals(schemaAndValue.value(), value);
  }

  private void assertSameSchemaMultipleTopic(
      JsonSchemaConverter converter, SchemaRegistryClient schemaRegistry, boolean isKey
  ) throws IOException, RestClientException {
    JsonNode rawSchemaJson1 = loader.readJsonNode("key.json");
    JsonNode rawSchemaJson2_1 = loader.readJsonNode("keyvalue.json");
    JsonNode rawSchemaJson2_2 = loader.readJsonNode("keyvalue.json");
    String subjectSuffix = isKey ? "key" : "value";
    schemaRegistry.register("topic1-" + subjectSuffix, new JsonSchema(rawSchemaJson2_1));
    schemaRegistry.register("topic2-" + subjectSuffix, new JsonSchema(rawSchemaJson1));
    schemaRegistry.register("topic2-" + subjectSuffix, new JsonSchema(rawSchemaJson2_2));

    ObjectMapper mapper = new ObjectMapper();

    ObjectNode objectNode1 = mapper.createObjectNode();
    objectNode1.put("key", 15);
    objectNode1.put("value", "bar");

    ObjectNode objectNode2 = mapper.createObjectNode();
    objectNode2.put("key", 15);
    objectNode2.put("value", "bar");

    KafkaJsonSchemaSerializer serializer = new KafkaJsonSchemaSerializer(schemaRegistry,
        ImmutableMap.of("schema.registry.url", "http://fake-url")
    );
    byte[] serializedRecord1 = serializer.serialize("topic1",
        JsonSchemaUtils.envelope(rawSchemaJson2_1, objectNode1)
    );
    byte[] serializedRecord2 = serializer.serialize("topic2",
        JsonSchemaUtils.envelope(rawSchemaJson2_2, objectNode2)
    );

    SchemaAndValue converted1 = converter.toConnectData("topic1", serializedRecord1);
    assertEquals(1L, (long) converted1.schema().version());

    SchemaAndValue converted2 = converter.toConnectData("topic2", serializedRecord2);
    assertEquals(2L, (long) converted2.schema().version());

    converted2 = converter.toConnectData("topic2", serializedRecord2);
    assertEquals(2L, (long) converted2.schema().version());
  }

  @Test(expected = NetworkException.class)
  public void testFromConnectDataThrowsNetworkExceptionOnSerializationExceptionCausedByIOException() {
    JsonSchemaConverter.Serializer serializer = mock(JsonSchemaConverter.Serializer.class);
    SerializationException serializationException = new SerializationException("fail", new java.io.IOException("io fail"));
    JsonSchemaData jsonSchemaData = new JsonSchemaData();
    when(serializer.serialize(TOPIC, null, false,
        jsonSchemaData.fromConnectData(Schema.STRING_SCHEMA, "value"),
        jsonSchemaData.fromConnectSchema(Schema.STRING_SCHEMA))).thenThrow(serializationException);

    try {
      java.lang.reflect.Field serializerField = JsonSchemaConverter.class.getDeclaredField("serializer");
      serializerField.setAccessible(true);
      serializerField.set(converter, serializer);
    } catch (Exception e) {
      fail("Reflection failed: " + e);
    }

    converter.fromConnectData(TOPIC, Schema.STRING_SCHEMA, "value");
  }

  @Test(expected = NetworkException.class)
  public void testToConnectDataThrowsNetworkExceptionOnSerializationExceptionCausedByIOException() {
    JsonSchemaConverter.Deserializer deserializer = mock(JsonSchemaConverter.Deserializer.class);
    SerializationException serializationException = new SerializationException("fail", new java.io.IOException("io fail"));
    SchemaAndValue schemaAndValue = new SchemaAndValue(Schema.BOOLEAN_SCHEMA, true);
    byte[] valueBytes =
        converter.fromConnectData(TOPIC, schemaAndValue.schema(), schemaAndValue.value());
    when(deserializer.deserialize(TOPIC, false, null, valueBytes)).thenThrow(
        serializationException);

    try {
      java.lang.reflect.Field deserializerField = JsonSchemaConverter.class.getDeclaredField("deserializer");
      deserializerField.setAccessible(true);
      deserializerField.set(converter, deserializer);
    } catch (Exception e) {
      fail("Reflection failed: " + e);
    }

    converter.toConnectData(TOPIC, valueBytes);
  }
}
