/**
 * Copyright 2015 Confluent Inc.
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
 **/

package io.confluent.connect.avro;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.junit.Before;
import org.junit.Test;
import org.powermock.reflect.Whitebox;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Collections;
import java.util.Map;

import io.confluent.kafka.schemaregistry.client.MockSchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.serializers.AbstractKafkaAvroSerDe;
import io.confluent.kafka.serializers.AbstractKafkaAvroSerDeConfig;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

// AvroConverter is a trivial combination of the serializers and the AvroData conversions, so
// most testing is performed on AvroData since it is much easier to compare the results in Avro
// runtime format than in serialized form. This just adds a few sanity checks to make sure things
// work end-to-end.
public class AvroConverterTest {
  private static final String TOPIC = "topic";

  private static final Map<String, ?> SR_CONFIG = Collections.singletonMap(
      AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, "localhost");

  private final SchemaRegistryClient schemaRegistry;
  private final AvroConverter converter;

  public AvroConverterTest() {
    schemaRegistry = new MockSchemaRegistryClient();
    converter = new AvroConverter(schemaRegistry);
  }

  @Before
  public void setUp() {
    converter.configure(Collections.singletonMap("schema.registry.url", "http://fake-url"), false);
  }

  @Test
  public void testConfigure() {
    converter.configure(SR_CONFIG, true);
    assertTrue(Whitebox.<Boolean>getInternalState(converter, "isKey"));
    assertNotNull(Whitebox.getInternalState(
        Whitebox.<AbstractKafkaAvroSerDe>getInternalState(converter, "serializer"),
        "schemaRegistry"));
  }

  @Test
  public void testConfigureAlt() {
    converter.configure(SR_CONFIG, false);
    assertFalse(Whitebox.<Boolean>getInternalState(converter, "isKey"));
    assertNotNull(Whitebox.getInternalState(
        Whitebox.<AbstractKafkaAvroSerDe>getInternalState(converter, "serializer"),
        "schemaRegistry"));
  }

  @Test
  public void testPrimitive() {
    SchemaAndValue original = new SchemaAndValue(Schema.BOOLEAN_SCHEMA, true);
    byte[] converted = converter.fromConnectData(TOPIC, original.schema(), original.value());
    SchemaAndValue schemaAndValue = converter.toConnectData(TOPIC, converted);
    assertEquals(original, schemaAndValue);
  }

  @Test
  public void testComplex() {
    Schema schema = SchemaBuilder.struct()
        .field("int8", SchemaBuilder.int8().defaultValue((byte) 2).doc("int8 field").build())
        .field("int16", Schema.INT16_SCHEMA)
        .field("int32", Schema.INT32_SCHEMA)
        .field("int64", Schema.INT64_SCHEMA)
        .field("float32", Schema.FLOAT32_SCHEMA)
        .field("float64", Schema.FLOAT64_SCHEMA)
        .field("boolean", Schema.BOOLEAN_SCHEMA)
        .field("string", Schema.STRING_SCHEMA)
        .field("bytes", Schema.BYTES_SCHEMA)
        .field("array", SchemaBuilder.array(Schema.STRING_SCHEMA).build())
        .field("map", SchemaBuilder.map(Schema.STRING_SCHEMA, Schema.INT32_SCHEMA).build())
        .field("mapNonStringKeys", SchemaBuilder.map(Schema.INT32_SCHEMA, Schema.INT32_SCHEMA).build())
        .build();
    Struct original = new Struct(schema)
        .put("int8", (byte) 12)
        .put("int16", (short) 12)
        .put("int32", 12)
        .put("int64", 12L)
        .put("float32", 12.2f)
        .put("float64", 12.2)
        .put("boolean", true)
        .put("string", "foo")
        .put("bytes", ByteBuffer.wrap("foo".getBytes()))
        .put("array", Arrays.asList("a", "b", "c"))
        .put("map", Collections.singletonMap("field", 1))
        .put("mapNonStringKeys", Collections.singletonMap(1, 1));

    byte[] converted = converter.fromConnectData(TOPIC, original.schema(), original);
    SchemaAndValue schemaAndValue = converter.toConnectData(TOPIC, converted);
    assertEquals(original, schemaAndValue.value());
  }


  @Test
  public void testNull() {
    // Because of the way our serialization works, it's expected that we'll lose schema information
    // when the entire schema is optional.
    byte[] converted = converter.fromConnectData(TOPIC, Schema.OPTIONAL_BOOLEAN_SCHEMA, null);
    SchemaAndValue schemaAndValue = converter.toConnectData(TOPIC, converted);
    assertNull(schemaAndValue);
  }
}
