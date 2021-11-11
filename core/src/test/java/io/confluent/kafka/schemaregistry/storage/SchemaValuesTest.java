/*
 * Copyright 2018 Confluent Inc.
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

package io.confluent.kafka.schemaregistry.storage;

import io.confluent.kafka.schemaregistry.avro.AvroSchema;
import io.confluent.kafka.schemaregistry.protobuf.ProtobufSchema;
import io.confluent.kafka.schemaregistry.protobuf.ProtobufSchemaProvider;
import org.junit.Test;

import io.confluent.kafka.schemaregistry.storage.exceptions.SerializationException;
import io.confluent.kafka.schemaregistry.storage.serialization.SchemaRegistrySerializer;
import io.confluent.kafka.schemaregistry.storage.serialization.Serializer;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.fail;

public class SchemaValuesTest {


  @Test
  public void testSchemaValueDeserializeForMagicByte0() throws SerializationException {
    String subject = "test";
    int version = 1;
    SchemaKey key = new SchemaKey(subject, version);
    key.setMagicByte(0);
    Serializer<SchemaRegistryKey, SchemaRegistryValue> serializer = new SchemaRegistrySerializer();

    String schemaValueJson = "{\"subject\":\"test\",\"version\":1,\"id\":1,"
                             + "\"schema\":\"{\\\"type\\\":\\\"record\\\","
                             + "\\\"name\\\":\\\"myrecord\\\",\\\"fields\\\":"
                             + "[{\\\"name\\\":\\\"f1067572235\\\","
                             + "\\\"type\\\":\\\"string\\\"}]}\"}";

    SchemaValue schemaValue =
        (SchemaValue) serializer.deserializeValue(key, schemaValueJson.getBytes());

    assertSchemaValue(subject, version, 1,
                      "{\"type\":\"record\",\"name\":\"myrecord\","
                      + "\"fields\":[{\"name\":\"f1067572235\",\"type\":\"string\"}]}",
                      AvroSchema.TYPE, false, schemaValue);

  }

  @Test
  public void testSchemaValueDeserializeForMagicByte1WithDeleteFlagFalse() throws
                                                                           SerializationException {
    String subject = "test";
    int version = 1;
    SchemaKey key = new SchemaKey(subject, version);
    key.setMagicByte(1);
    Serializer<SchemaRegistryKey, SchemaRegistryValue> serializer = new SchemaRegistrySerializer();

    String
        schemaValueJson = "{\"subject\":\"test\",\"version\":1,\"id\":1,"
                          + "\"schema\":\"{\\\"type\\\":\\\"record\\\","
                          + "\\\"name\\\":\\\"myrecord\\\","
                          + "\\\"fields\\\":[{\\\"name\\\":\\\"f1067572235\\\","
                          + "\\\"type\\\":\\\"string\\\"}]}\",\"deleted\":false}";

    SchemaValue schemaValue =
        (SchemaValue) serializer.deserializeValue(key, schemaValueJson.getBytes());

    assertSchemaValue(subject, version, 1,
                      "{\"type\":\"record\",\"name\":\"myrecord\","
                      + "\"fields\":[{\"name\":\"f1067572235\",\"type\":\"string\"}]}",
                      AvroSchema.TYPE, false, schemaValue);

  }

  @Test
  public void testSchemaValueDeserializeForMagicByte1WithDeleteFlagTrue() throws
                                                                          SerializationException {
    String subject = "test";
    int version = 1;
    SchemaKey key = new SchemaKey(subject, version);
    key.setMagicByte(1);
    Serializer<SchemaRegistryKey, SchemaRegistryValue> serializer = new SchemaRegistrySerializer();

    String
        schemaValueJson = "{\"subject\":\"test\",\"version\":1,\"id\":1,"
                          + "\"schema\":\"{\\\"type\\\":\\\"record\\\","
                          + "\\\"name\\\":\\\"myrecord\\\","
                          + "\\\"fields\\\":[{\\\"name\\\":\\\"f1067572235\\\","
                          + "\\\"type\\\":\\\"string\\\"}]}\",\"deleted\":true}";

    SchemaValue schemaValue =
        (SchemaValue) serializer.deserializeValue(key, schemaValueJson.getBytes());

    assertSchemaValue(subject, version, 1,
                      "{\"type\":\"record\",\"name\":\"myrecord\","
                      + "\"fields\":[{\"name\":\"f1067572235\",\"type\":\"string\"}]}",
                      AvroSchema.TYPE,true, schemaValue);

  }

  @Test
  public void testSchemaValueDeserializeForUnSupportedMagicByte() {
    String subject = "test";
    int version = 1;
    SchemaKey key = new SchemaKey(subject, version);
    key.setMagicByte(2);
    Serializer<SchemaRegistryKey, SchemaRegistryValue> serializer = new SchemaRegistrySerializer();

    String
        schemaValueJson = "{\"subject\":\"test\",\"version\":1,\"id\":1,"
                          + "\"schema\":\"{\\\"type\\\":\\\"record\\\","
                          + "\\\"name\\\":\\\"myrecord\\\","
                          + "\\\"fields\\\":[{\\\"name\\\":\\\"f1067572235\\\","
                          + "\\\"type\\\":\\\"string\\\"}]}\",\"deleted\":true}";

    try {
      serializer.deserializeValue(key, schemaValueJson.getBytes());
      fail("Deserialization shouldn't be supported");
    } catch (SerializationException e) {
      assertEquals("Can't deserialize schema for the magic byte 2", e.getMessage());
    }

  }

  @Test
  public void testSchemaValueDeserializeForOffsetTimestamp() throws SerializationException {
    String subject = "test";
    int version = 1;
    SchemaKey key = new SchemaKey(subject, version);
    key.setMagicByte(1);
    Serializer<SchemaRegistryKey, SchemaRegistryValue> serializer = new SchemaRegistrySerializer();

    String
        schemaValueJson = "{\"subject\":\"test\",\"version\":1,\"id\":1,"
        + "\"schema\":\"{\\\"type\\\":\\\"record\\\","
        + "\\\"name\\\":\\\"myrecord\\\","
        + "\\\"fields\\\":[{\\\"name\\\":\\\"f1067572235\\\","
        + "\\\"type\\\":\\\"string\\\"}]}\",\"deleted\":true,\"offset\":1,\"ts\":123}";

    SchemaValue schemaValue =
        (SchemaValue) serializer.deserializeValue(key, schemaValueJson.getBytes());

    assertEquals(1L, schemaValue.getOffset().longValue());
    assertEquals(123L, schemaValue.getTimestamp().longValue());
  }

  @Test
  public void testSchemaValueCanonicalize() {
    String oldSchema = "syntax = \"proto3\";\npackage com.mycorp.mynamespace;\n\n// Test Comment.\r\nmessage value {\n  int32 myField1 = 1;\n}\n";
    String newSchema = "syntax = \"proto3\";\npackage com.mycorp.mynamespace;\n\nmessage value {\n  int32 myField1 = 1;\n}\n";
    SchemaValue schemaValue = new SchemaValue("sub", 1, 0, ProtobufSchema.TYPE, null, oldSchema, false);
    KafkaStoreMessageHandler.canonicalize(new ProtobufSchemaProvider(), schemaValue);
    assertEquals(newSchema, schemaValue.getSchema());
  }

  private void assertSchemaValue(String subject, int version, int schemaId,
                                 String schema, String type, boolean deleted,
                                 SchemaValue schemaValue) {
    assertNotNull("Not Null", schemaValue);
    assertEquals("Subject Matches", subject, schemaValue.getSubject());
    assertEquals("Version matches", (Integer) version, schemaValue.getVersion());
    assertEquals("SchemaId matches", (Integer) schemaId, schemaValue.getId());
    assertEquals("Schema Matches", schema, schemaValue.getSchema());
    assertEquals("Type matches", type, schemaValue.getSchemaType());
    assertEquals("Delete Flag Matches", deleted, schemaValue.isDeleted());
  }
}
