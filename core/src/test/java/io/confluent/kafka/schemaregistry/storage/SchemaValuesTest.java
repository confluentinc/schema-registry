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
import io.confluent.kafka.schemaregistry.client.rest.entities.Schema;
import io.confluent.kafka.schemaregistry.protobuf.ProtobufSchema;
import io.confluent.kafka.schemaregistry.protobuf.ProtobufSchemaProvider;
import java.util.Collections;
import java.util.List;
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

  @Test
  public void testSchemaValueMD5() {
    String schemaString = "{\"type\":\"record\","
        + "\"name\":\"myrecord\","
        + "\"fields\":"
        + "[{\"type\":\"string\",\"default\":null,\"name\":"
        + "\"f" + "\"}]}";

    SchemaValue schema = new SchemaValue("subject", 1, 1, "AVRO", null, schemaString, false);

    // Pin the MD5
    MD5 md5 = new MD5(new byte[]
        { 29, 32, -53, -71, -27, -29, 94, -105, 84, -25, -38, 23, 11, 110, 93, -37 });

    assertEquals("MD5 hash should be equal",
        md5,
        MD5.ofSchema(schema.toSchemaEntity()));

    // Test entities with null values
    Metadata metadata = new Metadata(null, null, null);
    List<Rule> domainRules = Collections.singletonList(new Rule(null, null, RuleKind.TRANSFORM,
        RuleMode.WRITEREAD, null, null, null, null, null, null, false));
    RuleSet ruleSet = new RuleSet(domainRules, null, null);

    schema = new SchemaValue(
        "subject", 1, 1, null, "AVRO", null, metadata, ruleSet, schemaString, false);

    // Pin the MD5
    md5 = new MD5(new byte[]
        { 22, -15, -20, -91, -21, -7, 34, -77, -38, -128, 33, 47, -113, -89, 52, 46 });

    assertEquals("MD5 hash should be equal",
        md5,
        MD5.ofSchema(schema.toSchemaEntity()));
  }

  @Test
  public void testSchemaValueComplexMD5() {
    String schemaString = "{\n"
        + "  \"type\": \"record\",\n"
        + "  \"name\": \"Complex\",\n"
        + "  \"namespace\": \"io.confluent.avro\",\n"
        + "  \"fields\": [\n"
        + "    {\n"
        + "      \"name\": \"int8\",\n"
        + "      \"type\": {\n"
        + "        \"type\": \"int\",\n"
        + "        \"connect.doc\": \"int8 field\",\n"
        + "        \"connect.default\": 2,\n"
        + "        \"connect.type\": \"int8\"\n"
        + "      },\n"
        + "      \"default\": 2\n"
        + "    },\n"
        + "    {\n"
        + "      \"name\": \"int16\",\n"
        + "      \"type\": {\n"
        + "        \"type\": \"int\",\n"
        + "        \"connect.type\": \"int16\"\n"
        + "      }\n"
        + "    },\n"
        + "    {\n"
        + "      \"name\": \"int32\",\n"
        + "      \"type\": \"int\"\n"
        + "    },\n"
        + "    {\n"
        + "      \"name\": \"int64\",\n"
        + "      \"type\": \"long\"\n"
        + "    },\n"
        + "    {\n"
        + "      \"name\": \"float32\",\n"
        + "      \"type\": \"float\"\n"
        + "    },\n"
        + "    {\n"
        + "      \"name\": \"float64\",\n"
        + "      \"type\": \"double\"\n"
        + "    },\n"
        + "    {\n"
        + "      \"name\": \"boolean\",\n"
        + "      \"type\": \"boolean\"\n"
        + "    },\n"
        + "    {\n"
        + "      \"name\": \"string\",\n"
        + "      \"type\": \"string\"\n"
        + "    },\n"
        + "    {\n"
        + "      \"name\": \"bytes\",\n"
        + "      \"type\": \"bytes\"\n"
        + "    },\n"
        + "    {\n"
        + "      \"name\": \"array\",\n"
        + "      \"type\": {\n"
        + "        \"type\": \"array\",\n"
        + "        \"items\": \"string\"\n"
        + "      }\n"
        + "    },\n"
        + "    {\n"
        + "      \"name\": \"map\",\n"
        + "      \"type\": {\n"
        + "        \"type\": \"map\",\n"
        + "        \"values\": \"int\"\n"
        + "      }\n"
        + "    },\n"
        + "    {\n"
        + "      \"name\": \"mapNonStringKeys\",\n"
        + "      \"type\": {\n"
        + "        \"type\": \"array\",\n"
        + "        \"items\": {\n"
        + "          \"type\": \"record\",\n"
        + "          \"name\": \"MapEntry\",\n"
        + "          \"fields\": [\n"
        + "            {\n"
        + "              \"name\": \"key\",\n"
        + "              \"type\": \"int\"\n"
        + "            },\n"
        + "            {\n"
        + "              \"name\": \"value\",\n"
        + "              \"type\": \"int\"\n"
        + "            }\n"
        + "          ]\n"
        + "        }\n"
        + "      }\n"
        + "    }\n"
        + "  ]\n"
        + "}\n";

    SchemaValue schema = new SchemaValue("subject", 1, 1, "AVRO", null, schemaString, false);

    // Pin the MD5
    MD5 md5 = MD5.fromString("c4ad6448-4d68-34d2-c8cf-7443afc25d54");

    assertEquals("MD5 hash should be equal",
        md5,
        MD5.ofSchema(schema.toSchemaEntity()));
  }

  @Test
  public void testSchemaValueComplexProtobufMD5() {
    String schemaString = "syntax = \"proto3\";\n"
        + "\n"
        + "message Complex {\n"
        + "  int32 int32 = 1;\n"
        + "  int64 int64 = 2;\n"
        + "  float float32 = 3;\n"
        + "  double float64 = 4;\n"
        + "  bool boolean = 5;\n"
        + "  string string = 6;\n"
        + "  bytes bytes = 7;\n"
        + "  repeated string array = 8;\n"
        + "  repeated ComplexEntry map = 9;\n"
        + "\n"
        + "  message ComplexEntry {\n"
        + "    string key = 1;\n"
        + "    int32 value = 2;\n"
        + "  }\n"
        + "}\n";

    SchemaValue schema = new SchemaValue("subject", 1, 1, "PROTOBUF", null, schemaString, false);

    // Pin the MD5
    MD5 md5 = MD5.fromString("d49cbf63-d35b-ba54-e1f7-0bdd51528a37");

    assertEquals("MD5 hash should be equal",
        md5,
        MD5.ofSchema(schema.toSchemaEntity()));
  }

  @Test
  public void testSchemaValueComplexJsonMD5() {
    String schemaString = "\n"
        + "  \"type\": \"object\",\n"
        + "  \"properties\": {\n"
        + "    \"boolean\": {\n"
        + "      \"connect.index\": 6,\n"
        + "      \"type\": \"boolean\"\n"
        + "    },\n"
        + "    \"string\": {\n"
        + "      \"type\": \"string\",\n"
        + "      \"connect.index\": 7\n"
        + "    },\n"
        + "    \"int32\": {\n"
        + "      \"type\": \"integer\",\n"
        + "      \"connect.index\": 2,\n"
        + "      \"connect.type\": \"int32\"\n"
        + "    },\n"
        + "    \"array\": {\n"
        + "      \"type\": \"array\",\n"
        + "      \"connect.index\": 9,\n"
        + "      \"items\": {\n"
        + "        \"type\": \"string\"\n"
        + "      }\n"
        + "    },\n"
        + "    \"int64\": {\n"
        + "      \"type\": \"integer\",\n"
        + "      \"connect.index\": 3,\n"
        + "      \"connect.type\": \"int64\"\n"
        + "    },\n"
        + "    \"bytes\": {\n"
        + "      \"type\": \"string\",\n"
        + "      \"connect.index\": 8,\n"
        + "      \"connect.type\": \"bytes\"\n"
        + "    },\n"
        +  "\"int8\": {\n"
        + "      \"type\": \"integer\",\n"
        + "      \"connect.index\": 0,\n"
        + "      \"connect.type\": \"int8\"\n"
        + "    },\n"
        + "    \"float32\": {\n"
        + "      \"type\": \"number\",\n"
        + "      \"connect.index\": 4,\n"
        + "      \"connect.type\": \"float32\"\n"
        + "    },\n"
        + "    \"float64\": {\n"
        + "      \"type\": \"number\",\n"
        + "      \"connect.index\": 5,\n"
        + "      \"connect.type\": \"float64\"\n"
        + "    },\n"
        + "    \"map\": {\n"
        + "      \"type\": \"object\",\n"
        + "      \"connect.index\": 10,\n"
        + "      \"connect.type\": \"map\",\n"
        + "      \"additionalProperties\": {\n"
        + "        \"type\": \"integer\",\n"
        + "        \"connect.type\": \"int32\"\n"
        + "      }\n"
        + "    },\n"
        + "    \"int16\": {\n"
        + "      \"type\": \"integer\",\n"
        + "      \"connect.index\": 1,\n"
        + "      \"connect.type\": \"int16\"\n"
        + "    }\n"
        + "  }\n"
        + "}\n";

    SchemaValue schema = new SchemaValue("subject", 1, 1, "JSON", null, schemaString, false);

    // Pin the MD5
    MD5 md5 = MD5.fromString("ae6f9592-621e-546e-45b2-dfb7ca884c15");

    assertEquals("MD5 hash should be equal",
        md5,
        MD5.ofSchema(schema.toSchemaEntity()));
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
