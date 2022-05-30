/*
 * Copyright 2022 Confluent Inc.
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

package io.confluent.kafka.schemaregistry.maven.derive.schema;

import io.confluent.kafka.schemaregistry.maven.derive.schema.utils.ReadFileUtils;
import com.google.gson.JsonElement;
import com.google.gson.JsonParser;
import io.confluent.kafka.schemaregistry.avro.AvroSchema;
import io.confluent.kafka.schemaregistry.avro.AvroSchemaUtils;
import io.confluent.kafka.schemaregistry.client.MockSchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import io.confluent.kafka.serializers.KafkaAvroDeserializerConfig;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import org.json.JSONArray;
import org.json.JSONObject;
import org.junit.Test;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.PrintStream;
import java.nio.charset.StandardCharsets;
import java.util.*;

import static org.junit.Assert.assertEquals;


public class UnionTests {

  private final KafkaAvroSerializer avroSerializer;
  private final KafkaAvroDeserializer avroDeserializer;
  private final String topic;
  private final DeriveAvroSchema schemaGenerator = new DeriveAvroSchema(true);


  public UnionTests() {

    Properties defaultConfig = new Properties();
    defaultConfig.put(KafkaAvroDeserializerConfig.SCHEMA_REGISTRY_URL_CONFIG, "bogus");
    SchemaRegistryClient schemaRegistry = new MockSchemaRegistryClient();
    avroSerializer = new KafkaAvroSerializer(schemaRegistry, new HashMap(defaultConfig));
    avroDeserializer = new KafkaAvroDeserializer(schemaRegistry);
    topic = "test";

  }

  void serializeAndDeserializeCheckMulti(List<String> messages, List<JSONObject> schemas) throws IOException {

    for (JSONObject schemaInfo : schemas) {

      AvroSchema schema = new AvroSchema(schemaInfo.getJSONObject("schema").toString());
      JSONArray matchingMessages = schemaInfo.getJSONArray("messagesMatched");
      for (int i = 0; i < matchingMessages.length(); i++) {
        int index = matchingMessages.getInt(i);
        serializeAndDeserializeCheck(messages.get(index), schema);
      }

    }

  }

  void serializeAndDeserializeCheck(String message, AvroSchema schema) throws IOException {

    Object test = AvroSchemaUtils.toObject(message, schema);
    byte[] bytes = this.avroSerializer.serialize(this.topic, test);
    assertEquals(test, this.avroDeserializer.deserialize(this.topic, bytes, schema.rawSchema()));

    final ByteArrayOutputStream bs = new ByteArrayOutputStream();
    final String utf8 = StandardCharsets.UTF_8.name();
    try (PrintStream ps = new PrintStream(bs, true, utf8)) {
      AvroSchemaUtils.toJson(test, ps);
    }

    JsonElement jsonElement1 = JsonParser.parseString(message);
    JsonElement jsonElement2 = JsonParser.parseString(bs.toString());
    assertEquals(jsonElement1, jsonElement2);

  }


  @Test
  public void TestUnionsBasic() throws IOException {

    /*
    Merging branches of primitive Types and Array
    */

    String message1 = "[\n" +
        "  {\"length\": {\"long\": 12}},\n" +
        "  {\"length\": {\"double\": 1.5}},\n" +
        "  {\"length\": {\"boolean\": true}},\n" +
        "  {\"length\": {\"array\": [true, false, true, false]}}\n" +
        "]";

    List<String> messages1 = ReadFileUtils.readMessagesToString(message1);
    List<JSONObject> schemas1 = schemaGenerator.getSchemaForMultipleMessages(messages1);
    serializeAndDeserializeCheckMulti(messages1, schemas1);
    String expectedSchema1 = "{\"name\":\"Record\",\"type\":\"record\",\"fields\":[{\"name\":\"length\",\"type\":[\"boolean\",\"double\",\"long\",{\"name\":\"array\",\"type\":\"array\",\"items\":\"boolean\"}]}]}";
    assertEquals(expectedSchema1, schemas1.get(0).getJSONObject("schema").toString());


    /*
    Merging branches of Primitive Types and Null
    */

    String message2 = "[\n" +
        "  {\"length\": null},\n" +
        "  {\"length\": {\"long\": 12}},\n" +
        "  {\"length\": {\"double\": 1.5}}\n" +
        "]";

    List<String> messages2 = ReadFileUtils.readMessagesToString(message2);
    List<JSONObject> schemas2 = schemaGenerator.getSchemaForMultipleMessages(messages2);
    serializeAndDeserializeCheckMulti(messages2, schemas2);
    String expectedSchema2 = "{\"name\":\"Record\",\"type\":\"record\",\"fields\":[{\"name\":\"length\",\"type\":[\"double\",\"long\",\"null\"]}]}";
    assertEquals(expectedSchema2, schemas2.get(0).getJSONObject("schema").toString());


    /*
    Merging branches of primitive Types, array and records of different types
    */

    String message3 = "[\n" +
        "  {\"length\": {\"long\": 12}},\n" +
        "  {\"length\": {\"double\": 1.5}},\n" +
        "  {\"length\": {\"double\": 1.325}},\n" +
        "  {\"length\": {\"branch\": {\"properties_length\": {\"temp\": 12}}}},\n" +
        "  {\"length\": {\"array\": [[[1, 2, 3, 4]]]}},\n" +
        "  {\"length\": {\"branch2\": {\"haha\": {\"new\": 5, \"old\": true}}}},\n" +
        "  {\"length\": {\"aa\": {\"a\": {\"new\": 5, \"old\": true}}}}\n" +
        "]";

    List<String> messages3 = ReadFileUtils.readMessagesToString(message3);
    List<JSONObject> schemas3 = schemaGenerator.getSchemaForMultipleMessages(messages3);
    serializeAndDeserializeCheckMulti(messages3, schemas3);
    String expectedSchema3 = "{\"name\":\"Record\",\"type\":\"record\",\"fields\":[{\"name\":\"length\",\"type\":[\"double\",\"long\",{\"name\":\"aa\",\"type\":\"record\",\"fields\":[{\"name\":\"a\",\"type\":{\"name\":\"a\",\"fields\":[{\"name\":\"new\",\"type\":\"int\"},{\"name\":\"old\",\"type\":\"boolean\"}],\"type\":\"record\"}}]},{\"name\":\"array\",\"type\":\"array\",\"items\":{\"name\":\"array\",\"type\":\"array\",\"items\":{\"name\":\"array\",\"type\":\"array\",\"items\":\"int\"}}},{\"name\":\"branch\",\"type\":\"record\",\"fields\":[{\"name\":\"properties_length\",\"type\":{\"name\":\"properties_length\",\"fields\":[{\"name\":\"temp\",\"type\":\"int\"}],\"type\":\"record\"}}]},{\"name\":\"branch2\",\"type\":\"record\",\"fields\":[{\"name\":\"haha\",\"type\":{\"name\":\"haha\",\"fields\":[{\"name\":\"new\",\"type\":\"int\"},{\"name\":\"old\",\"type\":\"boolean\"}],\"type\":\"record\"}}]}]}]}";
    assertEquals(expectedSchema3, schemas3.get(0).getJSONObject("schema").toString());

  }

  @Test
  public void RecursiveTestUnions() throws IOException {

    /*
    recursiveTest - Merging branches of null and records, null and int and merging of null and record which has
    a field of type Union

    */

    File file = new File("src/test/resources/Avro/UnionTestData/recursiveUnionsTest.json");
    List<String> messages = ReadFileUtils.readMessagesToString(file);
    List<JSONObject> schemas = schemaGenerator.getSchemaForMultipleMessages(messages);
    serializeAndDeserializeCheckMulti(messages, schemas);

    String expectedSchema = "{\"name\":\"Record\",\"type\":\"record\",\"fields\":[{\"name\":\"__confluent_index\",\"type\":\"int\"},{\"name\":\"headers\",\"type\":{\"type\":\"array\",\"items\":[]}},{\"name\":\"key\",\"type\":\"null\"},{\"name\":\"offset\",\"type\":\"int\"},{\"name\":\"partition\",\"type\":\"int\"},{\"name\":\"timestamp\",\"type\":\"long\"},{\"name\":\"timestampType\",\"type\":\"string\"},{\"name\":\"topic\",\"type\":\"string\"},{\"name\":\"value\",\"type\":{\"name\":\"value\",\"fields\":[{\"name\":\"bot\",\"type\":[\"boolean\"]},{\"name\":\"comment\",\"type\":[\"string\"]},{\"name\":\"id\",\"type\":[\"long\",\"null\"]},{\"name\":\"length\",\"type\":[\"null\",{\"name\":\"properties.length\",\"type\":\"record\",\"fields\":[{\"name\":\"new\",\"type\":[\"long\"]},{\"name\":\"old\",\"type\":[\"long\",\"null\"]}]}]},{\"name\":\"log_action\",\"type\":[\"null\",\"string\"]},{\"name\":\"log_action_comment\",\"type\":[\"null\",\"string\"]},{\"name\":\"log_id\",\"type\":[\"long\",\"null\"]},{\"name\":\"log_type\",\"type\":[\"null\",\"string\"]},{\"name\":\"meta\",\"type\":{\"name\":\"meta\",\"fields\":[{\"name\":\"domain\",\"type\":[\"string\"]},{\"name\":\"dt\",\"type\":\"long\"},{\"name\":\"id\",\"type\":\"string\"},{\"name\":\"request_id\",\"type\":[\"string\"]},{\"name\":\"stream\",\"type\":\"string\"},{\"name\":\"uri\",\"type\":[\"string\"]}],\"type\":\"record\"}},{\"name\":\"minor\",\"type\":[\"boolean\",\"null\"]},{\"name\":\"namespace\",\"type\":[\"long\"]},{\"name\":\"parsedcomment\",\"type\":[\"string\"]},{\"name\":\"patrolled\",\"type\":[\"boolean\",\"null\"]},{\"name\":\"revision\",\"type\":[\"null\",{\"name\":\"properties.revision\",\"type\":\"record\",\"fields\":[{\"name\":\"new\",\"type\":[\"long\"]},{\"name\":\"old\",\"type\":[\"long\",\"null\"]}]}]},{\"name\":\"server_name\",\"type\":[\"string\"]},{\"name\":\"server_script_path\",\"type\":[\"string\"]},{\"name\":\"server_url\",\"type\":[\"string\"]},{\"name\":\"timestamp\",\"type\":[\"long\"]},{\"name\":\"title\",\"type\":[\"string\"]},{\"name\":\"type\",\"type\":[\"string\"]},{\"name\":\"user\",\"type\":[\"string\"]},{\"name\":\"wiki\",\"type\":[\"string\"]}],\"type\":\"record\"}}]}";
    assertEquals(expectedSchema, schemas.get(0).getJSONObject("schema").toString());

    /*
      Recursive test of merging branches of records and null
     */
    String message1 = "[\n" +
        "  {\n" +
        "    \"value\": {\"length\": {\"properties_length\": {\"new\": {\"long\": 5}, \"old\": {\"long\": 53}}}},\n" +
        "    \"__confluent_index\": 1\n" +
        "  },\n" +
        "  {\n" +
        "    \"value\": {\"length\": {\"properties_length\": {\"new\": {\"long\": 10}, \"old\": null}}},\n" +
        "    \"__confluent_index\": 14\n" +
        "  },\n" +
        "  {\n" +
        "    \"value\": {\"length\": {\"properties_length\": {\"new\": {\"long\": 10}, \"old\": null}}},\n" +
        "    \"__confluent_index\": 123\n" +
        "  },\n" +
        "  {\n" +
        "    \"value\": {\"length\": null},\n" +
        "    \"__confluent_index\": 18\n" +
        "  },\n" +
        "  {\n" +
        "    \"value\": {\"length\": {\"name\": {\"first\": \"Jinit\", \"second\": \"Shah\"}}},\n" +
        "    \"__confluent_index\": 11.4\n" +
        "  }\n" +
        "]";

    List<String> messages1 = ReadFileUtils.readMessagesToString(message1);
    List<JSONObject> schemas1 = schemaGenerator.getSchemaForMultipleMessages(messages1);
    serializeAndDeserializeCheckMulti(messages1, schemas1);

    String expectedSchema1 = "{\"name\":\"Record\",\"type\":\"record\",\"fields\":[{\"name\":\"__confluent_index\",\"type\":\"double\"},{\"name\":\"value\",\"type\":{\"name\":\"value\",\"fields\":[{\"name\":\"length\",\"type\":[\"null\",{\"name\":\"name\",\"type\":\"record\",\"fields\":[{\"name\":\"first\",\"type\":\"string\"},{\"name\":\"second\",\"type\":\"string\"}]},{\"name\":\"properties_length\",\"type\":\"record\",\"fields\":[{\"name\":\"new\",\"type\":[\"long\"]},{\"name\":\"old\",\"type\":[\"long\",\"null\"]}]}]}],\"type\":\"record\"}}]}";
    assertEquals(expectedSchema1, schemas1.get(0).getJSONObject("schema").toString());
  }

  @Test
  public void TestUnionsMerging() throws IOException {

    /*
    Case when first message doesn't have all fields,
    Second and Third has union and can be merged, this is the most occurring type
    *

     */
    String message = "[\n" +
        "  {\"name\" : \"Testing Merging check for union\"}" +
        "  {\"length\": {\"long\": 12\n}}," +
        "  {\"length\": {\"string\": \"2\"}}" +
        "]";
    List<String> messages = ReadFileUtils.readMessagesToString(message);
    List<JSONObject> schemas = schemaGenerator.getSchemaForMultipleMessages(messages);
    serializeAndDeserializeCheckMulti(messages, schemas);

    String expectedSchema1 = "{\"name\":\"Record\",\"type\":\"record\",\"fields\":[{\"name\":\"length\",\"type\":[\"long\",\"string\"]}]}";
    assertEquals(expectedSchema1, schemas.get(0).getJSONObject("schema").toString());

    String expectedSchema2 = "{\"name\":\"Record\",\"type\":\"record\",\"fields\":[{\"name\":\"name\",\"type\":\"string\"}]}";
    assertEquals(expectedSchema2, schemas.get(1).getJSONObject("schema").toString());

  }

  @Test
  public void TestArrayOfUnions() throws IOException {

    /*
    Testing Array of Unions
     */
    String message = "[{ \"A\" : [{\"string\":\"12\"}, {\"int\":12}]}, { \"A\" : [{\"double\":12.5}]}]";
    List<String> messages = ReadFileUtils.readMessagesToString(message);
    List<JSONObject> schemas = schemaGenerator.getSchemaForMultipleMessages(messages);
    serializeAndDeserializeCheckMulti(messages, schemas);

    String expectedSchema = "{\"name\":\"Record\",\"type\":\"record\",\"fields\":[{\"name\":\"A\",\"type\":{\"type\":\"array\",\"items\":[\"double\",\"int\",\"string\"]}}]}";
    assertEquals(expectedSchema, schemas.get(0).getJSONObject("schema").toString());

  }

  @Test
  public void TestingReadFolder() throws IOException {

    File file = new File("src/test/resources/Avro/UnionTestData/arrayOfUnionsTest.txt");
    List<String> messages = ReadFileUtils.readMessagesToString(file);
    List<JSONObject> schemas = schemaGenerator.getSchemaForMultipleMessages(messages);
    serializeAndDeserializeCheckMulti(messages, schemas);

    String expectedSchema = "{\"name\":\"Record\",\"type\":\"record\",\"fields\":[{\"name\":\"aa\",\"type\":\"double\"},{\"name\":\"ahha\",\"type\":\"int\"},{\"name\":\"arr\",\"type\":{\"type\":\"array\",\"items\":[\"int\",\"long\",\"null\",{\"name\":\"array\",\"type\":\"array\",\"items\":\"string\"},{\"name\":\"org.apache.avro.test.TestRecord2\",\"type\":\"record\",\"fields\":[{\"name\":\"arr\",\"type\":{\"type\":\"array\",\"items\":{\"name\":\"arr\",\"type\":\"record\",\"fields\":[{\"name\":\"arrFloat\",\"type\":{\"type\":\"array\",\"items\":\"int\"}},{\"name\":\"g\",\"type\":\"double\"},{\"name\":\"l\",\"type\":{\"type\":\"array\",\"items\":[\"long\",\"null\"]}},{\"name\":\"name\",\"type\":\"string\"}]}}},{\"name\":\"arrFloat\",\"type\":{\"type\":\"array\",\"items\":\"double\"}},{\"name\":\"l\",\"type\":{\"type\":\"array\",\"items\":[\"boolean\",\"long\",\"null\"]}},{\"name\":\"name\",\"type\":\"string\"}]},{\"name\":\"org.apache.avro.test.TestRecord3\",\"type\":\"record\",\"fields\":[{\"name\":\"arrFloat\",\"type\":{\"type\":\"array\",\"items\":\"int\"}},{\"name\":\"g\",\"type\":\"double\"},{\"name\":\"l\",\"type\":{\"type\":\"array\",\"items\":[\"long\",\"null\"]}},{\"name\":\"name\",\"type\":\"string\"}]}]}},{\"name\":\"arr2\",\"type\":{\"type\":\"array\",\"items\":{\"name\":\"arr2\",\"type\":\"record\",\"fields\":[{\"name\":\"arr\",\"type\":{\"type\":\"array\",\"items\":{\"name\":\"arr\",\"type\":\"record\",\"fields\":[{\"name\":\"arrFloat\",\"type\":{\"type\":\"array\",\"items\":\"int\"}},{\"name\":\"g\",\"type\":\"double\"},{\"name\":\"l\",\"type\":{\"type\":\"array\",\"items\":[\"long\",\"null\"]}},{\"name\":\"name\",\"type\":\"string\"}]}}},{\"name\":\"arrFloat\",\"type\":{\"type\":\"array\",\"items\":\"double\"}},{\"name\":\"l\",\"type\":{\"type\":\"array\",\"items\":[\"boolean\",\"long\",\"null\"]}},{\"name\":\"name\",\"type\":\"string\"}]}}}]}";
    assertEquals(expectedSchema, schemas.get(0).getJSONObject("schema").toString());

  }

}
