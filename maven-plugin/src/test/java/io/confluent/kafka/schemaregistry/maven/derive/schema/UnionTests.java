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

import com.google.gson.JsonElement;
import com.google.gson.JsonParser;
import io.confluent.kafka.schemaregistry.avro.AvroSchema;
import io.confluent.kafka.schemaregistry.avro.AvroSchemaUtils;
import io.confluent.kafka.schemaregistry.client.MockSchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import io.confluent.kafka.serializers.KafkaAvroDeserializerConfig;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import io.confluent.kafka.schemaregistry.maven.derive.schema.utils.ReadFileUtils;
import org.json.JSONArray;
import org.json.JSONObject;
import org.junit.Test;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.PrintStream;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Properties;

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
    Merging branches of the following types in each test:
    Simple1 - Primitive Types and Array
    Simple2 - Primitive Types and Null
    Simple3 - Primitive Types, array and records of different types
    */

    for (int i = 3; i <= 3; i++) {
      File file = new File(String.format("src/test/resources/Avro/UnionTestData/wikiSimple%d.json", i));
      List<String> messages = ReadFileUtils.readMessagesToString(file);
      List<JSONObject> schemas = schemaGenerator.getSchemaForMultipleMessages(messages);
      serializeAndDeserializeCheckMulti(messages, schemas);
    }

  }

  @Test
  public void RecursiveTestUnions() throws IOException {

    /*
    recursiveTest - Combination of null and object, null and int and merging of null and record which has
    a field of type Union

    recursiveTest2 - Combining multiple records and null

    */

//    for (String str : Arrays.asList("recursiveTest", "recursiveTest2")) {
    for (String str : Arrays.asList("recursiveTest2")) {
      File file = new File(String.format("src/test/resources/Avro/UnionTestData/%s.json", str));
      List<String> messages = ReadFileUtils.readMessagesToString(file);
      List<JSONObject> schemas = schemaGenerator.getSchemaForMultipleMessages(messages);
      serializeAndDeserializeCheckMulti(messages, schemas);
    }

  }

  @Test
  public void TestUnionsLenient() throws IOException {

    /*
    Case when first message doesn't have all fields,
    Second and Third has union and can be merged, this is the most occurring type
    *

     */
    File file = new File("src/test/resources/Avro/UnionTestData/wikiLenientCheck.json");
    List<String> messages = ReadFileUtils.readMessagesToString(file);
    List<JSONObject> schemas = schemaGenerator.getSchemaForMultipleMessages(messages);
    serializeAndDeserializeCheckMulti(messages, schemas);

  }

  @Test
  public void TestArrayOfUnions() throws IOException {

    /*
    Case when first message doesn't have all fields,
    Second and Third has union and can be merged, this is the most occurring type
    *

     */
    String message = "[{ \"A\" : [{\"string\":\"12\"}, {\"int\":12}]}, { \"A\" : [{\"double\":12.5}]}]";
    List<String> messages = ReadFileUtils.readMessagesToString(message);
    List<JSONObject> schemas = schemaGenerator.getSchemaForMultipleMessages(messages);
    serializeAndDeserializeCheckMulti(messages, schemas);

  }

  @Test
  public void TestingReadFolder() throws IOException {

    File file = new File("src/test/resources/Avro/UnionTestData/arrayOfUnions.txt");
    List<String> messages = ReadFileUtils.readMessagesToString(file);
    List<JSONObject> schemas = schemaGenerator.getSchemaForMultipleMessages(messages);
    serializeAndDeserializeCheckMulti(messages, schemas);

  }

}
