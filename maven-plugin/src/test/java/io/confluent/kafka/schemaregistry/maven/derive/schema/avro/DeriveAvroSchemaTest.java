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

package io.confluent.kafka.schemaregistry.maven.derive.schema.avro;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import io.confluent.kafka.schemaregistry.avro.AvroSchema;
import io.confluent.kafka.schemaregistry.avro.AvroSchemaUtils;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.util.*;

import io.confluent.kafka.schemaregistry.client.MockSchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.schemaregistry.maven.derive.schema.NumericNodeComparator;
import io.confluent.kafka.schemaregistry.maven.derive.schema.avro.DeriveAvroSchema;
import io.confluent.kafka.serializers.*;
import io.confluent.kafka.schemaregistry.maven.derive.schema.utils.ReadFileUtils;
import org.apache.avro.generic.GenericRecord;
import org.junit.Test;

import static io.confluent.kafka.schemaregistry.maven.derive.schema.avro.DeriveAvroSchemaRecord.getSchemaForRecord;
import static org.junit.Assert.*;
import static io.confluent.kafka.schemaregistry.maven.derive.schema.DeriveSchema.mapper;

public class DeriveAvroSchemaTest {

  final DeriveAvroSchema strictAvroGenerator = new DeriveAvroSchema(true);
  final DeriveAvroSchema lenientAvroGenerator = new DeriveAvroSchema(false);
  private final KafkaAvroSerializer avroSerializer;
  private final KafkaAvroDeserializer avroDeserializer;
  private final String topic;

  boolean typeProtoBuf = false;

  public DeriveAvroSchemaTest() {

    Properties defaultConfig = new Properties();
    defaultConfig.put(KafkaAvroDeserializerConfig.SCHEMA_REGISTRY_URL_CONFIG, "bogus");
    SchemaRegistryClient schemaRegistry = new MockSchemaRegistryClient();
    avroSerializer = new KafkaAvroSerializer(schemaRegistry, new HashMap(defaultConfig));
    avroDeserializer = new KafkaAvroDeserializer(schemaRegistry);
    topic = "test";

  }

  void serializeAndDeserializeCheckMulti(List<String> messages, List<ObjectNode> schemas) throws IOException {

    for (ObjectNode schemaInfo : schemas) {

      AvroSchema schema = new AvroSchema(schemaInfo.get("schema").toString());
      ArrayNode matchingMessages = (ArrayNode) schemaInfo.get("messagesMatched");
      for (int i = 0; i < matchingMessages.size(); i++) {
        int index = matchingMessages.get(i).asInt();
        serializeAndDeserializeCheck(messages.get(index), schema);
      }

    }

  }

  void serializeAndDeserializeCheck(String message, AvroSchema schema) throws IOException {

    String formattedString = mapper.readTree(message).toString();
    Object test = AvroSchemaUtils.toObject(formattedString, schema);
    byte[] bytes = this.avroSerializer.serialize(this.topic, test);
    assertEquals(test, this.avroDeserializer.deserialize(this.topic, bytes, schema.rawSchema()));

    final ByteArrayOutputStream bs = new ByteArrayOutputStream();
    final String utf8 = StandardCharsets.UTF_8.name();
    try (PrintStream ps = new PrintStream(bs, true, utf8)) {
      AvroSchemaUtils.toJson(test, ps);
    }

    ObjectNode tree1 = (ObjectNode) mapper.readTree(formattedString);
    ObjectNode tree2 = (ObjectNode) mapper.readTree(bs.toString(utf8));

    NumericNodeComparator cmp = new NumericNodeComparator();
    assertTrue(tree1.equals(cmp, tree2));

  }

  @Test
  public void testPrimitiveTypes() throws IOException {

    /*
    Primitive data types test
    */
    String message =
        "{\n"
            + "    \"String\": \"John Smith\",\n"
            + "    \"LongName\": 1202021021034,\n"
            + "    \"Integer\": 9999239,\n"
            + "    \"Boolean\": false,\n"
            + "    \"Float\": 1e16,\n"
            + "    \"Double\": 62323232.78901245,\n"
            + "    \"Null\": null\n"
            + "  }";

    AvroSchema avroSchema = new AvroSchema(getSchemaForRecord((ObjectNode) mapper.readTree(message),
        "record", true, typeProtoBuf).toString());
    serializeAndDeserializeCheck(message, avroSchema);

    String expectedSchema = "{\"type\":\"record\",\"name\":\"record\",\"fields\":[{\"name\":\"Boolean\",\"type\":\"boolean\"},{\"name\":\"Double\",\"type\":\"double\"},{\"name\":\"Float\",\"type\":\"double\"},{\"name\":\"Integer\",\"type\":\"int\"},{\"name\":\"LongName\",\"type\":\"long\"},{\"name\":\"Null\",\"type\":\"null\"},{\"name\":\"String\",\"type\":\"string\"}]}";
    assertEquals(expectedSchema, avroSchema.toString());

    /*
    Lenient and Strict checking both give same schema
     */
    AvroSchema avroSchema2 = new AvroSchema(getSchemaForRecord((ObjectNode) mapper.readTree(message),
        "record", true, typeProtoBuf).toString());

    assertEquals(avroSchema, avroSchema2);
  }

  @Test
  public void testDoubleAndNum() throws IOException {

    /*
    Long and int can be mapped to double
    int can be mapped to long
    Both these mappings are done only if value is present in similar situation like [1, 1.5, 3]
    Here, field can be taken as array of double
     */

    String message =
        "{\n"
            + "    \"ArrayDouble\": [1e16, 11.5],\n"
            + "    \"ArrayInt\": [1, 11],\n"
            + "    \"ArrayDoubleInt\": [1, 11.322, 0.2222],\n"
            + "    \"ArrayLongInt\": [1, 212121212212121, 121212124324343432],\n"
            + "    \"ArrayLongInt2\": [2121212122312121, 2],\n"
            + "    \"ArrayLongIntDouble\": [0.5, 1, 2121212124212121, 121212124324343432],\n"
            + "    \"ArrayComb\": [{\"K\":10}, {\"K\":10.5}, {\"K\":10.43223}],\n"
            + "    \"ArrayComb2\": [{\"K\":[10]}, {\"K\":[10.5]}, {\"K\":[10.43223]}],\n"
            + "    \"ArrayComb3\": [{\"K\":[[10]]}, {\"K\":[[10.5]]}, {\"K\":[[10.43223]]}],\n"
            + "    \"ArrayComb4\": [{\"K\":[[{\"K\":10}]]}, {\"K\":[[{\"K\":0.5}]]}, {\"K\":[[{\"K\":1}]]}]\n"
            + "  }";

    AvroSchema avroSchema = new AvroSchema(getSchemaForRecord((ObjectNode) mapper.readTree(message),
        "record", true, typeProtoBuf).toString());

    serializeAndDeserializeCheck(message, avroSchema);
    String expectedSchema = "{\"type\":\"record\",\"name\":\"record\",\"fields\":[{\"name\":\"ArrayComb\",\"type\":{\"type\":\"array\",\"items\":{\"type\":\"record\",\"name\":\"ArrayComb\",\"fields\":[{\"name\":\"K\",\"type\":\"double\"}]}}},{\"name\":\"ArrayComb2\",\"type\":{\"type\":\"array\",\"items\":{\"type\":\"record\",\"name\":\"ArrayComb2\",\"fields\":[{\"name\":\"K\",\"type\":{\"type\":\"array\",\"items\":\"double\"}}]}}},{\"name\":\"ArrayComb3\",\"type\":{\"type\":\"array\",\"items\":{\"type\":\"record\",\"name\":\"ArrayComb3\",\"fields\":[{\"name\":\"K\",\"type\":{\"type\":\"array\",\"items\":{\"type\":\"array\",\"items\":\"double\"}}}]}}},{\"name\":\"ArrayComb4\",\"type\":{\"type\":\"array\",\"items\":{\"type\":\"record\",\"name\":\"ArrayComb4\",\"fields\":[{\"name\":\"K\",\"type\":{\"type\":\"array\",\"items\":{\"type\":\"array\",\"items\":{\"type\":\"record\",\"name\":\"K\",\"fields\":[{\"name\":\"K\",\"type\":\"double\"}]}}}}]}}},{\"name\":\"ArrayDouble\",\"type\":{\"type\":\"array\",\"items\":\"double\"}},{\"name\":\"ArrayDoubleInt\",\"type\":{\"type\":\"array\",\"items\":\"double\"}},{\"name\":\"ArrayInt\",\"type\":{\"type\":\"array\",\"items\":\"int\"}},{\"name\":\"ArrayLongInt\",\"type\":{\"type\":\"array\",\"items\":\"long\"}},{\"name\":\"ArrayLongInt2\",\"type\":{\"type\":\"array\",\"items\":\"long\"}},{\"name\":\"ArrayLongIntDouble\",\"type\":{\"type\":\"array\",\"items\":\"double\"}}]}";
    assertEquals(expectedSchema, avroSchema.toString());

    /*
    Lenient and Strict checking both give same schema
     */
    AvroSchema avroSchema2 = new AvroSchema(getSchemaForRecord((ObjectNode) mapper.readTree(message),
        "record", false, typeProtoBuf).toString());

    assertEquals(avroSchema, avroSchema2);

    String message2 = "[{\"A\" : 12}, {\"A\" : 12}, {\"B\" : 12.5}]";
    List<String> m = ReadFileUtils.readMessagesToString(message2);
    List<ObjectNode> schemas = strictAvroGenerator.getSchemaForMultipleMessages(m);
    serializeAndDeserializeCheckMulti(m, schemas);

  }


  @Test
  public void testStrictPrimitiveTypesErrors() throws IOException {

    /*
    Big Integer is not allowed for strict check, raises error
   */

    String bigIntMessage =
        "{\n"
            + "    \"BigDataType\": 1202021022234434333333444444444444444444443333\n"
            + "  }";

    assertThrows(IllegalArgumentException.class,
        () -> getSchemaForRecord((ObjectNode) mapper.readTree(bigIntMessage),
            "record", true, typeProtoBuf));

    AvroSchema avroSchema = new AvroSchema(getSchemaForRecord((ObjectNode) mapper.readTree(bigIntMessage),
        "record", false, typeProtoBuf).toString());

    Object test = AvroSchemaUtils.toObject(bigIntMessage, avroSchema);
    assert (test instanceof GenericRecord);
    GenericRecord ObjectNode = (GenericRecord) test;
    assertEquals(ObjectNode.get("BigDataType"), 1.2020210222344344E45);


    /*
    Big Decimal is coerced to double and value is less accurate
    Observation: Double allows up to 16 digits without loss
     */

    String bigDoubleMessage =
        "{\n"
            + "    \"BigDataType\": 62323232.789012456\n"
            + "  }";

    avroSchema = new AvroSchema(getSchemaForRecord((ObjectNode) mapper.readTree(bigDoubleMessage),
        "record", true, typeProtoBuf).toString());
    test = AvroSchemaUtils.toObject(bigDoubleMessage, avroSchema);
    assert (test instanceof GenericRecord);
    ObjectNode = (GenericRecord) test;
    assertEquals(ObjectNode.get("BigDataType"), 6.2323232789012454E7);
  }

  @Test
  public void testComplexTypesWithPrimitiveValues() throws IOException {

    /*
    Testing arrays and records with elements as primitive data types
    */

    String message =
        "{\n"
            + "    \"ArrayString\": [\"John Smith\", \"Tom Davies\"],\n"
            + "    \"ArrayInteger\": [12, 13, 14],\n"
            + "    \"ArrayNull\": [null, null, null],\n"
            + "    \"ArrayIntegersDoubles\": [12, 13, 14.3, 1211221212121],\n"
            + "    \"ArrayBoolean\": [false, true, false],\n"
            + "    \"DoubleRecord\": {\"Double1\": 62.4122121, \"Double2\": 62.4122121},\n"
            + "    \"IntRecord\": {\"Int1\": 62, \"Int2\": 12.1},\n"
            + "    \"MixedRecord\": {\"Int1\": 62, \"Double1\": 1.2212, \"name\" : \"Just Testing\"}\n"
            + "  }";

    AvroSchema avroSchema = new AvroSchema(getSchemaForRecord((ObjectNode) mapper.readTree(message),
        "record", true, typeProtoBuf).toString());
    serializeAndDeserializeCheck(message, avroSchema);

    /*
    Lenient and Strict checking both give same schema
    */

    AvroSchema avroSchema2 = new AvroSchema(getSchemaForRecord((ObjectNode) mapper.readTree(message),
        "record", true, typeProtoBuf).toString());

    assertEquals(avroSchema, avroSchema2);


    /*
    Array with different data types
    Highest occurring one is chosen
    */

    String message1 =
        "{\n"
            + "\"ArrayOfBoolean\": [0, 1, true, true, true, null],\n"
            + "\"ArrayOfInts\": [0, \"Java\", 10, 100, -12, 11221],\n"
            + "\"ArrayOfStrings\": [null, \"Java\", 10, 100, \"C++\", \"Scala\"]\n"
            + "  }";

    ObjectNode messageObject1 = (ObjectNode) mapper.readTree(message1);
    ObjectNode schema1 = getSchemaForRecord(messageObject1, "Record", false, typeProtoBuf);

    assert (schema1.get("fields") instanceof ArrayNode);
    ArrayNode fields = (ArrayNode) schema1.get("fields");

    assert (fields.get(0) instanceof ObjectNode);
    ObjectNode ArrayOfBoolean = (ObjectNode) fields.get(0);
    assertEquals(ArrayOfBoolean.get("type").toString(),
        "{\"type\":\"array\",\"items\":\"boolean\"}");

    assert (fields.get(1) instanceof ObjectNode);
    ObjectNode ArrayOfInts = (ObjectNode) fields.get(1);
    assertEquals(ArrayOfInts.get("type").toString(), "{\"type\":\"array\",\"items\":\"int\"}");

    assert (fields.get(2) instanceof ObjectNode);
    ObjectNode ArrayOfStrings = (ObjectNode) fields.get(2);
    assertEquals(ArrayOfStrings.get("type").toString(),
        "{\"type\":\"array\",\"items\":\"string\"}");

  }

  @Test
  public void testStrictDifferentDataTypes() throws IOException {

    /*
    Array with different data types should raise error
    */
    String message1 =
        "{\n"
            + "\"ArrayOfRecords\": [0, \"Java\"]\n"
            + "  }";

    ObjectNode messageObject1 = (ObjectNode) mapper.readTree(message1);
    assertThrows(IllegalArgumentException.class,
        () -> getSchemaForRecord(messageObject1, "Record", true, typeProtoBuf));

    /*
    Array with different data types should raise error
    */
    String message11 =
        "{\n"
            + "\"ArrayOfRecords\": [[12, true, false]]\n"
            + "  }";

    ObjectNode messageObject11 = (ObjectNode) mapper.readTree(message11);
    assertThrows(IllegalArgumentException.class,
        () -> getSchemaForRecord(messageObject11, "Record", true, typeProtoBuf));

    // Empty Record not allowed in Avro, ignoring this message
    String message1x = "{}";
    List<String> messageObject3x = ReadFileUtils.readMessagesToString(message1x);
    assertThrows(IllegalArgumentException.class,
        () -> strictAvroGenerator.getSchemaForMultipleMessages(messageObject3x));

    /*
    Array with no name
    */
    String message2 =
        "{\n"
            + "\"\": [0, \"Java\"]\n"
            + "  }";

    ObjectNode messageObject2 = (ObjectNode) mapper.readTree(message2);
    assertThrows(IllegalArgumentException.class,
        () -> getSchemaForRecord(messageObject2, "Record", true, typeProtoBuf));


    /*
    Record with naming errors
    */
    String message3 =
        "{\n"
            + "\"J\": {\"\":12},\n"
            + "\"097\": {\"L\":12}\n"
            + "  }";

    List<String> messages = ReadFileUtils.readMessagesToString(message3);
    assertThrows(IllegalArgumentException.class,
        () -> strictAvroGenerator.getSchemaForMultipleMessages(messages));


  }


  @Test
  public void testStrictComplexTypesRecursive() throws IOException {

    /*
    Different combinations of map and array are tested
   */
    String message =
        "{\n"
            + "    \"ArrayOfRecords\": [{\"Int1\": 62323232.78901245, \"Int2\": 12}, {\"Int1\": 6232323, \"Int2\": 2}],\n"
            + "    \"RecordOfArrays\": {\"ArrayInt1\": [12, 13,14], \"ArrayBoolean1\": [true, false]},\n"
            + "    \"RecordOfRecords\": {\"Record1\": {\"name\": \"Tom\", \"place\": \"Bom\"}, \"Record2\": {\"thing\": \"Kom\", \"place\": \"Bom\"}},\n"
            + "    \"Array3dX\": [[[1,2]], [[2,3,3,4]]],\n"
            + "    \"RecordOfArrays2\": { \"Array2D\": [ [1,3], [2,3,3, 3.5] ], \"Array3D\": [ [[1e4,2]], [[2,3,3,4]]] },\n"
            + "    \"RecordOfArrays3\": { \"Array2D1\": [ [{\"name\": \"J\"},{\"name\": \"K\"}], [{\"name\": \"T\"}] ]},\n"
            + "    \"RecordOfArrays4\": { \"Array2D2\": [ [{\"name\": 2},{\"name\": 2.5}], [{\"name\": 4}] ]},\n"
            + "    \"RecordOfArrays5\": {\"key\": {\"keys\":3}},\n"
            + "    \"RecordOfArrays6\": {\"New\": {\"keys\":\"4\"}, \"News\": {\"keys\":\"43\"}}\n"
            + "  }";

    ObjectNode ObjectNode = getSchemaForRecord((ObjectNode) mapper.readTree(message), "record", true, typeProtoBuf);
    AvroSchema avroSchema = new AvroSchema(ObjectNode.toString());

    serializeAndDeserializeCheck(message, avroSchema);


    /*
    Lenient and Strict checking both give same schema
    */

    AvroSchema avroSchema2 = new AvroSchema(getSchemaForRecord((ObjectNode) mapper.readTree(message),
        "record", false, typeProtoBuf).toString());

    assertEquals(avroSchema, avroSchema2);


    /*
    Array of records but records have different structure, picking highest occurring datatype
    */
    String message2 =
        "{\n"
            + "\"ArrayOfRecords1\": [{\"J\": true}, {\"J\": false}, {\"J\": 1}, {\"J\": false}],\n"
            + "\"ArrayOfRecords2\": [{\"Int1\": 10.1, \"Int2\":1.2}, {\"Int1\": -0.5, \"Int2\":1}, {\"Int1\": true, \"Int3\":1}],\n"
            + "\"ArrayOfRecords3\": [{\"Int1\": true, \"Int2\":false}, {\"Int1\": -0.5, \"Int2\":1}, {\"Int1\": 0.1, \"Int2\": -10}]\n"
            + "  }";

    ObjectNode messageObject2 = (ObjectNode) mapper.readTree(message2);
    ObjectNode schema2 = getSchemaForRecord(messageObject2, "Record", false, typeProtoBuf);

    assert (schema2.get("fields") instanceof ArrayNode);
    ArrayNode fields = (ArrayNode) schema2.get("fields");

    // {"J": boolean} is chosen as most occurring datatype
    assert (fields.get(0) instanceof ObjectNode);
    ObjectNode ArrayOfRecords1 = (ObjectNode) fields.get(0);
    assertEquals(ArrayOfRecords1.get("type").toString(), "{\"type\":\"array\",\"items\":{\"name\":\"ArrayOfRecords1\",\"type\":\"record\",\"fields\":[{\"name\":\"J\",\"type\":\"boolean\"}]}}");

    // {"Int1": double, "Int2": double} is chosen as most occurring datatype
    assert (fields.get(1) instanceof ObjectNode);
    ObjectNode ArrayOfRecords2 = (ObjectNode) fields.get(1);
    assertEquals(ArrayOfRecords2.get("type").toString(),
        "{\"type\":\"array\",\"items\":{\"name\":\"ArrayOfRecords2\",\"type\":\"record\",\"fields\":[{\"name\":\"Int1\",\"type\":\"double\"},{\"name\":\"Int2\",\"type\":\"double\"}]}}");

    // {"Int1": double, "Int2": int} is chosen as most occurring datatype
    assert (fields.get(2) instanceof ObjectNode);
    ObjectNode ArrayOfRecords3 = (ObjectNode) fields.get(2);
    assertEquals(ArrayOfRecords3.get("type").toString(),
        "{\"type\":\"array\",\"items\":{\"name\":\"ArrayOfRecords3\",\"type\":\"record\",\"fields\":[{\"name\":\"Int1\",\"type\":\"double\"},{\"name\":\"Int2\",\"type\":\"int\"}]}}");

  }

  @Test
  public void testLenientArrayRecursive() throws IOException {

    /*
    Highest datatype is chosen recursively
    Element 1 - array of integers
    Element 2 - array of strings
    Element 3 - array of strings
    Hence, schema chosen is array of strings
     */

    String message =
        "{\n"
            + "\"ArrayOfRecords1\": [{\"J\": [10,11,true]}, {\"J\": [10, \"first\", \"second\"]}, {\"J\": [\"first\", \"first\", 11]}]}\n"
            + "  }";

    ObjectNode messageObject = (ObjectNode) mapper.readTree(message);
    ObjectNode schema = getSchemaForRecord(messageObject, "Record", false, typeProtoBuf);

    assert (schema.get("fields") instanceof ArrayNode);
    ArrayNode fields = (ArrayNode) schema.get("fields");

    assert (fields.get(0) instanceof ObjectNode);
    ObjectNode ArrayOfRecords1 = (ObjectNode) fields.get(0);
    assertEquals(ArrayOfRecords1.get("type").toString(),
        "{\"type\":\"array\",\"items\":{\"name\":\"ArrayOfRecords1\",\"type\":\"record\",\"fields\":[{\"name\":\"J\",\"type\":{\"type\":\"array\",\"items\":\"string\"}}]}}");

  }

  @Test
  public void testStrictArrayOfRecordsError() throws IOException {

    /*
    Array of Records where structure is different, (name is different)
    Can be interpreted as union, branch has to specified correctly
    */
    String message =
        "{\n"
            + "\"ArrayOfRecords\": [{\"array\":[12]}, {\"long\": 12}]\n"
            + "  }";

    ObjectNode messageObject = (ObjectNode) mapper.readTree(message);
    ObjectNode t = getSchemaForRecord(messageObject, "Record", true, typeProtoBuf);
    assertEquals(t.toString(), "{\"name\":\"Record\",\"type\":\"record\",\"fields\":[{\"name\":\"ArrayOfRecords\",\"type\":{\"type\":\"array\",\"items\":[\"long\",{\"name\":\"array\",\"type\":\"array\",\"items\":\"int\"}]}}]}");
    serializeAndDeserializeCheck(message, new AvroSchema(t.toString()));

    /*
    Array of Records where structure is different, (name is different)
    Cannot be interpreted as union
    */
    String message1 =
        "{\n"
            + "\"ArrayOfRecords\": [{\"K\":12}, {\"J\": 12}]\n"
            + "  }";

    ObjectNode messageObject1 = (ObjectNode) mapper.readTree(message1);
    assertThrows(IllegalArgumentException.class,
        () -> getSchemaForRecord(messageObject1, "Record", true, typeProtoBuf));

    /*
    Array of Records where structure is different, (name is different)
    Cannot be interpreted as union
    */
    String message12 =
        "{\n"
            + "\"ArrayOfRecords\": [{\"long\":12}, {\"kong\": 12}]\n"
            + "  }";

    ObjectNode messageObject12 = (ObjectNode) mapper.readTree(message12);
    assertThrows(IllegalArgumentException.class,
        () -> getSchemaForRecord(messageObject12, "Record", true, typeProtoBuf));


    /*
    Array of Records where structure is different, same name but different datatype
    Should throw Error
     */
    String message2 =
        "{\n"
            + "\"ArrayOfRecords\": [{\"J\":[12]}, {\"J\":[true, false]}]\n"
            + "  }";

    ObjectNode messageObject2 = (ObjectNode) mapper.readTree(message2);
    assertThrows(IllegalArgumentException.class,
        () -> getSchemaForRecord(messageObject2, "Record", true, typeProtoBuf));


    /*
    Array of Records where structure is different, same name but different datatype, more complex test
    Should throw Error
     */
    String message3 =
        "{\n"
            + "\"ArrayOfRecords\": [{\"J\":[{\"K\":12}]}, {\"J\":[{\"K\":true}]}]\n"
            + "  }";

    ObjectNode messageObject3 = (ObjectNode) mapper.readTree(message3);
    assertThrows(IllegalArgumentException.class,
        () -> getSchemaForRecord(messageObject3, "Record", true, typeProtoBuf));

    /*
      Field is missing for object
      Treated as separate object and highest occurring is chosen
    */

    String message4 =
        "{\n"
            + "\"ArrayOfRecords\": [{\"J\":23, \"K\":23}, {\"J\": 33}]\n"
            + "  }";

    ObjectNode messageObject4 = (ObjectNode) mapper.readTree(message4);
    ObjectNode schema = getSchemaForRecord(messageObject4, "Record", false, typeProtoBuf);
    String expectedSchema = "{\"name\":\"Record\",\"type\":\"record\",\"fields\":[{\"name\":\"ArrayOfRecords\",\"type\":{\"type\":\"array\",\"items\":{\"name\":\"ArrayOfRecords\",\"type\":\"record\",\"fields\":[{\"name\":\"J\",\"type\":\"int\"},{\"name\":\"K\",\"type\":\"int\"}]}}}]}";
    assertEquals(schema.toString(), expectedSchema);

  }

  @Test
  public void testMultipleMessages() throws IOException {

    /*
    Multiple messages with exact same structure, returns 1 schema
    */

    ArrayList<String> arr = new ArrayList<>();

    for (int i = 0; i < 10; i++) {
      String message = String.format("{\n"
          + "    \"String\": \"%d\",\n"
          + "    \"Integer\": %d,\n"
          + "    \"Boolean\": %b\n"
          + "  }", i * 100, i, i % 2 == 0);
      arr.add(message);
    }
    List<ObjectNode> schemas1 = strictAvroGenerator.getSchemaForMultipleMessages(arr);
    assert (schemas1.size() == 1);

    String schemas1Expected1 = "{\"schema\":{\"name\":\"Record\",\"type\":\"record\",\"fields\":[{\"name\":\"Boolean\",\"type\":\"boolean\"},{\"name\":\"Integer\",\"type\":\"int\"},{\"name\":\"String\",\"type\":\"string\"}]},\"messagesMatched\":[0,1,2,3,4,5,6,7,8,9],\"numMessagesMatched\":10}";
    assertEquals(schemas1Expected1, schemas1.get(0).toString());
    serializeAndDeserializeCheckMulti(arr, schemas1);

    /*
    Multiple messages with different structure, top 3 most occurring schemas returned
    */

    ArrayList<String> arr2 = new ArrayList<>();
    for (int i = 0; i < 7; i++) {
      String message = String.format("{\n"
          + "    \"String\": \"%d\",\n"
          + "    \"Integer\": %d,\n"
          + "    \"Boolean\": %b,\n", i * 100, i, i % 2 == 0);

      if (i % 3 == 0) {
        String message2 = String.format("\"Long\": %d", i * 121202212);
        arr2.add(message + message2 + "}");
      } else if (i % 3 == 2) {
        String message2 = String.format("\"Bool2\": %b", false);
        arr2.add(message + message2 + "}");
      } else {
        arr2.add(message.substring(0, message.length() - 2) + "}");
      }
    }

    List<ObjectNode> schemas2 = strictAvroGenerator.getSchemaForMultipleMessages(arr2);
    assert (schemas2.size() == 3);

    String schemas2Expected1 = "{\"schema\":{\"name\":\"Record\",\"type\":\"record\",\"fields\":[{\"name\":\"Boolean\",\"type\":\"boolean\"},{\"name\":\"Integer\",\"type\":\"int\"},{\"name\":\"Long\",\"type\":\"int\"},{\"name\":\"String\",\"type\":\"string\"}]},\"messagesMatched\":[0,3,6],\"numMessagesMatched\":3}";
    assertEquals(schemas2.get(0).toString(), schemas2Expected1);

    String schemas2Expected2 = "{\"schema\":{\"name\":\"Record\",\"type\":\"record\",\"fields\":[{\"name\":\"Bool2\",\"type\":\"boolean\"},{\"name\":\"Boolean\",\"type\":\"boolean\"},{\"name\":\"Integer\",\"type\":\"int\"},{\"name\":\"String\",\"type\":\"string\"}]},\"messagesMatched\":[2,5],\"numMessagesMatched\":2}";
    assertEquals(schemas2.get(1).toString(), schemas2Expected2);

    String schemas2Expected3 = "{\"schema\":{\"name\":\"Record\",\"type\":\"record\",\"fields\":[{\"name\":\"Boolean\",\"type\":\"boolean\"},{\"name\":\"Integer\",\"type\":\"int\"},{\"name\":\"String\",\"type\":\"string\"}]},\"messagesMatched\":[1,4],\"numMessagesMatched\":2}";
    assertEquals(schemas2.get(2).toString(), schemas2Expected3);

    serializeAndDeserializeCheckMulti(arr2, schemas2);

    /*
    Multiple messages with different structure but lenient check returns 1 schema
    Treated as array of messages and highest occurring one is returned
    */

    List<ObjectNode> schemas3 = lenientAvroGenerator.getSchemaForMultipleMessages(arr2);
    assert (schemas3.size() == 1);

    String schema3Expected1 = "{\"schema\":{\"name\":\"Record\",\"type\":\"record\",\"fields\":[{\"name\":\"Boolean\",\"type\":\"boolean\"},{\"name\":\"Integer\",\"type\":\"int\"},{\"name\":\"Long\",\"type\":\"int\"},{\"name\":\"String\",\"type\":\"string\"}]}}";
    assertEquals(schemas3.get(0).toString(), schema3Expected1);

  }

  @Test
  public void testMultipleMessagesErrors() throws IOException {


    /*
    Strict schema cannot be found for message1 because of field Arr having multiple data types
    Hence, message1 is ignored in multiple messages
     */

    ArrayList<String> arr = new ArrayList<>();
    String message1 = "{\n"
        + "    \"String\": \"John Smith\","
        + "    \"Arr\": [1.5, true]"
        + "  }";
    arr.add(message1);

    // If no schemas can be generated error is raised
    assertThrows(IllegalArgumentException.class, () -> strictAvroGenerator.getSchemaForMultipleMessages(arr));

    String message2 = "{\n"
        + "    \"String\": \"John\",\n"
        + "    \"Float\": 1e16\n"
        + "  }";
    arr.add(message2);

    List<ObjectNode> schemas2 = strictAvroGenerator.getSchemaForMultipleMessages(arr);
    assert (schemas2.size() == 1);

    String schemas2Expected1 = "{\"schema\":{\"name\":\"Record\",\"type\":\"record\",\"fields\":[{\"name\":\"Float\",\"type\":\"double\"},{\"name\":\"String\",\"type\":\"string\"}]},\"messagesMatched\":[1],\"numMessagesMatched\":1}";
    assertEquals(schemas2.get(0).toString(), schemas2Expected1);

    serializeAndDeserializeCheckMulti(arr, schemas2);


    /*
    Lenient generator will pick the most occurring schema, here both occur only once so first element is picked
    */

    List<ObjectNode> schemas3 = lenientAvroGenerator.getSchemaForMultipleMessages(arr);
    assert (schemas3.size() == 1);

    String schemas3Expected1 = "{\"schema\":{\"name\":\"Record\",\"type\":\"record\",\"fields\":[{\"name\":\"Arr\",\"type\":{\"type\":\"array\",\"items\":\"double\"}},{\"name\":\"String\",\"type\":\"string\"}]}}";
    assertEquals(schemas3.get(0).toString(), schemas3Expected1);


    /*
    Order Checking Tests for Multiple Messages
    */


    String message31 = "    {\n" +
        "      \"name\" : \"J\",\n" +
        "      \"Age\" : 13,\n" +
        "        \"Date\":151109,\n" +
        "      \"arr\" : [12, 45, 56]\n" +
        "    }";

    String message32 = "    {\n" +
        "      \"arr\" : [4.5],\n" +
        "        \"Date\":151109,\n" +
        "      \"Age\" : 13,\n" +
        "      \"name\" : \"J\"\n" +
        "    }";

    String message33 = "    {\n" +
        "      \"Age\" : 13,\n" +
        "      \"name\" : \"J\",\n" +
        "      \"arr\" : [2121212],\n" +
        "        \"Date\":151109\n" +
        "    }";

    ArrayList<String> messages = new ArrayList<>(Arrays.asList(message31, message32, message33));
    List<ObjectNode> schemas4 = strictAvroGenerator.getSchemaForMultipleMessages(messages);
    assert (schemas4.size() == 1);

    String schemas4Expected1 = "{\"schema\":{\"name\":\"Record\",\"type\":\"record\",\"fields\":[{\"name\":\"Age\",\"type\":\"int\"},{\"name\":\"Date\",\"type\":\"int\"},{\"name\":\"arr\",\"type\":{\"type\":\"array\",\"items\":\"double\"}},{\"name\":\"name\",\"type\":\"string\"}]},\"messagesMatched\":[0,1,2],\"numMessagesMatched\":3}";
    assertEquals(schemas4.get(0).toString(), schemas4Expected1);

    serializeAndDeserializeCheckMulti(messages, schemas4);

    String message41 = "   { \"J\":{\n" +
        "      \"name\" : \"J\",\n" +
        "      \"Age\" : 13,\n" +
        "        \"Date\":151109,\n" +
        "      \"arr\" : [12, 45, 56]\n" +
        "    }}";

    String message42 = " {  \"J\" :{\n" +
        "      \"arr\" : [1.4],\n" +
        "        \"Date\":151109,\n" +
        "      \"Age\" : 13,\n" +
        "      \"name\" : \"J\"\n" +
        "    }}";

    String message43 = " {\"J\" :   {\n" +
        "      \"Age\" : 13,\n" +
        "      \"name\" : \"J\",\n" +
        "      \"arr\" : [12, 45, 56],\n" +
        "        \"Date\":151109\n" +
        "    }}";

    ArrayList<String> messages2 = new ArrayList<>(Arrays.asList(message41, message42, message43));
    List<ObjectNode> schemas5 = strictAvroGenerator.getSchemaForMultipleMessages(messages2);

    assert (schemas5.size() == 1);

    String schemas5Expected1 = "{\"schema\":{\"name\":\"Record\",\"type\":\"record\",\"fields\":[{\"name\":\"J\",\"type\":{\"name\":\"J\",\"fields\":[{\"name\":\"Age\",\"type\":\"int\"},{\"name\":\"Date\",\"type\":\"int\"},{\"name\":\"arr\",\"type\":{\"type\":\"array\",\"items\":\"double\"}},{\"name\":\"name\",\"type\":\"string\"}],\"type\":\"record\"}}]},\"messagesMatched\":[0,1,2],\"numMessagesMatched\":3}";
    assertEquals(schemas5.get(0).toString(), schemas5Expected1);

    serializeAndDeserializeCheckMulti(messages2, schemas5);

  }


}