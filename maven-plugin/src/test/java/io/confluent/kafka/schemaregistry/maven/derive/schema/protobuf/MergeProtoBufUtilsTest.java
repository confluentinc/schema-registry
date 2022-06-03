package io.confluent.kafka.schemaregistry.maven.derive.schema.protobuf;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import io.confluent.kafka.schemaregistry.maven.derive.schema.protobuf.MapAndArray;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static io.confluent.kafka.schemaregistry.maven.derive.schema.protobuf.MergeProtoBufUtils.*;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;

public class MergeProtoBufUtilsTest {

  private static final ObjectMapper mapper = new ObjectMapper();

  String schemaRecordOfRecords = "{\n" +
      "  \"__type\": \"record\",\n" +
      "  \"name\": \"mainMessage\",\n" +
      "  \"type\": \"record\",\n" +
      "  \"fields\": [\n" +
      "    {\n" +
      "      \"__type\": \"record\",\n" +
      "      \"name\": \"IntRecord\",\n" +
      "      \"type\": {\n" +
      "        \"__type\": \"record\",\n" +
      "        \"name\": \"IntRecord\",\n" +
      "        \"fields\": [\n" +
      "          {\n" +
      "            \"name\": \"%s\",\n" +
      "            \"type\": \"int32\"\n" +
      "          },\n" +
      "          {\n" +
      "            \"name\": \"%s\",\n" +
      "            \"type\": \"int32\"\n" +
      "          }\n" +
      "        ],\n" +
      "        \"type\": \"record\"\n" +
      "      }\n" +
      "    }\n" +
      "  ]\n" +
      "}";

  String schemaRecordOfRecordsNameChange = "{\n" +
      "  \"__type\": \"record\",\n" +
      "  \"name\": \"mainMessage\",\n" +
      "  \"type\": \"record\",\n" +
      "  \"fields\": [\n" +
      "    {\n" +
      "      \"__type\": \"record\",\n" +
      "      \"name\": \"DoubleRecord\",\n" +
      "      \"type\": {\n" +
      "        \"__type\": \"record\",\n" +
      "        \"name\": \"DoubleRecord\",\n" +
      "        \"fields\": [\n" +
      "          {\n" +
      "            \"name\": \"%s\",\n" +
      "            \"type\": \"int32\"\n" +
      "          },\n" +
      "          {\n" +
      "            \"name\": \"%s\",\n" +
      "            \"type\": \"int32\"\n" +
      "          }\n" +
      "        ],\n" +
      "        \"type\": \"record\"\n" +
      "      }\n" +
      "    }\n" +
      "  ]\n" +
      "}";

  String schemaRecordOfRecordsTypeChange = "{\n" +
      "  \"__type\": \"record\",\n" +
      "  \"name\": \"mainMessage\",\n" +
      "  \"type\": \"record\",\n" +
      "  \"fields\": [\n" +
      "    {\n" +
      "      \"__type\": \"record\",\n" +
      "      \"name\": \"IntRecord\",\n" +
      "      \"type\": {\n" +
      "        \"__type\": \"record\",\n" +
      "        \"name\": \"IntRecord\",\n" +
      "        \"fields\": [\n" +
      "          {\n" +
      "            \"name\": \"%s\",\n" +
      "            \"type\": \"string\"\n" +
      "          },\n" +
      "          {\n" +
      "            \"name\": \"%s\",\n" +
      "            \"type\": \"int32\"\n" +
      "          }\n" +
      "        ],\n" +
      "        \"type\": \"record\"\n" +
      "      }\n" +
      "    }\n" +
      "  ]\n" +
      "}";

  @Test
  public void shouldMakeNoChangeMergeRecords() throws JsonProcessingException {
    ObjectNode schema1 = (ObjectNode) mapper.readTree("{\"__type\":\"record\",\"name\":\"R1\",\"type\":\"record\",\"fields\":[{\"name\":\"K\",\"type\":\"int\"}]}");
    ObjectNode schema2 = (ObjectNode) mapper.readTree("{\"__type\":\"record\",\"name\":\"R2\",\"type\":\"record\",\"fields\":[{\"name\":\"K\",\"type\":\"int\"}]}");
    ArrayList<ObjectNode> schemas = new ArrayList<>(Arrays.asList(schema1, schema2));
    ObjectNode schemaStrict = mergeRecords(schemas, true, false);
    assert (schemaStrict.equals(schema1));

    ObjectNode schemaLenient = mergeRecords(schemas, false, false);
    assert (schemaLenient.equals(schema1));

  }

  @Test
  public void shouldCombineFieldsMergeRecords() throws JsonProcessingException {
    ObjectNode schema1 = (ObjectNode) mapper.readTree("{\"__type\":\"record\",\"name\":\"R1\",\"type\":\"record\",\"fields\":[{\"name\":\"K\",\"type\":\"int\"}]}");
    ObjectNode schema2 = (ObjectNode) mapper.readTree("{\"__type\":\"record\",\"name\":\"R2\",\"type\":\"record\",\"fields\":[{\"name\":\"J\",\"type\":\"int\"}]}");
    ArrayList<ObjectNode> schemas = new ArrayList<>(Arrays.asList(schema1, schema2));

    String expectedSchema = "{\"__type\":\"record\",\"name\":\"R1\",\"type\":\"record\",\"fields\":[{\"name\":\"J\",\"type\":\"int\"},{\"name\":\"K\",\"type\":\"int\"}]}";

    ObjectNode schemaStrict = mergeRecords(schemas, true, false);
    assertEquals(expectedSchema, schemaStrict.toString());

    ObjectNode schemaLenient = mergeRecords(schemas, false, false);
    assertEquals(expectedSchema, schemaLenient.toString());
  }

  @Test
  public void shouldCombineArrayFieldsMergeRecords() throws JsonProcessingException {

    ObjectNode schema1 = (ObjectNode) mapper.readTree("{\"__type\":\"record\",\"name\":\"A1\",\"type\":\"record\",\"fields\":[{\"__type\":\"array\",\"name\":\"arr\",\"type\":{\"__type\":\"array\",\"type\":\"array\",\"items\":\"double\"}}]}");
    ObjectNode schema2 = (ObjectNode) mapper.readTree("{\"__type\":\"record\",\"name\":\"A1\",\"type\":\"record\",\"fields\":[{\"__type\":\"array\",\"name\":\"arr1\",\"type\":{\"__type\":\"array\",\"type\":\"array\",\"items\":\"double\"}}]}");
    ArrayList<ObjectNode> schemas = new ArrayList<>(Arrays.asList(schema1, schema2));

    String expectedSchema = "{\"__type\":\"record\",\"name\":\"A1\",\"type\":\"record\",\"fields\":[{\"__type\":\"array\",\"name\":\"arr\",\"type\":{\"__type\":\"array\",\"type\":\"array\",\"items\":\"double\"}},{\"__type\":\"array\",\"name\":\"arr1\",\"type\":{\"__type\":\"array\",\"type\":\"array\",\"items\":\"double\"}}]}";
    ObjectNode schemaStrict = mergeRecords(schemas, true, false);
    assertEquals(expectedSchema, schemaStrict.toString());

    ObjectNode schemaLenient = mergeRecords(schemas, false, false);
    assertEquals(expectedSchema, schemaLenient.toString());
  }

  @Test
  public void SameArrayDifferentTypeMergeRecords() throws JsonProcessingException {

    ObjectNode schema1 = (ObjectNode) mapper.readTree("{\"__type\":\"record\",\"name\":\"A1\",\"type\":\"record\",\"fields\":[{\"__type\":\"array\",\"name\":\"arr\",\"type\":{\"__type\":\"array\",\"type\":\"array\",\"items\":\"double\"}}]}");
    ObjectNode schema2 = (ObjectNode) mapper.readTree("{\"__type\":\"record\",\"name\":\"A1\",\"type\":\"record\",\"fields\":[{\"__type\":\"array\",\"name\":\"arr\",\"type\":{\"__type\":\"array\",\"type\":\"array\",\"items\":\"string\"}}]}");
    ArrayList<ObjectNode> schemas = new ArrayList<>(Arrays.asList(schema1, schema1, schema2));

    assertThrows(IllegalArgumentException.class, () -> mergeRecords(schemas, true, false));

    // Picks schema1 type as most occurring
    ObjectNode schemaLenient = mergeRecords(schemas, false, false);
    assertEquals(schema1.toString(), schemaLenient.toString());
  }

  @Test
  public void SameFieldDifferentTypeMergeRecords() throws JsonProcessingException {
    ObjectNode schema1 = (ObjectNode) mapper.readTree("{\"__type\":\"record\",\"name\":\"R1\",\"type\":\"record\",\"fields\":[{\"name\":\"K\",\"type\":\"int\"}]}");
    ObjectNode schema2 = (ObjectNode) mapper.readTree("{\"__type\":\"record\",\"name\":\"R1\",\"type\":\"record\",\"fields\":[{\"name\":\"K\",\"type\":\"boolean\"}]}");

    // shouldRaiseError
    ArrayList<ObjectNode> schemas = new ArrayList<>(Arrays.asList(schema1, schema2, schema2));
    assertThrows(IllegalArgumentException.class, () -> mergeRecords(schemas, true, false));

    // Picks K's type as boolean
    ObjectNode schemaLenient = mergeRecords(schemas, false, false);
    assert (schemaLenient.equals(schema2));
  }


  @Test
  public void shouldMakeNoChangeTypeRecordMergeRecords() throws JsonProcessingException {
    ObjectNode schema1 = (ObjectNode) mapper.readTree(String.format(schemaRecordOfRecords, "J", "K"));
    ArrayList<ObjectNode> schemas = new ArrayList<>(Arrays.asList(schema1, schema1));
    ObjectNode schemaStrict = mergeRecords(schemas, true, false);
    assert (schemaStrict.equals(schema1));

    ObjectNode schemaLenient = mergeRecords(schemas, true, false);
    assert (schemaLenient.equals(schema1));
  }


  @Test
  public void shouldMergeFieldsInsideRecordTypeMergeRecords() throws JsonProcessingException {

    ObjectNode schema1 = (ObjectNode) mapper.readTree(String.format(schemaRecordOfRecords, "J", "P"));
    ObjectNode schema2 = (ObjectNode) mapper.readTree(String.format(schemaRecordOfRecords, "J", "K"));
    ArrayList<ObjectNode> schemas = new ArrayList<>(Arrays.asList(schema1, schema2));

    String expectedSchema = "{\"__type\":\"record\",\"name\":\"mainMessage\",\"type\":\"record\",\"fields\":[{\"__type\":\"record\",\"name\":\"IntRecord\",\"type\":{\"__type\":\"record\",\"name\":\"IntRecord\",\"type\":\"record\",\"fields\":[{\"name\":\"J\",\"type\":\"int32\"},{\"name\":\"K\",\"type\":\"int32\"},{\"name\":\"P\",\"type\":\"int32\"}]}}]}";

    ObjectNode schemaStrict = mergeRecords(schemas, true, false);
    assertEquals(expectedSchema, schemaStrict.toString());

    ObjectNode schemaLenient = mergeRecords(schemas, true, false);
    assertEquals(expectedSchema, schemaLenient.toString());

  }

  @Test
  public void shouldMakeNoChangeTryAndMergeStrict() throws JsonProcessingException {

    String schema1 = "{\"__type\":\"record\",\"name\":\"R1\",\"type\":\"record\",\"fields\":[{\"name\":\"K\",\"type\":\"int\"}]}";
    String schema2 = "{\"__type\":\"record\",\"name\":\"R1\",\"type\":\"record\",\"fields\":[{\"name\":\"K\",\"type\":\"int\"}]}";
    ArrayList<String> schemas = new ArrayList<>(Arrays.asList(schema1, schema2));
    MapAndArray mapAndArray = tryAndMergeStrict(schemas);

    assertEquals(mapAndArray.getSchemas().size(), 1);
    assertEquals(mapAndArray.getSchemas().get(0).toString(), schema1);

    assertEquals(mapAndArray.getSchemaToMessagesInfo().size(), 1);
    assertEquals(mapAndArray.getSchemaToMessagesInfo().get(0), new ArrayList<>(Arrays.asList(0, 1)));

  }

  @Test
  public void shouldMergeFieldsSchemasTryAndMergeStrict() throws JsonProcessingException {

    String schema1 = "{\"__type\":\"record\",\"name\":\"R1\",\"type\":\"record\",\"fields\":[{\"name\":\"K\",\"type\":\"int\"}]}";
    String schema2 = "{\"__type\":\"record\",\"name\":\"R1\",\"type\":\"record\",\"fields\":[{\"name\":\"J\",\"type\":\"int\"}]}";
    ArrayList<String> schemas = new ArrayList<>(Arrays.asList(schema1, schema2));
    MapAndArray mapAndArray = tryAndMergeStrict(schemas);

    String expectedSchema = "{\"__type\":\"record\",\"name\":\"R1\",\"type\":\"record\",\"fields\":[{\"name\":\"J\",\"type\":\"int\"},{\"name\":\"K\",\"type\":\"int\"}]}";
    assertEquals(mapAndArray.getSchemas().size(), 1);
    assertEquals(mapAndArray.getSchemas().get(0).toString(), expectedSchema);

    assertEquals(mapAndArray.getSchemaToMessagesInfo().size(), 1);
    assertEquals(mapAndArray.getSchemaToMessagesInfo().get(0), new ArrayList<>(Arrays.asList(0, 1)));

  }

  @Test
  public void shouldNotMergeFieldsSchemasTryAndMergeStrict() throws JsonProcessingException {

    String schema1 = "{\"__type\":\"record\",\"name\":\"R1\",\"type\":\"record\",\"fields\":[{\"name\":\"K\",\"type\":\"int\"}]}";
    String schema2 = "{\"__type\":\"record\",\"name\":\"R1\",\"type\":\"record\",\"fields\":[{\"name\":\"K\",\"type\":\"boolean\"}]}";
    ArrayList<String> schemas = new ArrayList<>(Arrays.asList(schema1, schema2));
    MapAndArray mapAndArray = tryAndMergeStrict(schemas);

    assertEquals(mapAndArray.getSchemas().size(), 2);
    assertEquals(mapAndArray.getSchemas().get(0).toString(), schema2);
    assertEquals(mapAndArray.getSchemas().get(1).toString(), schema1);

    assertEquals(mapAndArray.getSchemaToMessagesInfo().size(), 2);
    assertEquals(mapAndArray.getSchemaToMessagesInfo().get(0), new ArrayList<>(Collections.singletonList(1)));
    assertEquals(mapAndArray.getSchemaToMessagesInfo().get(1), new ArrayList<>(Collections.singletonList(0)));

  }

  @Test
  public void shouldMergeMultipleFieldsSchemasTryAndMergeStrict() throws JsonProcessingException {

    String schema1 = "{\"__type\":\"record\",\"name\":\"R1\",\"type\":\"record\",\"fields\":[{\"name\":\"K\",\"type\":\"int\"}]}";
    String schema2 = "{\"__type\":\"record\",\"name\":\"R1\",\"type\":\"record\",\"fields\":[{\"name\":\"K\",\"type\":\"boolean\"}]}";
    String schema3 = "{\"__type\":\"record\",\"name\":\"R1\",\"type\":\"record\",\"fields\":[{\"name\":\"J\",\"type\":\"boolean\"}]}";

    ArrayList<String> schemas = new ArrayList<>(Arrays.asList(schema1, schema2, schema3));
    MapAndArray mapAndArray = tryAndMergeStrict(schemas);

    assertEquals(mapAndArray.getSchemas().size(), 2);
    assertEquals(mapAndArray.getSchemaToMessagesInfo().size(), 2);

    // First and second schema match same number of messages, but schema1 is earlier in the input list and hence first in output list as well
    String expectedSchema1 = "{\"__type\":\"record\",\"name\":\"R1\",\"type\":\"record\",\"fields\":[{\"name\":\"J\",\"type\":\"boolean\"},{\"name\":\"K\",\"type\":\"int\"}]}";
    assertEquals(mapAndArray.getSchemas().get(0).toString(), expectedSchema1);
    assertEquals(mapAndArray.getSchemaToMessagesInfo().get(0), new ArrayList<>(Arrays.asList(0, 2)));

    String expectedSchema2 = "{\"__type\":\"record\",\"name\":\"R1\",\"type\":\"record\",\"fields\":[{\"name\":\"J\",\"type\":\"boolean\"},{\"name\":\"K\",\"type\":\"boolean\"}]}";
    assertEquals(mapAndArray.getSchemas().get(1).toString(), expectedSchema2);
    assertEquals(mapAndArray.getSchemaToMessagesInfo().get(1), new ArrayList<>(Arrays.asList(1, 2)));

    // Schema 3 can use both schemas generated, the optional field 'K' with different type is causing issues

  }

  @Test
  public void shouldMergeArrayFieldsSchemasTryAndMergeStrict() throws JsonProcessingException {

    String schema1 = "{\"__type\":\"record\",\"name\":\"R1\",\"type\":\"record\",\"fields\":[{\"name\":\"K\",\"type\":\"int\"}]}";
    String schema2 = "{\"__type\":\"record\",\"name\":\"R1\",\"type\":\"record\",\"fields\":[{\"name\":\"K\",\"type\":\"boolean\"}]}";
    String schema3 = "{\"__type\":\"record\",\"name\":\"R1\",\"type\":\"record\",\"fields\":[{\"name\":\"J\",\"type\":\"boolean\"}]}";

    ArrayList<String> schemas = new ArrayList<>(Arrays.asList(schema1, schema2, schema3));
    MapAndArray mapAndArray = tryAndMergeStrict(schemas);

    assertEquals(mapAndArray.getSchemas().size(), 2);
    assertEquals(mapAndArray.getSchemaToMessagesInfo().size(), 2);

    // First and second schema match same number of messages, but schema1 is earlier in the input list and hence first in output list as well
    String expectedSchema1 = "{\"__type\":\"record\",\"name\":\"R1\",\"type\":\"record\",\"fields\":[{\"name\":\"J\",\"type\":\"boolean\"},{\"name\":\"K\",\"type\":\"int\"}]}";
    assertEquals(mapAndArray.getSchemas().get(0).toString(), expectedSchema1);
    assertEquals(mapAndArray.getSchemaToMessagesInfo().get(0), new ArrayList<>(Arrays.asList(0, 2)));

    String expectedSchema2 = "{\"__type\":\"record\",\"name\":\"R1\",\"type\":\"record\",\"fields\":[{\"name\":\"J\",\"type\":\"boolean\"},{\"name\":\"K\",\"type\":\"boolean\"}]}";
    assertEquals(mapAndArray.getSchemas().get(1).toString(), expectedSchema2);
    assertEquals(mapAndArray.getSchemaToMessagesInfo().get(1), new ArrayList<>(Arrays.asList(1, 2)));

    // Schema 3 can use both schemas generated, the optional field 'K' with different type is causing issues

  }

  @Test
  public void shouldMergeMultipleFieldsSchemasSortedTryAndMergeStrict() throws JsonProcessingException {

    String schema1 = "{\"__type\":\"record\",\"name\":\"R1\",\"type\":\"record\",\"fields\":[{\"name\":\"K\",\"type\":\"int\"}]}";
    String schema2 = "{\"__type\":\"record\",\"name\":\"R1\",\"type\":\"record\",\"fields\":[{\"name\":\"K\",\"type\":\"boolean\"}]}";
    String schema3 = "{\"__type\":\"record\",\"name\":\"R1\",\"type\":\"record\",\"fields\":[{\"name\":\"J\",\"type\":\"boolean\"}]}";

    ArrayList<String> schemas = new ArrayList<>(Arrays.asList(schema1, schema2, schema2, schema3));
    MapAndArray mapAndArray = tryAndMergeStrict(schemas);

    assertEquals(mapAndArray.getSchemas().size(), 2);
    assertEquals(mapAndArray.getSchemaToMessagesInfo().size(), 2);

    // First in list because the number of schemas it matches is higher (schema2, schema2, schema3 whose indices as 1,2 and 3)
    String expectedSchema1 = "{\"__type\":\"record\",\"name\":\"R1\",\"type\":\"record\",\"fields\":[{\"name\":\"J\",\"type\":\"boolean\"},{\"name\":\"K\",\"type\":\"boolean\"}]}";
    assertEquals(mapAndArray.getSchemas().get(0).toString(), expectedSchema1);
    assertEquals(mapAndArray.getSchemaToMessagesInfo().get(0), new ArrayList<>(Arrays.asList(1, 2, 3)));

    // Second in list because the number of schemas it matches is lower (schema1 and schema3 whose indices as 0 and 3)
    String expectedSchema2 = "{\"__type\":\"record\",\"name\":\"R1\",\"type\":\"record\",\"fields\":[{\"name\":\"J\",\"type\":\"boolean\"},{\"name\":\"K\",\"type\":\"int\"}]}";
    assertEquals(mapAndArray.getSchemas().get(1).toString(), expectedSchema2);
    assertEquals(mapAndArray.getSchemaToMessagesInfo().get(1), new ArrayList<>(Arrays.asList(0, 3)));

    // Schema 3 can use both schemas generated, the optional field 'K' with different type is causing issues

  }

  @Test
  public void shouldMergeArraySchemasTryAndMergeStrict() throws JsonProcessingException {

    String schema1 = "{\"__type\":\"record\",\"name\":\"R1\",\"type\":\"record\",\"fields\":[{\"__type\":\"array\",\"name\":\"arr\",\"type\":{\"__type\":\"array\",\"type\":\"array\",\"items\":\"double\"}}]}";
    String schema2 = "{\"__type\":\"record\",\"name\":\"R1\",\"type\":\"record\",\"fields\":[{\"__type\":\"array\",\"name\":\"arr1\",\"type\":{\"__type\":\"array\",\"type\":\"array\",\"items\":\"double\"}}]}";
    ArrayList<String> schemas = new ArrayList<>(Arrays.asList(schema1, schema2));
    MapAndArray mapAndArray = tryAndMergeStrict(schemas);

    String expectedSchema = "{\"__type\":\"record\",\"name\":\"R1\",\"type\":\"record\",\"fields\":[{\"__type\":\"array\",\"name\":\"arr\",\"type\":{\"__type\":\"array\",\"type\":\"array\",\"items\":\"double\"}},{\"__type\":\"array\",\"name\":\"arr1\",\"type\":{\"__type\":\"array\",\"type\":\"array\",\"items\":\"double\"}}]}";
    assertEquals(mapAndArray.getSchemas().size(), 1);
    assertEquals(expectedSchema, mapAndArray.getSchemas().get(0).toString());

    assertEquals(mapAndArray.getSchemaToMessagesInfo().size(), 1);
    assertEquals(mapAndArray.getSchemaToMessagesInfo().get(0), new ArrayList<>(Arrays.asList(0, 1)));

  }


  @Test
  public void shouldNotMergeArraySchemasTryAndMergeStrict() throws JsonProcessingException {

    String schema1 = "{\"__type\":\"record\",\"name\":\"R1\",\"type\":\"record\",\"fields\":[{\"__type\":\"array\",\"name\":\"arr\",\"type\":{\"__type\":\"array\",\"type\":\"array\",\"items\":\"double\"}}]}";
    String schema2 = "{\"__type\":\"record\",\"name\":\"R1\",\"type\":\"record\",\"fields\":[{\"__type\":\"array\",\"name\":\"arr\",\"type\":{\"__type\":\"array\",\"type\":\"array\",\"items\":\"string\"}}]}";
    ArrayList<String> schemas = new ArrayList<>(Arrays.asList(schema1, schema2));
    MapAndArray mapAndArray = tryAndMergeStrict(schemas);

    assertEquals(mapAndArray.getSchemas().size(), 2);
    assertEquals(mapAndArray.getSchemas().get(0).toString(), schema2);
    assertEquals(mapAndArray.getSchemas().get(1).toString(), schema1);

    assertEquals(mapAndArray.getSchemaToMessagesInfo().size(), 2);
    assertEquals(mapAndArray.getSchemaToMessagesInfo().get(0), new ArrayList<>(Collections.singletonList(1)));
    assertEquals(mapAndArray.getSchemaToMessagesInfo().get(1), new ArrayList<>(Collections.singletonList(0)));

  }

  @Test
  public void shouldMergeMultipleArrayFieldsSchemasTryAndMergeStrict() throws JsonProcessingException {

    String schema1 = "{\"__type\":\"record\",\"name\":\"R1\",\"type\":\"record\",\"fields\":[{\"__type\":\"array\",\"name\":\"arr\",\"type\":{\"__type\":\"array\",\"type\":\"array\",\"items\":\"double\"}}]}";
    String schema2 = "{\"__type\":\"record\",\"name\":\"R1\",\"type\":\"record\",\"fields\":[{\"__type\":\"array\",\"name\":\"arr\",\"type\":{\"__type\":\"array\",\"type\":\"array\",\"items\":\"string\"}}]}";
    String schema3 = "{\"__type\":\"record\",\"name\":\"R1\",\"type\":\"record\",\"fields\":[{\"__type\":\"array\",\"name\":\"arr1\",\"type\":{\"__type\":\"array\",\"type\":\"array\",\"items\":\"boolean\"}}]}";

    ArrayList<String> schemas = new ArrayList<>(Arrays.asList(schema3, schema1, schema2, schema2));
    MapAndArray mapAndArray = tryAndMergeStrict(schemas);

    assertEquals(mapAndArray.getSchemas().size(), 2);
    assertEquals(mapAndArray.getSchemaToMessagesInfo().size(), 2);

    // First in list because the number of schemas it matches is higher (schema3, schema2, schema2 whose indices as 1,2 and 3)
    String expectedSchema1 = "{\"__type\":\"record\",\"name\":\"R1\",\"type\":\"record\",\"fields\":[{\"__type\":\"array\",\"name\":\"arr\",\"type\":{\"__type\":\"array\",\"type\":\"array\",\"items\":\"string\"}},{\"__type\":\"array\",\"name\":\"arr1\",\"type\":{\"__type\":\"array\",\"type\":\"array\",\"items\":\"boolean\"}}]}";
    assertEquals(mapAndArray.getSchemas().get(0).toString(), expectedSchema1);
    assertEquals(mapAndArray.getSchemaToMessagesInfo().get(0), new ArrayList<>(Arrays.asList(0, 2, 3)));

    // Second in list because the number of schemas it matches is lower (schema3 and schema1 whose indices as 0 and 1)
    String expectedSchema2 = "{\"__type\":\"record\",\"name\":\"R1\",\"type\":\"record\",\"fields\":[{\"__type\":\"array\",\"name\":\"arr\",\"type\":{\"__type\":\"array\",\"type\":\"array\",\"items\":\"double\"}},{\"__type\":\"array\",\"name\":\"arr1\",\"type\":{\"__type\":\"array\",\"type\":\"array\",\"items\":\"boolean\"}}]}";
    assertEquals(mapAndArray.getSchemas().get(1).toString(), expectedSchema2);
    assertEquals(mapAndArray.getSchemaToMessagesInfo().get(1), new ArrayList<>(Arrays.asList(0, 1)));

  }

  @Test
  public void shouldMergeRecordFieldsSchemasTryAndMergeStrict() throws JsonProcessingException {

    String schema1 = String.format(schemaRecordOfRecords, "J", "K");
    String schema2 = String.format(schemaRecordOfRecords, "J", "P");
    ArrayList<String> schemas = new ArrayList<>(Arrays.asList(schema1, schema2));
    MapAndArray mapAndArray = tryAndMergeStrict(schemas);

    String expectedSchema = "{\"__type\":\"record\",\"name\":\"mainMessage\",\"type\":\"record\",\"fields\":[{\"__type\":\"record\",\"name\":\"IntRecord\",\"type\":{\"__type\":\"record\",\"name\":\"IntRecord\",\"type\":\"record\",\"fields\":[{\"name\":\"J\",\"type\":\"int32\"},{\"name\":\"K\",\"type\":\"int32\"},{\"name\":\"P\",\"type\":\"int32\"}]}}]}";
    assertEquals(mapAndArray.getSchemas().size(), 1);
    assertEquals(mapAndArray.getSchemas().get(0).toString(), expectedSchema);

    assertEquals(mapAndArray.getSchemaToMessagesInfo().size(), 1);
    assertEquals(mapAndArray.getSchemaToMessagesInfo().get(0), new ArrayList<>(Arrays.asList(0, 1)));

  }


  @Test
  public void shouldNotMergeRecordsTryAndMergeStrict() throws JsonProcessingException {

    String schema1 = String.format(schemaRecordOfRecords, "J", "K");
    String schema2 = String.format(schemaRecordOfRecordsNameChange, "J", "K");
    ArrayList<String> schemas = new ArrayList<>(Arrays.asList(schema1, schema2));
    MapAndArray mapAndArray = tryAndMergeStrict(schemas);

    String expectedSchema = "{\"__type\":\"record\",\"name\":\"mainMessage\",\"type\":\"record\",\"fields\":[{\"__type\":\"record\",\"name\":\"DoubleRecord\",\"type\":{\"__type\":\"record\",\"name\":\"DoubleRecord\",\"fields\":[{\"name\":\"J\",\"type\":\"int32\"},{\"name\":\"K\",\"type\":\"int32\"}],\"type\":\"record\"}},{\"__type\":\"record\",\"name\":\"IntRecord\",\"type\":{\"__type\":\"record\",\"name\":\"IntRecord\",\"fields\":[{\"name\":\"J\",\"type\":\"int32\"},{\"name\":\"K\",\"type\":\"int32\"}],\"type\":\"record\"}}]}";
    assertEquals(mapAndArray.getSchemas().size(), 1);
    assertEquals(mapAndArray.getSchemas().get(0).toString(), expectedSchema);

    assertEquals(mapAndArray.getSchemaToMessagesInfo().size(), 1);
    assertEquals(mapAndArray.getSchemaToMessagesInfo().get(0), new ArrayList<>(Arrays.asList(0, 1)));

  }

  @Test
  public void shouldMergeMultipleRecordFieldsSchemasTryAndMergeStrict() throws JsonProcessingException {

    String schema1 = String.format(schemaRecordOfRecords, "J", "K");
    String schema2 = String.format(schemaRecordOfRecords, "J", "P");
    String schema3 = String.format(schemaRecordOfRecords, "L", "M");

    ArrayList<String> schemas = new ArrayList<>(Arrays.asList(schema3, schema2, schema1));
    MapAndArray mapAndArray = tryAndMergeStrict(schemas);

    assertEquals(mapAndArray.getSchemas().size(), 1);
    assertEquals(mapAndArray.getSchemaToMessagesInfo().size(), 1);

    String expectedSchema = "{\"__type\":\"record\",\"name\":\"mainMessage\",\"type\":\"record\",\"fields\":[{\"__type\":\"record\",\"name\":\"IntRecord\",\"type\":{\"__type\":\"record\",\"name\":\"IntRecord\",\"type\":\"record\",\"fields\":[{\"name\":\"J\",\"type\":\"int32\"},{\"name\":\"K\",\"type\":\"int32\"},{\"name\":\"L\",\"type\":\"int32\"},{\"name\":\"M\",\"type\":\"int32\"},{\"name\":\"P\",\"type\":\"int32\"}]}}]}";
    assertEquals(mapAndArray.getSchemas().get(0).toString(), expectedSchema);

    assertEquals(mapAndArray.getSchemaToMessagesInfo().get(0), new ArrayList<>(Arrays.asList(0, 1, 2)));

  }

  @Test
  public void shouldMergeRecordAndRecordFieldsSchemasTryAndMergeStrict() throws JsonProcessingException {

    String schema1 = String.format(schemaRecordOfRecords, "J", "K");
    String schema2 = String.format(schemaRecordOfRecordsTypeChange, "J", "P");
    String schema3 = String.format(schemaRecordOfRecords, "L", "M");

    ArrayList<String> schemas = new ArrayList<>(Arrays.asList(schema3, schema2, schema2, schema1));
    MapAndArray mapAndArray = tryAndMergeStrict(schemas);

    assertEquals(mapAndArray.getSchemas().size(), 2);
    assertEquals(mapAndArray.getSchemaToMessagesInfo().size(), 2);

    // First in list because the number of schemas it matches is higher (schema3, schema2, schema2 whose indices as 0,1 and 2)
    String expectedSchema1 = "{\"__type\":\"record\",\"name\":\"mainMessage\",\"type\":\"record\",\"fields\":[{\"__type\":\"record\",\"name\":\"IntRecord\",\"type\":{\"__type\":\"record\",\"name\":\"IntRecord\",\"type\":\"record\",\"fields\":[{\"name\":\"J\",\"type\":\"string\"},{\"name\":\"L\",\"type\":\"int32\"},{\"name\":\"M\",\"type\":\"int32\"},{\"name\":\"P\",\"type\":\"int32\"}]}}]}";
    assertEquals(mapAndArray.getSchemas().get(0).toString(), expectedSchema1);
    assertEquals(mapAndArray.getSchemaToMessagesInfo().get(0), new ArrayList<>(Arrays.asList(0, 1, 2)));

    // Second in list because the number of schemas it matches is lower (schema3 and schema1 whose indices as 0 and 3)
    String expectedSchema2 = "{\"__type\":\"record\",\"name\":\"mainMessage\",\"type\":\"record\",\"fields\":[{\"__type\":\"record\",\"name\":\"IntRecord\",\"type\":{\"__type\":\"record\",\"name\":\"IntRecord\",\"type\":\"record\",\"fields\":[{\"name\":\"J\",\"type\":\"int32\"},{\"name\":\"K\",\"type\":\"int32\"},{\"name\":\"L\",\"type\":\"int32\"},{\"name\":\"M\",\"type\":\"int32\"}]}}]}";
    assertEquals(mapAndArray.getSchemas().get(1).toString(), expectedSchema2);
    assertEquals(mapAndArray.getSchemaToMessagesInfo().get(1), new ArrayList<>(Arrays.asList(0, 3)));

  }

  @Test
  public void shouldGenerateSchemaToMessageInfoPreviousInfoNull() throws JsonProcessingException {

    ObjectNode schema1 = (ObjectNode) mapper.readTree("{\"__type\":\"record\",\"name\":\"R1\",\"type\":\"record\",\"fields\":[{\"name\":\"K\",\"type\":\"int\"}]}");
    ObjectNode schema2 = (ObjectNode) mapper.readTree("{\"__type\":\"record\",\"name\":\"R1\",\"type\":\"record\",\"fields\":[{\"name\":\"K\",\"type\":\"boolean\"}]}");

    ArrayList<ObjectNode> schemaList = new ArrayList<>(Arrays.asList(schema1, schema1, schema2, schema2, schema1, schema1));
    ArrayList<ObjectNode> uniqueList = new ArrayList<>(Arrays.asList(schema1, schema2));
    List<List<Integer>> info = getUniqueWithMessageInfo(schemaList, uniqueList, null);

    assertEquals(info.size(), 2);
    assertEquals(info.get(0), new ArrayList<>(Arrays.asList(0, 1, 4, 5)));
    assertEquals(info.get(1), new ArrayList<>(Arrays.asList(2, 3)));

  }

  @Test
  public void shouldGenerateSchemaToMessageInfoPreviousInfoSupplied() throws JsonProcessingException {

    ObjectNode schema1 = (ObjectNode) mapper.readTree("{\"__type\":\"record\",\"name\":\"R1\",\"type\":\"record\",\"fields\":[{\"name\":\"K\",\"type\":\"int\"}]}");
    ObjectNode schema2 = (ObjectNode) mapper.readTree("{\"__type\":\"record\",\"name\":\"R1\",\"type\":\"record\",\"fields\":[{\"name\":\"K\",\"type\":\"boolean\"}]}");

    ArrayList<ObjectNode> schemaList = new ArrayList<>(Arrays.asList(schema1, schema1, schema2, schema2, schema1, schema1));
    ArrayList<ObjectNode> uniqueList = new ArrayList<>(Arrays.asList(schema1, schema2));

    List<List<Integer>> schemaToMessages = new ArrayList<>();
    schemaToMessages.add(new ArrayList<>(Arrays.asList(0, 1, 2)));
    schemaToMessages.add(new ArrayList<>(Arrays.asList(2, 3)));
    schemaToMessages.add(new ArrayList<>(Arrays.asList(4, 5)));
    schemaToMessages.add(new ArrayList<>(Arrays.asList(5, 6)));
    schemaToMessages.add(new ArrayList<>(Arrays.asList(4, 3)));
    schemaToMessages.add(new ArrayList<>(Arrays.asList(7, 3)));

    List<List<Integer>> info = getUniqueWithMessageInfo(schemaList, uniqueList, schemaToMessages);
    assertEquals(info.size(), 2);
    assertEquals(info.get(0), new ArrayList<>(Arrays.asList(0, 1, 2, 3, 4, 7)));
    assertEquals(info.get(1), new ArrayList<>(Arrays.asList(4, 5, 6)));

  }




}