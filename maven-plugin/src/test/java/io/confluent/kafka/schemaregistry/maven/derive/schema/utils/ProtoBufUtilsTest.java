package io.confluent.kafka.schemaregistry.maven.derive.schema.utils;

import com.fasterxml.jackson.core.JsonProcessingException;
import org.json.JSONObject;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static io.confluent.kafka.schemaregistry.maven.derive.schema.utils.ProtoBufUtils.*;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;

public class ProtoBufUtilsTest {

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
    JSONObject schema1 = new JSONObject("{\"__type\":\"record\",\"name\":\"R1\",\"type\":\"record\",\"fields\":[{\"name\":\"K\",\"type\":\"int\"}]}");
    JSONObject schema2 = new JSONObject("{\"__type\":\"record\",\"name\":\"R2\",\"type\":\"record\",\"fields\":[{\"name\":\"K\",\"type\":\"int\"}]}");
    ArrayList<JSONObject> schemas = new ArrayList<>(Arrays.asList(schema1, schema2));
    JSONObject schemaStrict = mergeRecords(schemas, true, false);
    assert (schemaStrict.similar(schema1));

    JSONObject schemaLenient = mergeRecords(schemas, false, false);
    assert (schemaLenient.similar(schema1));

  }

  @Test
  public void shouldCombineFieldsMergeRecords() throws JsonProcessingException {
    JSONObject schema1 = new JSONObject("{\"__type\":\"record\",\"name\":\"R1\",\"type\":\"record\",\"fields\":[{\"name\":\"K\",\"type\":\"int\"}]}");
    JSONObject schema2 = new JSONObject("{\"__type\":\"record\",\"name\":\"R2\",\"type\":\"record\",\"fields\":[{\"name\":\"J\",\"type\":\"int\"}]}");
    ArrayList<JSONObject> schemas = new ArrayList<>(Arrays.asList(schema1, schema2));

    String expectedSchema = "{\"__type\":\"record\",\"name\":\"R1\",\"type\":\"record\",\"fields\":[{\"name\":\"J\",\"type\":\"int\"},{\"name\":\"K\",\"type\":\"int\"}]}";

    JSONObject schemaStrict = mergeRecords(schemas, true, false);
    assertEquals(expectedSchema, schemaStrict.toString());

    JSONObject schemaLenient = mergeRecords(schemas, false, false);
    assertEquals(expectedSchema, schemaLenient.toString());
  }

  @Test
  public void shouldCombineArrayFieldsMergeRecords() throws JsonProcessingException {

    JSONObject schema1 = new JSONObject("{\"__type\":\"record\",\"name\":\"A1\",\"type\":\"record\",\"fields\":[{\"__type\":\"array\",\"name\":\"arr\",\"type\":{\"__type\":\"array\",\"type\":\"array\",\"items\":\"double\"}}]}");
    JSONObject schema2 = new JSONObject("{\"__type\":\"record\",\"name\":\"A1\",\"type\":\"record\",\"fields\":[{\"__type\":\"array\",\"name\":\"arr1\",\"type\":{\"__type\":\"array\",\"type\":\"array\",\"items\":\"double\"}}]}");
    ArrayList<JSONObject> schemas = new ArrayList<>(Arrays.asList(schema1, schema2));

    String expectedSchema = "{\"__type\":\"record\",\"name\":\"A1\",\"type\":\"record\",\"fields\":[{\"__type\":\"array\",\"name\":\"arr\",\"type\":{\"__type\":\"array\",\"type\":\"array\",\"items\":\"double\"}},{\"__type\":\"array\",\"name\":\"arr1\",\"type\":{\"__type\":\"array\",\"type\":\"array\",\"items\":\"double\"}}]}";
    JSONObject schemaStrict = mergeRecords(schemas, true, false);
    assertEquals(expectedSchema, schemaStrict.toString());

    JSONObject schemaLenient = mergeRecords(schemas, false, false);
    assertEquals(expectedSchema, schemaLenient.toString());
  }

  @Test
  public void SameArrayDifferentTypeMergeRecords() throws JsonProcessingException {

    JSONObject schema1 = new JSONObject("{\"__type\":\"record\",\"name\":\"A1\",\"type\":\"record\",\"fields\":[{\"__type\":\"array\",\"name\":\"arr\",\"type\":{\"__type\":\"array\",\"type\":\"array\",\"items\":\"double\"}}]}");
    JSONObject schema2 = new JSONObject("{\"__type\":\"record\",\"name\":\"A1\",\"type\":\"record\",\"fields\":[{\"__type\":\"array\",\"name\":\"arr\",\"type\":{\"__type\":\"array\",\"type\":\"array\",\"items\":\"string\"}}]}");
    ArrayList<JSONObject> schemas = new ArrayList<>(Arrays.asList(schema1, schema1, schema2));

    assertThrows(IllegalArgumentException.class, () -> mergeRecords(schemas, true, false));

    // Picks schema1 type as most occurring
    JSONObject schemaLenient = mergeRecords(schemas, false, false);
    assertEquals(schema1.toString(), schemaLenient.toString());
  }

  @Test
  public void SameFieldDifferentTypeMergeRecords() throws JsonProcessingException {
    JSONObject schema1 = new JSONObject("{\"__type\":\"record\",\"name\":\"R1\",\"type\":\"record\",\"fields\":[{\"name\":\"K\",\"type\":\"int\"}]}");
    JSONObject schema2 = new JSONObject("{\"__type\":\"record\",\"name\":\"R1\",\"type\":\"record\",\"fields\":[{\"name\":\"K\",\"type\":\"boolean\"}]}");

    // shouldRaiseError
    ArrayList<JSONObject> schemas = new ArrayList<>(Arrays.asList(schema1, schema2, schema2));
    assertThrows(IllegalArgumentException.class, () -> mergeRecords(schemas, true, false));

    // Picks K's type as boolean
    JSONObject schemaLenient = mergeRecords(schemas, false, false);
    assert (schemaLenient.similar(schema2));
  }


  @Test
  public void shouldMakeNoChangeTypeRecordMergeRecords() throws JsonProcessingException {
    JSONObject schema1 = new JSONObject(String.format(schemaRecordOfRecords, "J", "K"));
    ArrayList<JSONObject> schemas = new ArrayList<>(Arrays.asList(schema1, schema1));
    JSONObject schemaStrict = mergeRecords(schemas, true, false);
    assert (schemaStrict.similar(schema1));

    JSONObject schemaLenient = mergeRecords(schemas, true, false);
    assert (schemaLenient.similar(schema1));
  }


  @Test
  public void shouldMergeFieldsInsideRecordTypeMergeRecords() throws JsonProcessingException {

    JSONObject schema1 = new JSONObject(String.format(schemaRecordOfRecords, "J", "P"));
    JSONObject schema2 = new JSONObject(String.format(schemaRecordOfRecords, "J", "K"));
    ArrayList<JSONObject> schemas = new ArrayList<>(Arrays.asList(schema1, schema2));

    String expectedSchema = "{\"__type\":\"record\",\"name\":\"mainMessage\",\"type\":\"record\",\"fields\":[{\"__type\":\"record\",\"name\":\"IntRecord\",\"type\":{\"__type\":\"record\",\"name\":\"IntRecord\",\"type\":\"record\",\"fields\":[{\"name\":\"J\",\"type\":\"int32\"},{\"name\":\"K\",\"type\":\"int32\"},{\"name\":\"P\",\"type\":\"int32\"}]}}]}";

    JSONObject schemaStrict = mergeRecords(schemas, true, false);
    assertEquals(expectedSchema, schemaStrict.toString());

    JSONObject schemaLenient = mergeRecords(schemas, true, false);
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
    assertEquals(mapAndArray.getSchemas().get(0).toString(), schema1);
    assertEquals(mapAndArray.getSchemas().get(1).toString(), schema2);

    assertEquals(mapAndArray.getSchemaToMessagesInfo().size(), 2);
    assertEquals(mapAndArray.getSchemaToMessagesInfo().get(0), new ArrayList<>(Collections.singletonList(0)));
    assertEquals(mapAndArray.getSchemaToMessagesInfo().get(1), new ArrayList<>(Collections.singletonList(1)));

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
    assertEquals(mapAndArray.getSchemas().get(0).toString(), expectedSchema);

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
    assertEquals(mapAndArray.getSchemas().get(0).toString(), schema1);
    assertEquals(mapAndArray.getSchemas().get(1).toString(), schema2);

    assertEquals(mapAndArray.getSchemaToMessagesInfo().size(), 2);
    assertEquals(mapAndArray.getSchemaToMessagesInfo().get(0), new ArrayList<>(Collections.singletonList(0)));
    assertEquals(mapAndArray.getSchemaToMessagesInfo().get(1), new ArrayList<>(Collections.singletonList(1)));

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
  public void shouldGenerateSchemaToMessageInfoPreviousInfoNull() {

    JSONObject schema1 = new JSONObject("{\"__type\":\"record\",\"name\":\"R1\",\"type\":\"record\",\"fields\":[{\"name\":\"K\",\"type\":\"int\"}]}");
    JSONObject schema2 = new JSONObject("{\"__type\":\"record\",\"name\":\"R1\",\"type\":\"record\",\"fields\":[{\"name\":\"K\",\"type\":\"boolean\"}]}");

    ArrayList<JSONObject> schemaList = new ArrayList<>(Arrays.asList(schema1, schema1, schema2, schema2, schema1, schema1));
    ArrayList<JSONObject> uniqueList = new ArrayList<>(Arrays.asList(schema1, schema2));
    List<List<Integer>> info = getUniqueWithMessageInfo(schemaList, uniqueList, null);

    assertEquals(info.size(), 2);
    assertEquals(info.get(0), new ArrayList<>(Arrays.asList(0, 1, 4, 5)));
    assertEquals(info.get(1), new ArrayList<>(Arrays.asList(2, 3)));

  }

  @Test
  public void shouldGenerateSchemaToMessageInfoPreviousInfoSupplied() {

    JSONObject schema1 = new JSONObject("{\"__type\":\"record\",\"name\":\"R1\",\"type\":\"record\",\"fields\":[{\"name\":\"K\",\"type\":\"int\"}]}");
    JSONObject schema2 = new JSONObject("{\"__type\":\"record\",\"name\":\"R1\",\"type\":\"record\",\"fields\":[{\"name\":\"K\",\"type\":\"boolean\"}]}");

    ArrayList<JSONObject> schemaList = new ArrayList<>(Arrays.asList(schema1, schema1, schema2, schema2, schema1, schema1));
    ArrayList<JSONObject> uniqueList = new ArrayList<>(Arrays.asList(schema1, schema2));

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