package io.confluent.kafka.schemaregistry.maven.derive.schema;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

import static io.confluent.kafka.schemaregistry.maven.derive.schema.DeriveJsonSchemaTest.*;
import static org.junit.Assert.assertEquals;

public class DeriveSchemaUtilsTest {

  private static final ObjectMapper mapper = new ObjectMapper();
  static final String TYPE_INT = "{\"type\":\"int\"}";
  static final String TYPE_LONG = "{\"type\":\"long\"}";
  static final String TYPE_INT_32 = "{\"type\":\"int32\"}";
  static final String TYPE_INT_64 = "{\"type\":\"int64\"}";
  static final String TYPE_DOUBLE = "{\"type\":\"double\"}";

  private ObjectNode getObjectNodeForSorting() {
    ObjectNode node = mapper.createObjectNode();
    node.put("Second", "1");
    node.put("First", "1");
    node.put("Third", "1");
    return node;
  }

  private void checkOrderOfKeys(List<String> sortedKeys) {
    assertEquals(sortedKeys.size(), 3);
    assertEquals(sortedKeys.get(0), "First");
    assertEquals(sortedKeys.get(1), "Second");
    assertEquals(sortedKeys.get(2), "Third");
  }

  @Test
  public void shouldGetDifferentUniqueElements() throws JsonProcessingException {
    List<JsonNode> schemas = new ArrayList<>();
    for (String schema : Arrays.asList(ARRAY_OF_NUMBERS, ARRAY_OF_ARRAY_OF_NUMBERS, RECORD_WITH_STRING, RECORD_WITH_ARRAY_OF_STRINGS)) {
      schemas.add(mapper.readTree(schema));
    }
    List<JsonNode> uniqueSchemas = DeriveSchemaUtils.getUnique(schemas);
    assertEquals(uniqueSchemas.size(), 4);
    for (JsonNode schema : schemas) {
      assert (uniqueSchemas.contains(schema));
    }
  }

  @Test
  public void testGetListFromArray() throws JsonProcessingException {
    ArrayNode arrayNode = mapper.createArrayNode();
    arrayNode.add(mapper.readTree(TYPE_LONG));
    arrayNode.add(mapper.readTree(DeriveJsonSchemaTest.EMPTY_ARRAY));
    List<JsonNode> objectList = DeriveSchemaUtils.getListFromArray(arrayNode);
    assertEquals(objectList.size(), 2);
    assertEquals(objectList.get(0), mapper.readTree(TYPE_LONG));
    assertEquals(objectList.get(1), mapper.readTree(DeriveJsonSchemaTest.EMPTY_ARRAY));
  }

  @Test
  public void testGetSortedKeys() {
    List<String> sortedKeys = DeriveSchemaUtils.getSortedKeys(getObjectNodeForSorting());
    checkOrderOfKeys(sortedKeys);
  }

  @Test
  public void testSortObjectNode() {
    ObjectNode sortedObject = DeriveSchemaUtils.sortObjectNode(getObjectNodeForSorting());
    List<String> sortedKeys = new ArrayList<>();
    for (Iterator<String> it = sortedObject.fieldNames(); it.hasNext(); ) {
      sortedKeys.add(it.next());
    }
    checkOrderOfKeys(sortedKeys);
  }

  @Test
  public void shouldMergeNumberTypes() throws JsonProcessingException {
    List<ObjectNode> schemas = new ArrayList<>();
    for (String schema : Arrays.asList(TYPE_INT, TYPE_LONG, TYPE_INT_32, TYPE_INT_64, TYPE_DOUBLE)) {
      schemas.add(mapper.readValue(schema, ObjectNode.class));
    }
    DeriveSchemaUtils.mergeNumberTypes(schemas);
    for (ObjectNode schema : schemas) {
      assertEquals(schema, mapper.readValue(TYPE_DOUBLE, ObjectNode.class));
    }
  }

  @Test
  public void shouldGroupDifferentTypes() throws JsonProcessingException {
    List<ObjectNode> schemas = new ArrayList<>();
    for (String schema : Arrays.asList(TYPE_INT, String.format(RECORD_WITH_STRING, "F1"), ARRAY_OF_NUMBERS, ARRAY_OF_NUMBERS_AND_STRINGS)) {
      schemas.add(mapper.readValue(schema, ObjectNode.class));
    }
    List<JsonNode> primitives = new ArrayList<>();
    List<JsonNode> records = new ArrayList<>();
    List<JsonNode> arrays = new ArrayList<>();

    DeriveSchemaUtils.groupItems(schemas.get(0), primitives, records, arrays);
    assertEquals(primitives.size(), 1);
    DeriveSchemaUtils.groupItems(schemas.get(1), primitives, records, arrays);
    assertEquals(records.size(), 1);
    DeriveSchemaUtils.groupItems(schemas.get(2), primitives, records, arrays);
    assertEquals(arrays.size(), 1);
    DeriveSchemaUtils.groupItems(schemas.get(3), primitives, records, arrays);
    assertEquals(arrays.size(), 2);
  }
}
