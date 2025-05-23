package io.confluent.kafka.schemaregistry.builtin;

import static org.junit.jupiter.api.Assertions.assertEquals;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;

public class SchemaBuilderTest {

  @Test
  public void testSchemaBuilder() throws Exception {
    Schema schema = SchemaBuilder.struct("ExampleStruct")
        .namespace("com.example")
        .prop("key1", "value1")
        .fields().name("field1").type().stringBuilder().prop("key2", "value2").endString().stringDefault("hi")
        .name("field2").type().intType().intDefault(42)
        .endStruct();

    ObjectMapper mapper = new ObjectMapper();
    String actual = mapper.writeValueAsString(schema);
    String expected = "{\"name\":\"ExampleStruct\",\"namespace\":\"com.example\",\"params\":{\"key1\":\"value1\"},\"fields\":[{\"name\":\"field1\",\"type\":{\"type\":\"string\",\"params\":{\"key2\":\"value2\"}},\"default\":\"hi\"},{\"name\":\"field2\",\"type\":\"int32\",\"default\":42}],\"type\":\"struct\"}";
    assertEquals(expected, actual);

    schema = mapper.readValue(actual, Schema.class);
    actual = mapper.writeValueAsString(schema);
    expected = "{\"name\":\"ExampleStruct\",\"namespace\":\"com.example\",\"params\":{\"key1\":\"value1\"},\"fields\":[{\"name\":\"field1\",\"type\":{\"type\":\"string\",\"params\":{\"key2\":\"value2\"}},\"default\":\"hi\"},{\"name\":\"field2\",\"type\":\"int32\",\"default\":42}],\"type\":\"struct\"}";
    assertEquals(expected, actual);
  }
}
