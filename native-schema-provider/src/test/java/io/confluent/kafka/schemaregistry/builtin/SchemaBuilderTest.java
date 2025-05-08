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
        .fields().name("field1").type().stringType().stringDefault("hi")
        .name("field2").type().intType().intDefault(42)
        .endStruct();

    ObjectMapper mapper = new ObjectMapper();
    String actual = mapper.writeValueAsString(schema);
    String expected = "{\"name\":\"ExampleStruct\",\"fields\":["
        + "{\"name\":\"field1\",\"type\":{\"type\":\"string\"},\"default\":\"hi\"},"
        + "{\"name\":\"field2\",\"type\":{\"type\":\"int\"},\"default\":42}],"
        + "\"namespace\":\"com.example\",\"type\":\"struct\",\"params\":{\"key1\":\"value1\"}}";
    assertEquals(expected, actual);
  }
}
