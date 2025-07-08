package io.confluent.kafka.schemaregistry.rules;

import static org.junit.Assert.assertEquals;

import com.fasterxml.jackson.databind.JsonNode;
import io.confluent.kafka.schemaregistry.ParsedSchema;
import io.confluent.kafka.schemaregistry.avro.AvroSchema;
import io.confluent.kafka.schemaregistry.client.rest.entities.Metadata;
import io.confluent.kafka.schemaregistry.client.rest.entities.Rule;
import io.confluent.kafka.schemaregistry.client.rest.entities.RuleKind;
import io.confluent.kafka.schemaregistry.client.rest.entities.RuleMode;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.junit.Test;

public class FieldRedactionExecutorTest {

  protected Schema createUserSchema() {
    String userSchema = "{\"namespace\": \"example.avro\", \"type\": \"record\", "
        + "\"name\": \"User\","
        + "\"fields\": ["
        + "{\"name\": \"firstName\", \"type\": [\"null\", \"string\"], \"confluent:tags\": [\"PII\"]},"
        + "{\"name\": \"lastName\", \"type\": [\"null\", \"string\"]},"
        + "{\"name\": \"binary\", \"type\": [\"null\", \"bytes\"], \"confluent:tags\": [\"PII\"]},"
        + "{\"name\": \"age\", \"type\": [\"null\", \"int\"]}"
        + "]}";
    Schema.Parser parser = new Schema.Parser();
    Schema schema = parser.parse(userSchema);
    return schema;
  }

  protected GenericRecord createUserRecord() {
    Schema schema = createUserSchema();
    GenericRecord avroRecord = new GenericData.Record(schema);
    avroRecord.put("firstName", "John");
    avroRecord.put("lastName", "Doe");
    avroRecord.put("binary", ByteBuffer.wrap("secret".getBytes(StandardCharsets.UTF_8)));
    avroRecord.put("age", 18);
    return avroRecord;
  }

  @Test
  public void testSimpleRedaction() throws Exception {
    ParsedSchema schema = new AvroSchema(createUserSchema());
    Rule rule = new Rule("encrypt", null, RuleKind.TRANSFORM, RuleMode.WRITE, "ENCRYPT",
        Collections.singleton("PII"), null, null, null, null, false);
    RuleContext ctx = new RuleContext(Collections.emptyMap(), null, schema,
        "test-value", "test", null,
        null, null, false,
        RuleMode.WRITE, rule, 0, Collections.singletonList(rule));
    List<String> redactRuleTypes = Collections.singletonList("ENCRYPT");
    Object message = createUserRecord();
    message = DlqAction.redactFields(ctx, message, redactRuleTypes);
    JsonNode json = schema.toJson(message);
    assertEquals("<REDACTED>", json.get("firstName").get("string").asText());
    assertEquals("<REDACTED>", json.get("binary").get("bytes").asText());
  }

  @Test
  public void testWildcardRedaction() throws Exception {
    ParsedSchema schema = new AvroSchema(createUserSchema());
    Map<String, Set<String>> tags =
        Collections.singletonMap("**.lastName", Collections.singleton("PII2"));
    Metadata metadata = new Metadata(tags, null, null);
    schema = schema.copy(metadata, null);
    Rule rule = new Rule("encrypt", null, RuleKind.TRANSFORM, RuleMode.WRITE, "ENCRYPT",
        Collections.singleton("PII2"), null, null, null, null, false);
    RuleContext ctx = new RuleContext(Collections.emptyMap(), null, schema,
        "test-value", "test", null,
        null, null, false,
        RuleMode.WRITE, rule, 0, Collections.singletonList(rule));
    List<String> redactRuleTypes = Collections.singletonList("ENCRYPT");
    Object message = createUserRecord();
    message = DlqAction.redactFields(ctx, message, redactRuleTypes);
    JsonNode json = schema.toJson(message);
    assertEquals("<REDACTED>", json.get("lastName").get("string").asText());
  }
}
