/*
 * Copyright 2026 Confluent Inc.
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

package io.confluent.kafka.schemaregistry.avro;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import io.confluent.kafka.schemaregistry.rules.ValidationRuleError;
import io.confluent.kafka.schemaregistry.rules.ValidationRuleExecutor;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.junit.Test;

/**
 * Walker-level tests for {@link AvroSchema#validateMessage}. Uses a stub
 * {@link ValidationRuleExecutor} that always returns {@code false}, so every rule the
 * walker fires turns into a {@link ValidationRuleError}; we then assert on the rule names
 * + paths to verify the walker's dispatch (recursion into nested records, array/map
 * iteration, skip-on-null for nullable fields).
 */
public class AvroSchemaValidateMessageTest {

  /** Stub: every rule fires a violation. Lets us inspect what the walker did. */
  private static final ValidationRuleExecutor ALWAYS_FAIL =
      (rule, schema, value) -> Boolean.FALSE;

  /** Names of all rules the walker fired, paired with field paths. */
  private static List<String> firedRules(List<ValidationRuleError> errors) {
    List<String> out = new java.util.ArrayList<>();
    for (ValidationRuleError e : errors) {
      out.add(e.getRule().getName() + "@" + e.getFieldPath());
    }
    return out;
  }

  @Test
  public void nestedRecord_recursesAndProducesDottedPath() throws Exception {
    String schemaStr = "{"
        + "\"type\":\"record\",\"name\":\"Outer\","
        + "\"fields\":[{\"name\":\"inner\",\"type\":{"
        + "  \"type\":\"record\",\"name\":\"Inner\","
        + "  \"fields\":[{\"name\":\"x\",\"type\":\"int\","
        + "    \"confluent:rules\":[{\"name\":\"r\",\"expr\":\"true\"}]}]"
        + "}}]"
        + "}";
    AvroSchema schema = new AvroSchema(schemaStr);
    Schema avro = schema.rawSchema();
    GenericRecord inner = new GenericData.Record(avro.getField("inner").schema());
    inner.put("x", 5);
    GenericRecord outer = new GenericData.Record(avro);
    outer.put("inner", inner);

    List<ValidationRuleError> errors = schema.validateMessage(ALWAYS_FAIL, outer);
    assertEquals(java.util.Collections.singletonList("r@inner.x"), firedRules(errors));
  }

  @Test
  public void arrayOfRecords_firesRulePerElementWithIndexedPath() throws Exception {
    String schemaStr = "{"
        + "\"type\":\"record\",\"name\":\"Outer\","
        + "\"fields\":[{\"name\":\"items\",\"type\":{"
        + "  \"type\":\"array\",\"items\":{"
        + "    \"type\":\"record\",\"name\":\"Item\","
        + "    \"fields\":[{\"name\":\"x\",\"type\":\"int\","
        + "      \"confluent:rules\":[{\"name\":\"r\",\"expr\":\"true\"}]}]"
        + "  }}}]"
        + "}";
    AvroSchema schema = new AvroSchema(schemaStr);
    Schema avro = schema.rawSchema();
    Schema itemSchema = avro.getField("items").schema().getElementType();
    GenericRecord i0 = new GenericData.Record(itemSchema);
    i0.put("x", 1);
    GenericRecord i1 = new GenericData.Record(itemSchema);
    i1.put("x", 2);
    GenericRecord outer = new GenericData.Record(avro);
    outer.put("items", java.util.Arrays.asList(i0, i1));

    List<ValidationRuleError> errors = schema.validateMessage(ALWAYS_FAIL, outer);
    assertEquals(java.util.Arrays.asList("r@items[0].x", "r@items[1].x"), firedRules(errors));
  }

  @Test
  public void failFast_stopsAfterFirstViolation() throws Exception {
    // Same array-of-records shape as above (which without fail-fast produces
    // two violations, one per element). With failFast=true the walker should
    // short-circuit after the first.
    String schemaStr = "{"
        + "\"type\":\"record\",\"name\":\"Outer\","
        + "\"fields\":[{\"name\":\"items\",\"type\":{"
        + "  \"type\":\"array\",\"items\":{"
        + "    \"type\":\"record\",\"name\":\"Item\","
        + "    \"fields\":[{\"name\":\"x\",\"type\":\"int\","
        + "      \"confluent:rules\":[{\"name\":\"r\",\"expr\":\"true\"}]}]"
        + "  }}}]"
        + "}";
    AvroSchema schema = new AvroSchema(schemaStr);
    Schema avro = schema.rawSchema();
    Schema itemSchema = avro.getField("items").schema().getElementType();
    GenericRecord i0 = new GenericData.Record(itemSchema);
    i0.put("x", 1);
    GenericRecord i1 = new GenericData.Record(itemSchema);
    i1.put("x", 2);
    GenericRecord outer = new GenericData.Record(avro);
    outer.put("items", java.util.Arrays.asList(i0, i1));

    List<ValidationRuleError> errors = schema.validateMessage(ALWAYS_FAIL, outer, true);
    assertEquals(java.util.Collections.singletonList("r@items[0].x"), firedRules(errors));
  }

  @Test
  public void mapOfRecords_firesRulePerEntryWithKeyedPath() throws Exception {
    String schemaStr = "{"
        + "\"type\":\"record\",\"name\":\"Outer\","
        + "\"fields\":[{\"name\":\"scores\",\"type\":{"
        + "  \"type\":\"map\",\"values\":{"
        + "    \"type\":\"record\",\"name\":\"Score\","
        + "    \"fields\":[{\"name\":\"v\",\"type\":\"int\","
        + "      \"confluent:rules\":[{\"name\":\"r\",\"expr\":\"true\"}]}]"
        + "  }}}]"
        + "}";
    AvroSchema schema = new AvroSchema(schemaStr);
    Schema avro = schema.rawSchema();
    Schema valueSchema = avro.getField("scores").schema().getValueType();
    GenericRecord sa = new GenericData.Record(valueSchema);
    sa.put("v", 10);
    GenericRecord sb = new GenericData.Record(valueSchema);
    sb.put("v", 20);
    Map<String, GenericRecord> scores = new LinkedHashMap<>();
    scores.put("alice", sa);
    scores.put("bob", sb);
    GenericRecord outer = new GenericData.Record(avro);
    outer.put("scores", scores);

    List<ValidationRuleError> errors = schema.validateMessage(ALWAYS_FAIL, outer);
    // Iteration order over LinkedHashMap is preserved.
    assertEquals(java.util.Arrays.asList("r@scores[\"alice\"].v", "r@scores[\"bob\"].v"),
        firedRules(errors));
  }

  @Test
  public void nullableField_skipsRuleWhenValueIsNull() throws Exception {
    // ["null","string"] union with a field-level rule. When value is null the walker
    // must skip the rule; when set, the rule fires.
    String schemaStr = "{"
        + "\"type\":\"record\",\"name\":\"Outer\","
        + "\"fields\":[{\"name\":\"maybeName\",\"type\":[\"null\",\"string\"],"
        + "  \"default\":null,"
        + "  \"confluent:rules\":[{\"name\":\"r\",\"expr\":\"true\"}]}]"
        + "}";
    AvroSchema schema = new AvroSchema(schemaStr);
    Schema avro = schema.rawSchema();

    GenericRecord nullRec = new GenericData.Record(avro);
    nullRec.put("maybeName", null);
    assertTrue("Rule must skip when nullable field is null",
        schema.validateMessage(ALWAYS_FAIL, nullRec).isEmpty());

    GenericRecord setRec = new GenericData.Record(avro);
    setRec.put("maybeName", "alice");
    assertEquals(java.util.Collections.singletonList("r@maybeName"),
        firedRules(schema.validateMessage(ALWAYS_FAIL, setRec)));
  }

  @Test
  public void multipleRulesOnSameField_allFire() throws Exception {
    String schemaStr = "{"
        + "\"type\":\"record\",\"name\":\"Outer\","
        + "\"fields\":[{\"name\":\"x\",\"type\":\"int\","
        + "  \"confluent:rules\":["
        + "    {\"name\":\"r1\",\"expr\":\"true\"},"
        + "    {\"name\":\"r2\",\"expr\":\"true\"}]}]"
        + "}";
    AvroSchema schema = new AvroSchema(schemaStr);
    Schema avro = schema.rawSchema();
    GenericRecord rec = new GenericData.Record(avro);
    rec.put("x", 7);

    assertEquals(java.util.Arrays.asList("r1@x", "r2@x"),
        firedRules(schema.validateMessage(ALWAYS_FAIL, rec)));
  }
}
