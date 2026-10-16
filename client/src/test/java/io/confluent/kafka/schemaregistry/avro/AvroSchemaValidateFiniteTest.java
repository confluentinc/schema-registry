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

import io.confluent.kafka.schemaregistry.client.rest.entities.SchemaReference;
import org.apache.avro.AvroRuntimeException;
import org.junit.Test;

import java.util.Collections;
import java.util.Map;

import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

public class AvroSchemaValidateFiniteTest {

  private static AvroSchema parse(String schema) {
    return new AvroSchema(schema, Collections.emptyList(), Collections.emptyMap(), null, true);
  }

  private static String rejected(String schema) {
    AvroSchema parsed = parse(schema);
    return assertThrows(IllegalArgumentException.class, parsed::validate).getMessage();
  }

  @Test
  public void requiredSelfReferenceRejected() {
    String message = rejected("{\"type\":\"record\",\"name\":\"Node\",\"fields\":["
        + "{\"name\":\"id\",\"type\":\"int\"},"
        + "{\"name\":\"next\",\"type\":\"Node\"}]}");
    assertTrue(message, message.contains("Node -> Node"));
    assertTrue(message, message.contains("'next'"));
  }

  @Test
  public void onlyFieldSelfReferenceRejected() {
    rejected("{\"type\":\"record\",\"name\":\"Node\",\"fields\":["
        + "{\"name\":\"next\",\"type\":\"Node\"}]}");
  }

  @Test
  public void requiredMutualRecursionRejected() {
    String message = rejected("{\"type\":\"record\",\"name\":\"A\",\"fields\":["
        + "{\"name\":\"b\",\"type\":{\"type\":\"record\",\"name\":\"B\",\"fields\":["
        + "{\"name\":\"a\",\"type\":\"A\"}]}}]}");
    assertTrue(message, message.contains("A -> B -> A"));
  }

  @Test
  public void unbuildableNestedRecordRejected() {
    // The root itself is not recursive, but its required field can never be filled
    String message = rejected("{\"type\":\"record\",\"name\":\"Outer\",\"fields\":["
        + "{\"name\":\"node\",\"type\":{\"type\":\"record\",\"name\":\"Node\",\"fields\":["
        + "{\"name\":\"next\",\"type\":\"Node\"}]}}]}");
    assertTrue(message, message.contains("'node'"));
  }

  @Test
  public void unionOfOnlyUnbuildableBranchesRejected() {
    rejected("{\"type\":\"record\",\"name\":\"Node\",\"fields\":["
        + "{\"name\":\"next\",\"type\":[\"Node\"]}]}");
  }

  @Test
  public void selfReferenceWithDefaultRejected() {
    // A partial default fills missing fields from their own defaults, forever
    // New schemas already fail default validation while parsing; this covers lookup
    String schema = "{\"type\":\"record\",\"name\":\"Node\",\"fields\":["
        + "{\"name\":\"id\",\"type\":\"int\",\"default\":0},"
        + "{\"name\":\"next\",\"type\":\"Node\",\"default\":{\"id\":0}}]}";
    assertThrows(AvroRuntimeException.class, () -> parse(schema));
    AvroSchema parsed = new AvroSchema(
        schema, Collections.emptyList(), Collections.emptyMap(), null, false);
    assertThrows(IllegalArgumentException.class, parsed::validate);
  }

  @Test
  public void requiredSelfReferenceThroughReferenceRejected() {
    String node = "{\"type\":\"record\",\"name\":\"Node\",\"fields\":["
        + "{\"name\":\"next\",\"type\":\"Node\"}]}";
    String root = "{\"type\":\"record\",\"name\":\"Root\",\"fields\":["
        + "{\"name\":\"node\",\"type\":\"Node\"}]}";
    Map<String, String> resolved = Collections.singletonMap("node.avsc", node);
    AvroSchema parsed = new AvroSchema(root,
        Collections.singletonList(new SchemaReference("Node", "node", 1)),
        resolved, null, true);
    assertThrows(IllegalArgumentException.class, parsed::validate);
  }

  @Test
  public void emptyUnionRejected() {
    String message = rejected("{\"type\":\"record\",\"name\":\"R\",\"fields\":["
        + "{\"name\":\"u\",\"type\":[]}]}");
    assertTrue(message, message.contains("R through fields 'u' (an empty union)"));
  }

  @Test
  public void emptyEnumRejected() {
    String message = rejected("{\"type\":\"record\",\"name\":\"R\",\"fields\":["
        + "{\"name\":\"e\",\"type\":{\"type\":\"enum\",\"name\":\"E\",\"symbols\":[]}}]}");
    assertTrue(message, message.contains("R -> E through fields 'e' (an enum with no symbols)"));
  }

  @Test
  public void emptyEnumAtRootRejected() {
    rejected("{\"type\":\"enum\",\"name\":\"E\",\"symbols\":[]}");
  }

  @Test
  public void emptyEnumOnOptionalPathAccepted() {
    parse("{\"type\":\"record\",\"name\":\"R\",\"fields\":["
        + "{\"name\":\"e\",\"type\":[\"null\","
        + "{\"type\":\"enum\",\"name\":\"E\",\"symbols\":[]}]}]}").validate();
  }

  @Test
  public void nullableSelfReferenceAccepted() {
    parse("{\"type\":\"record\",\"name\":\"Node\",\"fields\":["
        + "{\"name\":\"id\",\"type\":\"int\"},"
        + "{\"name\":\"next\",\"type\":[\"null\",\"Node\"]}]}").validate();
  }

  @Test
  public void unionWithNonRecursiveBranchAccepted() {
    parse("{\"type\":\"record\",\"name\":\"Expr\",\"fields\":["
        + "{\"name\":\"left\",\"type\":[\"Expr\",\"int\"]},"
        + "{\"name\":\"right\",\"type\":[\"Expr\",\"int\"]}]}").validate();
  }

  @Test
  public void arraySelfReferenceAccepted() {
    parse("{\"type\":\"record\",\"name\":\"Tree\",\"fields\":["
        + "{\"name\":\"children\",\"type\":{\"type\":\"array\",\"items\":\"Tree\"}}]}")
        .validate();
  }

  @Test
  public void mapSelfReferenceAccepted() {
    parse("{\"type\":\"record\",\"name\":\"Tree\",\"fields\":["
        + "{\"name\":\"children\",\"type\":{\"type\":\"map\",\"values\":\"Tree\"}}]}")
        .validate();
  }

  @Test
  public void unbuildableTypeOnOptionalPathAccepted() {
    // Node has no finite value, but the null branch keeps Outer buildable
    parse("{\"type\":\"record\",\"name\":\"Outer\",\"fields\":["
        + "{\"name\":\"node\",\"type\":[\"null\",{\"type\":\"record\",\"name\":\"Node\","
        + "\"fields\":[{\"name\":\"next\",\"type\":\"Node\"}]}]}]}").validate();
  }

  @Test
  public void nonRecursiveSchemasAccepted() {
    parse("\"string\"").validate();
    parse("{\"type\":\"record\",\"name\":\"R\",\"fields\":["
        + "{\"name\":\"e\",\"type\":{\"type\":\"enum\",\"name\":\"E\",\"symbols\":[\"A\"]}},"
        + "{\"name\":\"f\",\"type\":{\"type\":\"fixed\",\"name\":\"F\",\"size\":4}},"
        + "{\"name\":\"empty\",\"type\":{\"type\":\"record\",\"name\":\"Empty\","
        + "\"fields\":[]}}]}").validate();
  }
}
