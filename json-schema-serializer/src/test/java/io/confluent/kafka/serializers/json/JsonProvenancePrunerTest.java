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

package io.confluent.kafka.serializers.json;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.confluent.kafka.schemaregistry.client.rest.entities.ProvenanceField;
import io.confluent.kafka.schemaregistry.client.rest.entities.ProvenanceVersion;
import io.confluent.kafka.schemaregistry.client.rest.entities.SchemaProvenance;
import io.confluent.kafka.schemaregistry.json.JsonSchema;
import io.confluent.kafka.serializers.provenance.ProvenanceMapping;
import java.util.Arrays;
import java.util.List;
import org.apache.kafka.common.errors.SerializationException;
import org.junit.Test;

/** A response the reader cannot follow fails every record from that writer. */
public class JsonProvenancePrunerTest {

  private static final JsonSchema READER = new JsonSchema("{\"type\":\"object\",\"properties\":"
      + "{\"a\":{\"type\":\"integer\"},\"u\":{\"oneOf\":[{\"type\":\"string\"},{\"type\":"
      + "\"object\",\"properties\":{\"b\":{\"type\":\"integer\"}}}]}}}");

  @Test
  public void aLocationWithoutNamesFailsEveryRecord() {
    assertThrows(SerializationException.class, () -> JsonProvenancePruner.plan(
        mapping(Arrays.asList(p(1, "a")),
            Arrays.asList(p(1, "a"), new ProvenanceField(Arrays.asList(2), null, "SCALAR", 2))),
        READER));
  }

  @Test
  public void aLocationWithoutAKindFailsEveryRecord() {
    SerializationException e = assertThrows(SerializationException.class,
        () -> JsonProvenancePruner.plan(mapping(Arrays.asList(p(1, "a")), Arrays.asList(p(1, "a"),
            new ProvenanceField(Arrays.asList(2), Arrays.asList("b"), 2))), READER));
    assertTrue(e.getMessage(), e.getMessage().contains("no kind for location [2]"));
  }

  @Test
  public void aNewPropertyTheReaderDoesNotDeclareFailsEveryRecord() {
    SerializationException e = assertThrows(SerializationException.class,
        () -> JsonProvenancePruner.plan(mapping(Arrays.asList(p(1, "a")),
            Arrays.asList(p(1, "a"), p(2, "ghost"))), READER));
    assertTrue(e.getMessage(), e.getMessage().contains("[ghost] of schema id 2"));
  }

  @Test
  public void aNewPropertyInAUnionBranchIsDeclared() throws Exception {
    // u.b is declared by u's object branch only; the plan follows it, and prunes the new u.
    JsonProvenancePruner pruner = JsonProvenancePruner.plan(mapping(Arrays.asList(p(1, "a")),
        Arrays.asList(p(1, "a"), p(2, "u"), p(3, "u", "b"))), READER);
    JsonNode document = new ObjectMapper().readTree("{\"a\":1,\"u\":{\"b\":2}}");
    pruner.prune(document);
    assertEquals("{\"a\":1}", document.toString());
  }

  private static ProvenanceMapping mapping(List<ProvenanceField> writer,
      List<ProvenanceField> reader) {
    return ProvenanceMapping.join(new SchemaProvenance("s", Arrays.asList(
        new ProvenanceVersion(1, 1, "STRUCT", writer),
        new ProvenanceVersion(2, 2, "STRUCT", reader))), 1, 2);
  }

  // Flat, unique paths: with no enclosing location, every location is a property.
  private static ProvenanceField p(int pid, String... names) {
    return new ProvenanceField(Arrays.asList(pid), Arrays.asList(names), "SCALAR", pid);
  }
}
