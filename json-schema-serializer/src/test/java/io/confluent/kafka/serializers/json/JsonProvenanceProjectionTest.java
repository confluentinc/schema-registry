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
import com.sun.management.ThreadMXBean;
import io.confluent.kafka.schemaregistry.client.rest.entities.ProvenanceField;
import io.confluent.kafka.schemaregistry.client.rest.entities.ProvenanceVersion;
import io.confluent.kafka.schemaregistry.client.rest.entities.SchemaProvenance;
import io.confluent.kafka.schemaregistry.json.JsonSchema;
import io.confluent.kafka.serializers.provenance.ProvenanceMapping;
import java.lang.management.ManagementFactory;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import org.apache.kafka.common.errors.SerializationException;
import org.junit.Test;

/** A response the reader cannot follow fails every record from that writer. */
public class JsonProvenanceProjectionTest {

  private static final JsonSchema READER = new JsonSchema("{\"type\":\"object\",\"properties\":"
      + "{\"a\":{\"type\":\"integer\"},\"u\":{\"oneOf\":[{\"type\":\"string\"},{\"type\":"
      + "\"object\",\"properties\":{\"b\":{\"type\":\"integer\"}}}]}}}");

  @Test
  public void aLocationWithoutNamesFailsEveryRecord() {
    assertThrows(SerializationException.class, () -> JsonProvenanceProjection.of(
        mapping(Arrays.asList(p(1, "a")),
            Arrays.asList(p(1, "a"), new ProvenanceField(Arrays.asList(2), null, "SCALAR", 2))),
        READER));
  }

  @Test
  public void aLocationWithoutAKindFailsEveryRecord() {
    SerializationException e = assertThrows(SerializationException.class,
        () -> JsonProvenanceProjection.of(mapping(Arrays.asList(p(1, "a")), Arrays.asList(p(1, "a"),
            new ProvenanceField(Arrays.asList(2), Arrays.asList("b"), null, 2))), READER));
    assertTrue(e.getMessage(), e.getMessage().contains("no kind for location [2]"));
  }

  @Test
  public void aNewPropertyTheReaderDoesNotDeclareFailsEveryRecord() {
    SerializationException e = assertThrows(SerializationException.class,
        () -> JsonProvenanceProjection.of(mapping(Arrays.asList(p(1, "a")),
            Arrays.asList(p(1, "a"), p(2, "ghost"))), READER));
    assertTrue(e.getMessage(), e.getMessage().contains("[ghost] of schema id 2"));
  }

  @Test
  public void aNewPropertyInAUnionBranchIsDeclared() throws Exception {
    // u.b is declared by u's object branch only; the plan follows it, and prunes the new u.
    JsonProvenanceProjection projection = JsonProvenanceProjection.of(mapping(Arrays.asList(p(1, "a")),
        Arrays.asList(p(1, "a"), p(2, "u"), p(3, "u", "b"))), READER);
    JsonNode document = new ObjectMapper().readTree("{\"a\":1,\"u\":{\"b\":2}}");
    projection.prune(document);
    assertEquals("{\"a\":1}", document.toString());
  }

  @Test
  public void aRootKindTheReaderContradictsFailsEveryRecord() {
    // Read as a union, the reader's properties would pass for branches: nothing would be pruned.
    SerializationException e = assertThrows(SerializationException.class,
        () -> JsonProvenanceProjection.of(ProvenanceMapping.join(new SchemaProvenance("s",
            Arrays.asList(new ProvenanceVersion(1, 1, "STRUCT", Arrays.asList(p(1, "a"))),
                new ProvenanceVersion(2, 2, "UNION", Arrays.asList(p(1, "a"), p(2, "u"))))),
            1, 2), READER));
    assertTrue(e.getMessage(), e.getMessage().contains("a root of kind UNION"));
  }

  @Test
  public void aRecordHoldingNoneOfManyNewPropertiesAllocatesLittleToPrune() throws Exception {
    // 25 properties new to the reader, none in the record: the writer is never walked for them,
    // and no walk keeps state. About 55 KB a record before, 8 KB after.
    StringBuilder properties = new StringBuilder("{\"type\":\"object\",\"properties\":{"
        + "\"a\":{\"type\":\"integer\"}");
    List<ProvenanceField> reader = new ArrayList<>(Arrays.asList(p(1, "a")));
    for (int i = 0; i < 25; i++) {
      properties.append(",\"n").append(i).append("\":{\"type\":\"integer\"}");
      reader.add(p(2 + i, "n" + i));
    }
    JsonProvenanceProjection projection = JsonProvenanceProjection.of(
        mapping(Arrays.asList(p(1, "a")), reader),
        new JsonSchema(properties.append("}}").toString()),
        new JsonSchema("{\"type\":\"object\",\"properties\":{\"a\":{\"type\":\"integer\"}}}"));
    JsonNode document = new ObjectMapper().readTree("{\"a\":1}");
    ThreadMXBean threads = (ThreadMXBean) ManagementFactory.getThreadMXBean();
    long self = Thread.currentThread().getId();
    for (int i = 0; i < 1000; i++) {
      projection.prune(document);
    }
    long before = threads.getThreadAllocatedBytes(self);
    for (int i = 0; i < 1000; i++) {
      projection.prune(document);
    }
    long perRecord = (threads.getThreadAllocatedBytes(self) - before) / 1000;
    assertEquals("{\"a\":1}", document.toString());
    assertTrue("allocated " + perRecord + " bytes a record", perRecord < 20_000);
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
