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

package io.confluent.kafka.schemaregistry.client.rest.entities;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.confluent.kafka.schemaregistry.utils.JacksonMapper;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import org.junit.Test;

public class SchemaProvenanceTest {

  private static final ObjectMapper MAPPER = JacksonMapper.INSTANCE;

  @Test
  public void serializesToTheDocumentedShape() throws Exception {
    SchemaProvenance provenance = new SchemaProvenance("orders-value", Arrays.asList(
        new ProvenanceVersion(1, 1001, Arrays.asList(
            field(path(0), names("id"), 1),
            field(path(1), names("name"), 2),
            field(path(2), names("region"), 3))),
        new ProvenanceVersion(2, 1002, Arrays.asList(
            field(path(0), names("id"), 1),
            field(path(1), names("full_name"), 2),
            field(path(2), names("tier"), 4)))));

    assertEquals(MAPPER.readTree(
        "{\"subject\":\"orders-value\",\"versions\":["
            + "{\"version\":1,\"id\":1001,\"fields\":["
            + "{\"path\":[0],\"names\":[\"id\"],\"pid\":1},"
            + "{\"path\":[1],\"names\":[\"name\"],\"pid\":2},"
            + "{\"path\":[2],\"names\":[\"region\"],\"pid\":3}]},"
            + "{\"version\":2,\"id\":1002,\"fields\":["
            + "{\"path\":[0],\"names\":[\"id\"],\"pid\":1},"
            + "{\"path\":[1],\"names\":[\"full_name\"],\"pid\":2},"
            + "{\"path\":[2],\"names\":[\"tier\"],\"pid\":4}]}]}"),
        MAPPER.valueToTree(provenance));
  }

  @Test
  public void namesAreOmittedWhenNotVerbose() {
    JsonNode json = MAPPER.valueToTree(field(path(0), null, 1));
    assertFalse(json.has("names"));
  }

  @Test
  public void aVersionWithNoMembersSaysSo() {
    JsonNode json = MAPPER.valueToTree(new ProvenanceVersion(1, 1001, Collections.emptyList()));
    assertTrue(json.get("fields").isArray());
    assertEquals(0, json.get("fields").size());
  }

  @Test
  public void roundTripsAndIgnoresUnknownProperties() throws Exception {
    String json = "{\"subject\":\"s\",\"future\":true,\"versions\":["
        + "{\"version\":3,\"id\":7,\"fields\":["
        + "{\"path\":[0,1],\"pid\":2,\"default\":0,\"alsoFuture\":1}]}]}";
    SchemaProvenance read = MAPPER.readValue(json, SchemaProvenance.class);

    assertEquals(new SchemaProvenance("s", Collections.singletonList(
        new ProvenanceVersion(3, 7, Collections.singletonList(
            field(path(0, 1), null, 2))))), read);
  }

  private static ProvenanceField field(List<Integer> path, List<String> names, int id) {
    return new ProvenanceField(path, names, id);
  }

  private static List<Integer> path(Integer... steps) {
    return Arrays.asList(steps);
  }

  private static List<String> names(String... names) {
    return Arrays.asList(names);
  }
}
