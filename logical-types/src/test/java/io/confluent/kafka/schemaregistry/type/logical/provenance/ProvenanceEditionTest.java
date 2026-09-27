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

package io.confluent.kafka.schemaregistry.type.logical.provenance;

import static org.assertj.core.api.Assertions.assertThat;

import io.confluent.kafka.schemaregistry.client.rest.entities.ProvenanceField;
import io.confluent.kafka.schemaregistry.client.rest.entities.SchemaProvenance;
import io.confluent.kafka.schemaregistry.json.JsonSchema;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import org.junit.jupiter.api.Test;

/**
 * Provenance reads JSON Schema as edition V1, the edition the Metastore's columns follow.
 */
class ProvenanceEditionTest {

  @Test
  void aBareOneBranchUnionKeepsItsStep() {
    List<List<Integer>> paths = paths(compute(json("{\"oneOf\":["
        + "{\"type\":\"object\",\"properties\":{\"a\":{\"type\":\"integer\"}}}]}")), 0);
    // u, its one branch, then a under the branch: V2 would collapse the branch away.
    assertThat(paths).containsExactly(
        Arrays.asList(0), Arrays.asList(0, 0), Arrays.asList(0, 0, 0));
  }

  @Test
  void aBranchTitleDoesNotChangeItsPid() {
    SchemaProvenance provenance = compute(
        json("{\"oneOf\":[{\"type\":\"integer\"},{\"type\":\"string\"}]}"),
        json("{\"oneOf\":[{\"title\":\"num\",\"type\":\"integer\"},"
            + "{\"title\":\"str\",\"type\":\"string\"}]}"));
    // V1 names branches by position, so a title is no rename.
    assertThat(pids(provenance, 1)).isEqualTo(pids(provenance, 0));
  }

  private static JsonSchema json(String u) {
    return new JsonSchema("{\"type\":\"object\",\"properties\":{\"u\":" + u + "}}");
  }

  private static SchemaProvenance compute(JsonSchema... versions) {
    List<ProvenanceHistory.Entry> history = new ArrayList<>();
    for (int i = 0; i < versions.length; i++) {
      history.add(new ProvenanceHistory.Entry(i + 1, i + 1, false));
    }
    return ProvenanceHistory.compute("s", history, Arrays.asList(versions));
  }

  private static List<List<Integer>> paths(SchemaProvenance provenance, int version) {
    return provenance.getVersions().get(version).getFields().stream()
        .map(ProvenanceField::getPath).collect(Collectors.toList());
  }

  private static List<Integer> pids(SchemaProvenance provenance, int version) {
    return provenance.getVersions().get(version).getFields().stream()
        .map(ProvenanceField::getPid).collect(Collectors.toList());
  }
}
