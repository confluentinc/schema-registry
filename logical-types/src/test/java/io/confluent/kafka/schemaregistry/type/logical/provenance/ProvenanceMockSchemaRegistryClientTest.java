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

import io.confluent.kafka.schemaregistry.ParsedSchema;
import io.confluent.kafka.schemaregistry.avro.AvroSchema;
import io.confluent.kafka.schemaregistry.client.rest.entities.ProvenanceAlgorithm;
import io.confluent.kafka.schemaregistry.client.rest.entities.ProvenanceField;
import io.confluent.kafka.schemaregistry.client.rest.entities.ProvenanceVersion;
import io.confluent.kafka.schemaregistry.client.rest.entities.SchemaProvenance;
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;
import io.confluent.kafka.schemaregistry.json.JsonSchema;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * The mock answers the provenance endpoint as the registry does — the same history semantics,
 * through the same shared code, failing with the same codes.
 */
class ProvenanceMockSchemaRegistryClientTest {

  private static final String SUBJECT = "orders-value";

  private final ProvenanceMockSchemaRegistryClient client =
      new ProvenanceMockSchemaRegistryClient();

  @Test
  void aRenameKeepsItsPidAcrossVersions() throws Exception {
    int v1 = register(record(field("id", "int"), field("name", "string")));
    int v2 = register(record(field("id", "int"),
        "{\"name\":\"full_name\",\"type\":\"string\",\"aliases\":[\"name\"]}"));

    SchemaProvenance provenance = client.getProvenanceById(SUBJECT, v2, v1, false, false, null);

    assertThat(provenance.getVersions()).extracting(ProvenanceVersion::getVersion)
        .containsExactly(1, 2);
    assertThat(provenance.getVersions()).extracting(ProvenanceVersion::getId)
        .containsExactly(v1, v2);
    assertThat(pids(provenance.getVersions().get(1))).containsExactly(1, 2);
    assertThat(provenance.getVersions().get(1).getFields().get(1).getNames())
        .containsExactly("full_name");
  }

  @Test
  void latestAndInteriorResolveAsTheRegistryResolvesThem() throws Exception {
    register(record(field("a", "int"), field("b", "int")));
    register(record(field("b", "int")));
    register(record(field("a", "int"), field("b", "int"), field("c", "int")));

    SchemaProvenance ends = client.getProvenanceByVersion(SUBJECT, "1", "latest", false, false, null);
    assertThat(ends.getVersions()).extracting(ProvenanceVersion::getVersion)
        .containsExactly(1, 3);
    // a was dropped at v2, which is not returned but still decides v3's ids.
    assertThat(pids(ends.getVersions().get(1))).containsExactly(3, 2, 4);
    assertThat(ends.getVersions().get(0).getFields().get(0).getNames()).containsExactly("a");

    assertThat(client.getProvenanceByVersion(SUBJECT, "1", "3", true, false, null).getVersions())
        .hasSize(3);
  }

  @Test
  void failuresCarryTheRegistrysCodes() throws Exception {
    int v1 = register(record(field("id", "int")));
    int other = client.register("other-value", new AvroSchema(record(field("x", "string"))));

    assertCode(404, 40401, () -> client.getProvenanceById("nope-value", v1, v1, false, false, null));
    assertCode(404, 40402, () -> client.getProvenanceByVersion(SUBJECT, "1", "9", false, false, null));
    assertCode(422, 42202, () -> client.getProvenanceByVersion(SUBJECT, "1", "x", false, false, null));
    assertCode(404, 40411, () -> client.getProvenanceById(SUBJECT, other, v1, false, false, null));
  }

  @Test
  void aMapWithNoValueSchemaHasNoLogicalForm() throws Exception {
    int v1 = client.register(SUBJECT, new JsonSchema("{\"type\":\"object\",\"properties\":"
        + "{\"m\":{\"type\":\"object\",\"connect.type\":\"map\"}}}"));
    assertCode(422, 42201, () -> client.getProvenanceById(SUBJECT, v1, v1, false, false, null));
  }

  @Test
  void anUnexpectedComputationFailureIsAServerError() throws Exception {
    ProvenanceMockSchemaRegistryClient failing = new ProvenanceMockSchemaRegistryClient() {
      @Override
      protected SchemaProvenance compute(String subject, List<ProvenanceHistory.Entry> range,
          List<ParsedSchema> schemas, boolean includeMultipleMessages,
          ProvenanceAlgorithm algorithm) {
        throw new NullPointerException("unexpected");
      }
    };
    int v1 = failing.register(SUBJECT, new AvroSchema(record(field("id", "int"))));
    assertCode(500, 500, () -> failing.getProvenanceById(SUBJECT, v1, v1, false, false, null));
  }

  @Test
  void aHistoryWhoseAliasesDetermineNoSingleIdentityIsA422() throws Exception {
    int v1 = register(record(field("a", "int")));
    int v2 = register(record("{\"name\":\"b\",\"type\":\"int\",\"aliases\":[\"a\"]}",
        "{\"name\":\"c\",\"type\":\"int\",\"aliases\":[\"a\"]}"));
    assertCode(422, 42217, () -> client.getProvenanceById(SUBJECT, v1, v2, false, false, null));
  }

  @Test
  void aRecursiveSchemaHasNoProvenance() throws Exception {
    int v1 = register("{\"type\":\"record\",\"name\":\"Node\",\"fields\":["
        + "{\"name\":\"next\",\"type\":[\"null\",\"Node\"],\"default\":null}]}");
    assertCode(422, 42213, () -> client.getProvenanceById(SUBJECT, v1, v1, false, false, null));
  }

  @Test
  void aVersionOutsideTheRangePlaysNoPart() throws Exception {
    register("{\"type\":\"record\",\"name\":\"Node\",\"fields\":["
        + "{\"name\":\"next\",\"type\":[\"null\",\"Node\"],\"default\":null}]}");
    register(record(field("a", "int")));
    register(record(field("a", "int"), field("b", "int")));

    // v1 has no provenance at all, and a range that leaves it out does not care.
    SchemaProvenance later = client.getProvenanceByVersion(SUBJECT, "2", "3", false, false, null);
    assertThat(pids(later.getVersions().get(1))).containsExactly(1, 2);
  }

  // -------------------------------------------------------------------------------------------
  // Helpers
  // -------------------------------------------------------------------------------------------

  private int register(String avro) throws Exception {
    return client.register(SUBJECT, new AvroSchema(avro));
  }

  private static void assertCode(int status, int code, ThrowingCall call) {
    assertThatThrownBy(call::run)
        .isInstanceOfSatisfying(RestClientException.class, e -> {
          assertThat(e.getStatus()).isEqualTo(status);
          assertThat(e.getErrorCode()).isEqualTo(code);
        });
  }

  private static List<Integer> pids(ProvenanceVersion version) {
    return version.getFields().stream().map(ProvenanceField::getPid)
        .collect(Collectors.toList());
  }

  private static String field(String name, String type) {
    return "{\"name\":\"" + name + "\",\"type\":\"" + type + "\"}";
  }

  private static String record(String... fields) {
    return "{\"type\":\"record\",\"name\":\"R\",\"namespace\":\"io.confluent\",\"fields\":["
        + String.join(",", fields) + "]}";
  }

  @FunctionalInterface
  private interface ThrowingCall {
    void run() throws Exception;
  }
}
