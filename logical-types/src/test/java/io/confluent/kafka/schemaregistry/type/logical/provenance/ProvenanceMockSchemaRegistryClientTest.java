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

import io.confluent.kafka.schemaregistry.client.SchemaMetadata;
import io.confluent.kafka.schemaregistry.type.logical.LogicalType;
import io.confluent.kafka.schemaregistry.ParsedSchemaHolder;
import io.confluent.kafka.schemaregistry.avro.AvroSchema;
import io.confluent.kafka.schemaregistry.client.rest.entities.ProvenanceField;
import io.confluent.kafka.schemaregistry.client.rest.entities.ProvenanceVersion;
import io.confluent.kafka.schemaregistry.client.rest.entities.SchemaProvenance;
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;
import io.confluent.kafka.schemaregistry.json.JsonSchema;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.IntFunction;
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
    assertCode(422, 42202, () -> client.getProvenanceByVersion(SUBJECT, "1", "0", false, false, null));
    assertCode(422, 42202, () -> client.getProvenanceByVersion(SUBJECT, "-2", "1", false, false, null));
    assertCode(422, 42202, () -> client.getProvenanceToVersion(SUBJECT, v1, 0, false, false, null));
    assertCode(404, 40411, () -> client.getProvenanceById(SUBJECT, other, v1, false, false, null));
  }

  @Test
  void aRangeWithItsInteriorCoversAtMostOneHundredVersions() throws Exception {
    // As the registry's default: the ends of a longer range are still read.
    for (int i = 0; i <= 100; i++) {
      register(record(field("f" + i, "int")));
    }
    assertCode(422, 42219,
        () -> client.getProvenanceByVersion(SUBJECT, "1", "101", true, false, null));
    assertThat(client.getProvenanceByVersion(SUBJECT, "1", "100", true, false, null)
        .getVersions()).hasSize(100);
    assertThat(client.getProvenanceByVersion(SUBJECT, "1", "101", false, false, null)
        .getVersions()).hasSize(2);
  }

  @Test
  void dynamicComputesAsV1AndSaysSo() throws Exception {
    int v1 = register(record(field("id", "int"), field("name", "string")));
    int v2 = register(record(field("id", "int"),
        "{\"name\":\"full_name\",\"type\":\"string\",\"aliases\":[\"name\"]}"));

    SchemaProvenance dynamic = client.getProvenanceById(SUBJECT, v1, v2, false, false, "dynamic");

    assertThat(dynamic.getAlgorithm()).isEqualTo("dynamic");
    assertThat(dynamic.getVersions()).isEqualTo(
        client.getProvenanceById(SUBJECT, v1, v2, false, false, "v1").getVersions());
  }

  @Test
  void anUnknownAlgorithmIsRejected() throws Exception {
    int v1 = register(record(field("id", "int")));
    assertCode(422, 42216, () -> client.getProvenanceById(SUBJECT, v1, v1, false, false, "v9"));
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
      protected SchemaProvenance compute(String subject, List<SchemaMetadata> range,
          List<? extends ParsedSchemaHolder> schemas, boolean includeMultipleMessages,
          boolean includeInterior, String algorithm) {
        throw new NullPointerException("unexpected");
      }
    };
    int v1 = failing.register(SUBJECT, new AvroSchema(record(field("id", "int"))));
    assertCode(500, 500, () -> failing.getProvenanceById(SUBJECT, v1, v1, false, false, null));
  }

  @Test
  void aDynamicRangeOverAnAlgorithmNotSupportedIsRejectedNotRetried() throws Exception {
    // A 422 with the unknown-algorithm code, which a client stops asking about; a 500 it retries.
    ProvenanceMockSchemaRegistryClient failing = new ProvenanceMockSchemaRegistryClient() {
      @Override
      protected SchemaProvenance compute(String subject, List<SchemaMetadata> range,
          List<? extends ParsedSchemaHolder> schemas, boolean includeMultipleMessages,
          boolean includeInterior, String algorithm) {
        throw new UnsupportedProvenanceAlgorithmException("v2 beside v1");
      }
    };
    int v1 = failing.register(SUBJECT, new AvroSchema(record(field("id", "int"))));
    assertCode(422, 42216, () -> failing.getProvenanceById(SUBJECT, v1, v1, false, false,
        "dynamic"));
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
    assertCode(422, 42220, () -> client.getProvenanceById(SUBJECT, v1, v1, false, false, null));
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

  @Test
  void aSoftDeletedVersionStillDecidesTheIds() throws Exception {
    // As in the registry: v2, soft-deleted, dropped a, so a is new at v3.
    int v1 = register(record(field("a", "int"), field("b", "int")));
    register(record(field("b", "int")));
    int v3 = register(record(field("a", "int"), field("b", "int"), field("c", "int")));
    client.deleteSchemaVersion(SUBJECT, "2", false);

    SchemaProvenance ends = client.getProvenanceById(SUBJECT, v1, v3, true, false, null);
    assertThat(ends.getVersions()).extracting(ProvenanceVersion::getVersion)
        .containsExactly(1, 2, 3);
    assertThat(pids(ends.getVersions().get(2))).containsExactly(3, 2, 4);
    // Found where deleted versions are looked up, as a structural match looks them up.
    assertThat(client.getAllVersions(SUBJECT, true)).containsExactly(1, 2, 3);
    assertThat(client.getAllVersions(SUBJECT, false)).containsExactly(1, 3);
    assertThat(client.getSchemaMetadata(SUBJECT, 2, true).getId()).isEqualTo(v1 + 1);
  }

  @Test
  void aVersionAfterASoftDeletedLatestTakesTheNextNumber() throws Exception {
    // As in the registry: v2, soft-deleted, dropped b, so b is new at v3.
    register(record(field("a", "int"), field("b", "int")));
    register(record(field("a", "int")));
    client.deleteSchemaVersion(SUBJECT, "2");
    register(record(field("a", "int"), field("b", "string")));

    assertThat(client.getAllVersions(SUBJECT, true)).containsExactly(1, 2, 3);
    SchemaProvenance all =
        client.getProvenanceByVersion(SUBJECT, "1", "latest", true, false, null);
    assertThat(pids(all.getVersions().get(2))).containsExactly(1, 3);
  }

  @Test
  void aSoftDeletedSchemaLookedUpThenRegisteredAgainIsANewVersion() throws Exception {
    String ab = record(field("a", "int"), field("b", "int"));
    int v1 = register(ab);
    register(record(field("a", "int")));
    register(record(field("a", "int"), field("c", "int")));
    client.deleteSchemaVersion(SUBJECT, "1");

    // As a consumer's lookups find it: soft-deleted, with no id but still version 1.
    assertCode(404, 40403, () -> client.getId(SUBJECT, new AvroSchema(ab)));
    assertThat(client.getVersion(SUBJECT, new AvroSchema(ab))).isEqualTo(1);
    assertThat(register(ab)).isEqualTo(v1);
    assertThat(client.getAllVersions(SUBJECT, false)).containsExactly(2, 3, 4);
  }

  @Test
  void aDeletedSubjectOrAResetForgetsItsSoftDeletedVersions() throws Exception {
    // As the base mock forgets the subject and its schemas: nothing stale is left to resolve.
    register(record(field("a", "int")));
    register(record(field("a", "int"), field("b", "int")));
    client.deleteSchemaVersion(SUBJECT, "1", false);
    client.deleteSubject(SUBJECT, false);
    assertCode(404, 40401, () -> client.getAllVersions(SUBJECT, true));

    register(record(field("a", "int"), field("c", "int")));
    register(record(field("a", "int"), field("d", "int")));
    client.deleteSchemaVersion(SUBJECT, "1", false);
    client.reset();
    assertCode(404, 40401, () -> client.getAllVersions(SUBJECT, true));
  }

  @Test
  void aLowerCapIsPagedAndThePagesStitchToTheWholeHistory() throws Exception {
    ProvenanceMockSchemaRegistryClient capped = new ProvenanceMockSchemaRegistryClient(3);
    String[] history = {
        record(field("a", "int"), field("b", "int")),
        record(field("b", "int")),
        record(field("a", "int"), field("b", "int"), field("c", "int")),
        record(field("b", "int"), field("c", "int"), field("d", "int")),
        record(field("a", "int"), field("c", "int"), field("d", "int"))};
    for (String schema : history) {
      capped.register(SUBJECT, new AvroSchema(schema));
      register(schema);
    }
    assertCode(422, 42219,
        () -> capped.getProvenanceByVersion(SUBJECT, "1", "5", true, false, null));

    // The pages share version 3, whose fields carry the first page's pids into the second.
    List<ProvenanceVersion> first =
        capped.getProvenanceByVersion(SUBJECT, "1", "3", true, false, null).getVersions();
    List<ProvenanceVersion> second =
        capped.getProvenanceByVersion(SUBJECT, "3", "5", true, false, null).getVersions();
    Map<Integer, String> carried = new HashMap<>();
    for (int i = 0; i < second.get(0).getFields().size(); i++) {
      carried.put(second.get(0).getFields().get(i).getPid(),
          "p" + first.get(2).getFields().get(i).getPid());
    }
    List<List<String>> stitched = new ArrayList<>();
    first.forEach(v -> stitched.add(labels(v, pid -> "p" + pid)));
    second.subList(1, 3).forEach(v ->
        stitched.add(labels(v, pid -> carried.getOrDefault(pid, "q" + pid))));

    List<List<String>> whole = new ArrayList<>();
    client.getProvenanceByVersion(SUBJECT, "1", "5", true, false, null).getVersions()
        .forEach(v -> whole.add(labels(v, pid -> "p" + pid)));
    assertThat(canonical(stitched)).isEqualTo(canonical(whole));
  }

  @Test
  void aSoftDeletedSchemaRegisteredAgainIsANewVersionCarryingItsId() throws Exception {
    int v1 = register(record(field("a", "int"), field("b", "int")));
    register(record(field("b", "int")));
    int v3 = register(record(field("a", "int"), field("b", "int"), field("c", "int")));
    client.deleteSchemaVersion(SUBJECT, "1");

    // As the registry: v1's text again is version 4, and v1's id now names it (latest wins).
    assertThat(register(record(field("a", "int"), field("b", "int")))).isEqualTo(v1);
    assertThat(client.getAllVersions(SUBJECT, true)).containsExactly(1, 2, 3, 4);
    assertThat(client.getProvenanceById(SUBJECT, v1, v3, false, false, null).getVersions())
        .extracting(ProvenanceVersion::getVersion).containsExactly(3, 4);
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

  private static List<String> labels(ProvenanceVersion version, IntFunction<String> label) {
    return version.getFields().stream().map(f -> label.apply(f.getPid()))
        .collect(Collectors.toList());
  }

  // Relabels identities in order of first appearance, so two numberings of one history agree.
  private static List<List<String>> canonical(List<List<String>> versions) {
    Map<String, String> renamed = new HashMap<>();
    return versions.stream().map(v -> v.stream()
            .map(l -> renamed.computeIfAbsent(l, k -> "id" + renamed.size()))
            .collect(Collectors.toList()))
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
