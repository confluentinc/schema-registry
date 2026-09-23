/*
 * Copyright 2026 Confluent Inc.
 *
 * Licensed under the Confluent Community License (the "License"); you may not use
 * this file except in compliance with the License.  You may obtain a copy of the
 * License at
 *
 * http://www.confluent.io/confluent-community-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OF ANY KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations under the License.
 */

package io.confluent.kafka.schemaregistry.rest;

import org.apache.kafka.common.header.internals.RecordHeaders;
import org.apache.avro.generic.GenericRecordBuilder;
import org.apache.avro.generic.GenericRecord;
import java.util.Map;
import java.util.HashMap;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.CachedSchemaRegistryClient;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

import com.fasterxml.jackson.core.type.TypeReference;
import io.confluent.kafka.schemaregistry.RestApp;
import io.confluent.kafka.schemaregistry.avro.AvroSchema;
import io.confluent.kafka.schemaregistry.protobuf.ProtobufSchema;
import io.confluent.kafka.schemaregistry.client.rest.RestService;
import io.confluent.kafka.schemaregistry.client.rest.entities.ProvenanceField;
import io.confluent.kafka.schemaregistry.client.rest.entities.ProvenanceVersion;
import io.confluent.kafka.schemaregistry.client.rest.entities.SchemaProvenance;
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;
import io.confluent.kafka.schemaregistry.rest.exceptions.Errors;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

/**
 * The provenance endpoint, over a live registry: the shape of its response, both ways of naming
 * a range, how interior and soft-deleted versions take part, and each error it can return.
 */
@Tag("IntegrationTest")
public abstract class RestApiProvenanceTest {

  private static final String SUBJECT = "orders-value";
  private static final TypeReference<SchemaProvenance> PROVENANCE =
      new TypeReference<SchemaProvenance>() {
      };

  protected RestApp restApp = null;

  public void setRestApp(RestApp restApp) {
    this.restApp = restApp;
  }

  @Test
  public void aRenameKeepsItsIdAndADroppedColumnRetiresIt() throws Exception {
    int v1 = register(SUBJECT, record(field("id", "int"), field("name", "string"),
        field("region", "string")));
    int v2 = register(SUBJECT, record(field("id", "int"),
        "{\"name\":\"full_name\",\"type\":\"string\",\"aliases\":[\"name\"]}",
        "{\"name\":\"tier\",\"type\":\"string\",\"default\":\"standard\"}"));

    SchemaProvenance provenance = byVersion(SUBJECT, "1", "2", false, true);

    assertEquals(SUBJECT, provenance.getSubject());
    assertEquals(Arrays.asList(1, 2), versions(provenance));
    assertEquals(Arrays.asList(v1, v2), schemaIds(provenance));
    assertEquals(Arrays.asList(1, 2, 3), pids(provenance.getVersions().get(0)));
    // full_name keeps name's pid; tier is new, and region's 3 is never handed out again.
    assertEquals(Arrays.asList(1, 2, 4), pids(provenance.getVersions().get(1)));
    ProvenanceField tier = provenance.getVersions().get(1).getFields().get(2);
    assertEquals(Collections.singletonList("tier"), tier.getNames());
  }

  @Test
  public void aRangeNamedBySchemaIdComesBackInVersionOrder() throws Exception {
    int v1 = register(SUBJECT, record(field("id", "int")));
    int v2 = register(SUBJECT, record(field("id", "int"), field("name", "string")));

    // Named newer-first, as a writer newer than its reader would be.
    SchemaProvenance provenance = restApp.restClient.getProvenanceById(
        RestService.DEFAULT_REQUEST_PROPERTIES, SUBJECT, v2, v1, false, false, false, null);

    assertEquals(Arrays.asList(1, 2), versions(provenance));
    assertEquals(Arrays.asList(v1, v2), schemaIds(provenance));
  }

  @Test
  public void anInteriorVersionDecidesIdsEvenWhenNotReturned() throws Exception {
    register(SUBJECT, record(field("a", "int"), field("b", "int")));
    register(SUBJECT, record(field("b", "int")));
    // Not identical to v1, which the registry would deduplicate back to v1 instead of adding v3.
    register(SUBJECT, record(field("a", "int"), field("b", "int"), field("c", "int")));

    SchemaProvenance ends = byVersion(SUBJECT, "1", "3", false, false);
    // a was dropped at v2, so at v3 it is a new column, though only v1 and v3 are returned.
    assertEquals(Arrays.asList(1, 3), versions(ends));
    assertEquals(Arrays.asList(1, 2), pids(ends.getVersions().get(0)));
    assertEquals(Arrays.asList(3, 2, 4), pids(ends.getVersions().get(1)));

    SchemaProvenance all = byVersion(SUBJECT, "1", "3", true, false);
    assertEquals(Arrays.asList(1, 2, 3), versions(all));
  }

  @Test
  public void aRangeIsComputedFromItsOwnFirstVersion() throws Exception {
    register(SUBJECT, record(field("a", "int")));
    register(SUBJECT, record(field("a", "int"), field("b", "int")));
    register(SUBJECT, record(field("a", "int"), field("b", "int"), field("c", "int")));

    // v1 plays no part: ids start at v2, and pair v2 with v3 exactly as the whole history would.
    SchemaProvenance later = byVersion(SUBJECT, "2", "3", false, false);
    assertEquals(Arrays.asList(1, 2), pids(later.getVersions().get(0)));
    assertEquals(Arrays.asList(1, 2, 3), pids(later.getVersions().get(1)));
  }

  @Test
  public void namesAreLeftOutUnlessVerbose() throws Exception {
    register(SUBJECT, record(field("id", "int")));

    ProvenanceField member =
        byVersion(SUBJECT, "1", "latest", false, false).getVersions().get(0).getFields().get(0);

    assertNull(member.getNames());
  }

  @Test
  public void aSoftDeletedVersionIsStillAddressableById() throws Exception {
    int v1 = register(SUBJECT, record(field("id", "int")));
    int v2 = register(SUBJECT, record(field("id", "int"), field("name", "string")));
    restApp.restClient.deleteSchemaVersion(RestService.DEFAULT_REQUEST_PROPERTIES, SUBJECT, "1");

    // Old records on a topic are exactly the ones written under since-deleted schemas.
    SchemaProvenance provenance = restApp.restClient.getProvenanceById(
        RestService.DEFAULT_REQUEST_PROPERTIES, SUBJECT, v1, v2, false, false, false, null);
    assertEquals(Arrays.asList(1, 2), versions(provenance));
  }

  @Test
  public void latestSkipsASoftDeletedLatestVersion() throws Exception {
    register(SUBJECT, record(field("id", "int")));
    register(SUBJECT, record(field("id", "int"), field("name", "string")));
    restApp.restClient.deleteSchemaVersion(RestService.DEFAULT_REQUEST_PROPERTIES, SUBJECT, "2");

    assertEquals(Collections.singletonList(1),
        versions(byVersion(SUBJECT, "latest", "latest", false, false)));
  }

  @Test
  public void aSubjectInAContextIsReadInThatContext() throws Exception {
    String qualified = ":.ctx:" + SUBJECT;
    register(qualified, record(field("id", "int")));
    register(qualified, record(field("id", "int"), field("name", "string")));

    SchemaProvenance provenance = byVersion(qualified, "1", "2", false, false);

    assertEquals(qualified, provenance.getSubject());
    assertEquals(Arrays.asList(1, 2), versions(provenance));
  }

  @Test
  public void aNewRegistrationIsSeenAtOnce() throws Exception {
    register(SUBJECT, record(field("id", "int")));
    register(SUBJECT, record(field("id", "int"), field("name", "string")));
    byVersion(SUBJECT, "1", "2", false, false);

    // The history is cached by its own contents, so a new version is a new key, not a stale hit.
    register(SUBJECT, record(field("id", "int"), field("name", "string"), field("x", "int")));
    SchemaProvenance provenance = byVersion(SUBJECT, "1", "latest", false, false);

    assertEquals(Arrays.asList(1, 3), versions(provenance));
    assertEquals(Arrays.asList(1, 2, 3), pids(provenance.getVersions().get(1)));
  }

  // -------------------------------------------------------------------------------------------
  // Errors
  // -------------------------------------------------------------------------------------------

  @Test
  public void anUnknownSubjectIsNotFound() {
    assertError(404, Errors.SUBJECT_NOT_FOUND_ERROR_CODE,
        () -> byVersion("nope-value", "1", "1", false, false));
  }

  @Test
  public void anUnknownVersionIsNotFound() throws Exception {
    register(SUBJECT, record(field("id", "int")));
    assertError(404, Errors.VERSION_NOT_FOUND_ERROR_CODE,
        () -> byVersion(SUBJECT, "1", "9", false, false));
  }

  @Test
  public void aSchemaIdFromAnotherSubjectIsNotFoundDistinctly() throws Exception {
    int v1 = register(SUBJECT, record(field("id", "int")));
    int other = register("other-value", record(field("x", "string")));

    // Distinct from every other 404, so a reader knows to stop asking and read without it.
    assertError(404, Errors.SCHEMA_ID_NOT_IN_SUBJECT_ERROR_CODE,
        () -> restApp.restClient.getProvenanceById(
            RestService.DEFAULT_REQUEST_PROPERTIES, SUBJECT, other, v1, false, false, false, null));
  }

  @Test
  public void anInvalidVersionIsRejected() throws Exception {
    register(SUBJECT, record(field("id", "int")));
    assertError(422, Errors.INVALID_VERSION_ERROR_CODE,
        () -> byVersion(SUBJECT, "1", "abc", false, false));
  }

  @Test
  public void aRecursiveSchemaHasNoProvenance() throws Exception {
    register(SUBJECT, "{\"type\":\"record\",\"name\":\"Node\",\"fields\":["
        + "{\"name\":\"next\",\"type\":[\"null\",\"Node\"],\"default\":null}]}");
    assertError(422, Errors.RECURSIVE_SCHEMA_ERROR_CODE,
        () -> byVersion(SUBJECT, "1", "1", false, false));
  }

  @Test
  public void aBrokenVersionOutsideTheRangeDoesNotFailIt() throws Exception {
    register(SUBJECT, "{\"type\":\"record\",\"name\":\"Node\",\"fields\":["
        + "{\"name\":\"next\",\"type\":[\"null\",\"Node\"],\"default\":null}]}");
    register(SUBJECT, record(field("a", "int")));
    register(SUBJECT, record(field("a", "int"), field("b", "int")));

    assertEquals(Arrays.asList(2, 3), versions(byVersion(SUBJECT, "2", "3", false, false)));
  }

  @Test
  public void aRangeMustBeNamedOneWayAndByBothEnds() throws Exception {
    register(SUBJECT, record(field("id", "int")));
    for (String query : Arrays.asList(
        "",                                     // neither
        "?fromVersion=1",                       // one end
        "?fromId=1",                            // one end
        "?fromVersion=1&toVersion=1&fromId=1&toId=1")) {  // both ways
      assertError(422, Errors.INVALID_PROVENANCE_REQUEST_ERROR_CODE,
          () -> restApp.restClient.httpRequest("/subjects/" + SUBJECT + "/provenance" + query,
              "GET", null, RestService.DEFAULT_REQUEST_PROPERTIES, PROVENANCE));
    }
  }

  @Test
  public void includeMultipleMessagesRootsAProtobufSubjectAtEveryMessage() throws Exception {
    String proto = "syntax = \"proto3\";\npackage io.confluent;\n"
        + "message Order { int32 id = 1; }\nmessage Refund { int32 amount = 1; }\n";
    int id = restApp.restClient.registerSchema(
        proto, ProtobufSchema.TYPE, Collections.emptyList(), SUBJECT).getId();

    SchemaProvenance plain = restApp.restClient.getProvenanceById(
        RestService.DEFAULT_REQUEST_PROPERTIES, SUBJECT, id, id, false, true, false, null);
    SchemaProvenance multi = restApp.restClient.getProvenanceById(
        RestService.DEFAULT_REQUEST_PROPERTIES, SUBJECT, id, id, false, true, true, null);

    assertEquals(Arrays.asList(Arrays.asList(0)), paths(plain.getVersions().get(0)));
    assertEquals(Arrays.asList(Arrays.asList(0), Arrays.asList(0, 0), Arrays.asList(1),
        Arrays.asList(1, 0)), paths(multi.getVersions().get(0)));
    assertEquals(Arrays.asList("io.confluent.Refund", "amount"),
        multi.getVersions().get(0).getFields().get(3).getNames());
  }

  @Test
  public void includeMultipleMessagesIsIgnoredForOtherFormats() throws Exception {
    int id = register(SUBJECT, record(field("id", "int")));

    assertEquals(
        restApp.restClient.getProvenanceById(
            RestService.DEFAULT_REQUEST_PROPERTIES, SUBJECT, id, id, false, true, false, null),
        restApp.restClient.getProvenanceById(
            RestService.DEFAULT_REQUEST_PROPERTIES, SUBJECT, id, id, false, true, true, null));
  }

  @Test
  public void theResponseNamesTheAlgorithmThatComputedIt() throws Exception {
    int id = register(SUBJECT, record(field("id", "int")));

    SchemaProvenance latest = restApp.restClient.getProvenanceById(
        RestService.DEFAULT_REQUEST_PROPERTIES, SUBJECT, id, id, false, false, false, null);
    SchemaProvenance v1 = restApp.restClient.getProvenanceById(
        RestService.DEFAULT_REQUEST_PROPERTIES, SUBJECT, id, id, false, false, false, "v1");

    assertEquals("v1", latest.getAlgorithm());
    assertEquals(latest, v1);
  }

  @Test
  public void anUnknownAlgorithmIsRejected() throws Exception {
    int id = register(SUBJECT, record(field("id", "int")));

    assertError(422, Errors.UNKNOWN_PROVENANCE_ALGORITHM_ERROR_CODE,
        () -> restApp.restClient.getProvenanceById(
            RestService.DEFAULT_REQUEST_PROPERTIES, SUBJECT, id, id, false, false, false, "v9"));
  }

  @Test
  public void aSoftDeletedReaderIsStillProjectedByTheDeserializer() throws Exception {
    assertEquals("new", readReAddedColumn(true));
  }

  @Test
  public void aRegisteredReaderIsProjectedByTheDeserializer() throws Exception {
    assertEquals("new", readReAddedColumn(false));
  }

  private String readReAddedColumn(boolean deleteReader) throws Exception {
    String v1 = record(field("id", "int"), field("name", "string"));
    String v2 = record(field("id", "int"));
    String v3 = record(field("id", "int"),
        "{\"name\":\"name\",\"type\":\"string\",\"default\":\"new\"}");
    register(SUBJECT, v1);
    register(SUBJECT, v2);
    register(SUBJECT, v3);
    if (deleteReader) {
      restApp.restClient.deleteSchemaVersion(RestService.DEFAULT_REQUEST_PROPERTIES, SUBJECT, "3");
    }

    SchemaRegistryClient client = new CachedSchemaRegistryClient(restApp.restClient, 10);
    Map<String, Object> config = new HashMap<>();
    config.put("schema.registry.url", "bogus");
    config.put("auto.register.schemas", false);
    config.put("use.latest.version", false);
    org.apache.avro.Schema writer = new org.apache.avro.Schema.Parser().parse(v1);
    GenericRecord record = new GenericRecordBuilder(writer).set("id", 7).set("name", "ada").build();
    config.put("use.schema.id", client.getId(SUBJECT, new AvroSchema(v1)));
    byte[] bytes = new KafkaAvroSerializer(client, config).serialize("orders", record);
    config.remove("use.schema.id");
    config.put("use.provenance", "v1");

    // v3 re-adds name, dropped at v2: with provenance it is a new column and reads its default.
    GenericRecord read = (GenericRecord) new KafkaAvroDeserializer(client, config)
        .deserializeWithSchema("orders", new RecordHeaders(), bytes,
            new org.apache.avro.Schema.Parser().parse(v3)).getValue();
    return read.get("name").toString();
  }

  // -------------------------------------------------------------------------------------------
  // Helpers
  // -------------------------------------------------------------------------------------------

  private int register(String subject, String avro) throws Exception {
    return restApp.restClient.registerSchema(
        avro, AvroSchema.TYPE, Collections.emptyList(), subject).getId();
  }

  private SchemaProvenance byVersion(String subject, String from, String to,
      boolean includeInterior, boolean verbose) throws Exception {
    return restApp.restClient.getProvenanceByVersion(
        RestService.DEFAULT_REQUEST_PROPERTIES, subject, from, to, includeInterior, verbose, false,
        null);
  }

  private static void assertError(int status, int code, ThrowingRunnable call) {
    RestClientException e = assertThrows(RestClientException.class, call::run);
    assertEquals(status, e.getStatus(), e.getMessage());
    assertEquals(code, e.getErrorCode(), e.getMessage());
  }

  private static List<Integer> versions(SchemaProvenance provenance) {
    return provenance.getVersions().stream().map(ProvenanceVersion::getVersion)
        .collect(Collectors.toList());
  }

  private static List<Integer> schemaIds(SchemaProvenance provenance) {
    return provenance.getVersions().stream().map(ProvenanceVersion::getId)
        .collect(Collectors.toList());
  }

  private static List<List<Integer>> paths(ProvenanceVersion version) {
    return version.getFields().stream().map(ProvenanceField::getPath)
        .collect(Collectors.toList());
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
  private interface ThrowingRunnable {
    void run() throws Exception;
  }
}
