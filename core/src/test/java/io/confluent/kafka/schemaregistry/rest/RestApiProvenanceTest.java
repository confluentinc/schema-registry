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
import io.confluent.kafka.serializers.provenance.ReaderSchema;
import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.CachedSchemaRegistryClient;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import com.fasterxml.jackson.core.type.TypeReference;
import io.confluent.kafka.schemaregistry.RestApp;
import io.confluent.kafka.schemaregistry.avro.AvroSchema;
import io.confluent.kafka.schemaregistry.json.JsonSchema;
import io.confluent.kafka.schemaregistry.protobuf.ProtobufSchema;
import io.confluent.kafka.schemaregistry.client.rest.RestService;
import io.confluent.kafka.schemaregistry.client.rest.entities.Metadata;
import io.confluent.kafka.schemaregistry.client.rest.entities.ProvenanceField;
import io.confluent.kafka.schemaregistry.client.rest.entities.ProvenanceVersion;
import io.confluent.kafka.schemaregistry.client.rest.entities.SchemaProvenance;
import io.confluent.kafka.schemaregistry.client.rest.entities.requests.RegisterSchemaResponse;
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;
import io.confluent.kafka.schemaregistry.rest.exceptions.Errors;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.logging.Handler;
import java.util.logging.LogRecord;
import java.util.logging.Logger;
import java.util.stream.Collectors;
import io.confluent.kafka.schemaregistry.client.rest.entities.Schema;
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

    SchemaProvenance provenance = byVersion(SUBJECT, "1", "2", false);

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
  public void eachLocationAndTheRootCarryTheirKind() throws Exception {
    register(SUBJECT, record(field("id", "int"),
        "{\"name\":\"tags\",\"type\":{\"type\":\"array\",\"items\":[\"int\",\"string\"]}}",
        "{\"name\":\"m\",\"type\":{\"type\":\"map\",\"values\":{\"type\":\"record\","
            + "\"name\":\"In\",\"fields\":[{\"name\":\"i\",\"type\":\"int\"}]}}}"));

    ProvenanceVersion version = byVersion(SUBJECT, "1", "1", false).getVersions().get(0);

    assertEquals("STRUCT", version.getKind());
    assertEquals(Arrays.asList("SCALAR", "ARRAY<UNION>", "SCALAR", "SCALAR",
        "MAP<SCALAR, STRUCT>", "SCALAR"), version.getFields().stream()
        .map(ProvenanceField::getKind).collect(Collectors.toList()));
  }

  @Test
  public void aRangeNamedBySchemaIdComesBackInVersionOrder() throws Exception {
    int v1 = register(SUBJECT, record(field("id", "int")));
    int v2 = register(SUBJECT, record(field("id", "int"), field("name", "string")));

    // Named newer-first, as a writer newer than its reader would be.
    SchemaProvenance provenance = restApp.restClient.getProvenanceById(
        RestService.DEFAULT_REQUEST_PROPERTIES, SUBJECT, v2, v1, false, false, null);

    assertEquals(Arrays.asList(1, 2), versions(provenance));
    assertEquals(Arrays.asList(v1, v2), schemaIds(provenance));
  }

  @Test
  public void anInteriorVersionDecidesIdsEvenWhenNotReturned() throws Exception {
    register(SUBJECT, record(field("a", "int"), field("b", "int")));
    register(SUBJECT, record(field("b", "int")));
    // Not identical to v1, which the registry would deduplicate back to v1 instead of adding v3.
    register(SUBJECT, record(field("a", "int"), field("b", "int"), field("c", "int")));

    SchemaProvenance ends = byVersion(SUBJECT, "1", "3", false);
    // a was dropped at v2, so at v3 it is a new column, though only v1 and v3 are returned.
    assertEquals(Arrays.asList(1, 3), versions(ends));
    assertEquals(Arrays.asList(1, 2), pids(ends.getVersions().get(0)));
    assertEquals(Arrays.asList(3, 2, 4), pids(ends.getVersions().get(1)));

    SchemaProvenance all = byVersion(SUBJECT, "1", "3", true);
    assertEquals(Arrays.asList(1, 2, 3), versions(all));
  }

  @Test
  public void aRangeWithItsInteriorCoversAtMostTheConfiguredVersions() throws Exception {
    // The cluster test caps it at 3: four versions are read in two ranges sharing version 3,
    // whose locations join the two ranges' ids. The ends alone are never capped.
    register(SUBJECT, record(field("a", "int")));
    register(SUBJECT, record(field("a", "int"), field("b", "int")));
    register(SUBJECT, record(field("a", "int"), field("b", "int"), field("c", "int")));
    register(SUBJECT, record(field("a", "int"), field("b", "int"), field("c", "int"),
        field("d", "int")));

    assertError(422, 42219, () -> byVersion(SUBJECT, "1", "4", true));
    SchemaProvenance first = byVersion(SUBJECT, "1", "3", true);
    SchemaProvenance second = byVersion(SUBJECT, "3", "4", true);
    assertEquals(Arrays.asList(1, 2, 3), versions(first));
    assertEquals(Arrays.asList(3, 4), versions(second));
    assertEquals(Arrays.asList(1, 4), versions(byVersion(SUBJECT, "1", "4", false)));
  }

  @Test
  public void aRangeIsComputedFromItsOwnFirstVersion() throws Exception {
    register(SUBJECT, record(field("a", "int")));
    register(SUBJECT, record(field("a", "int"), field("b", "int")));
    register(SUBJECT, record(field("a", "int"), field("b", "int"), field("c", "int")));

    // v1 plays no part: ids start at v2, and pair v2 with v3 exactly as the whole history would.
    SchemaProvenance later = byVersion(SUBJECT, "2", "3", false);
    assertEquals(Arrays.asList(1, 2), pids(later.getVersions().get(0)));
    assertEquals(Arrays.asList(1, 2, 3), pids(later.getVersions().get(1)));
  }

  @Test
  public void namesAreAlwaysReturned() throws Exception {
    register(SUBJECT, record(field("id", "int")));

    ProvenanceField member =
        byVersion(SUBJECT, "1", "latest", false).getVersions().get(0).getFields().get(0);

    assertEquals(Collections.singletonList("id"), member.getNames());
  }

  @Test
  public void aSoftDeletedVersionIsStillAddressableById() throws Exception {
    int v1 = register(SUBJECT, record(field("id", "int")));
    int v2 = register(SUBJECT, record(field("id", "int"), field("name", "string")));
    restApp.restClient.deleteSchemaVersion(RestService.DEFAULT_REQUEST_PROPERTIES, SUBJECT, "1");

    // Old records on a topic are exactly the ones written under since-deleted schemas.
    SchemaProvenance provenance = restApp.restClient.getProvenanceById(
        RestService.DEFAULT_REQUEST_PROPERTIES, SUBJECT, v1, v2, false, false, null);
    assertEquals(Arrays.asList(1, 2), versions(provenance));
  }

  @Test
  public void latestSkipsASoftDeletedLatestVersion() throws Exception {
    register(SUBJECT, record(field("id", "int")));
    register(SUBJECT, record(field("id", "int"), field("name", "string")));
    restApp.restClient.deleteSchemaVersion(RestService.DEFAULT_REQUEST_PROPERTIES, SUBJECT, "2");

    assertEquals(Collections.singletonList(1),
        versions(byVersion(SUBJECT, "latest", "latest", false)));
  }

  @Test
  public void aSubjectInAContextIsReadInThatContext() throws Exception {
    String qualified = ":.ctx:" + SUBJECT;
    register(qualified, record(field("id", "int")));
    register(qualified, record(field("id", "int"), field("name", "string")));

    SchemaProvenance provenance = byVersion(qualified, "1", "2", false);

    assertEquals(qualified, provenance.getSubject());
    assertEquals(Arrays.asList(1, 2), versions(provenance));
  }

  @Test
  public void aNewRegistrationIsSeenAtOnce() throws Exception {
    register(SUBJECT, record(field("id", "int")));
    register(SUBJECT, record(field("id", "int"), field("name", "string")));
    byVersion(SUBJECT, "1", "2", false);

    // The history is cached by its own contents, so a new version is a new key, not a stale hit.
    register(SUBJECT, record(field("id", "int"), field("name", "string"), field("x", "int")));
    SchemaProvenance provenance = byVersion(SUBJECT, "1", "latest", false);

    assertEquals(Arrays.asList(1, 3), versions(provenance));
    assertEquals(Arrays.asList(1, 2, 3), pids(provenance.getVersions().get(1)));
  }

  // -------------------------------------------------------------------------------------------
  // Errors
  // -------------------------------------------------------------------------------------------

  @Test
  public void anUnknownSubjectIsNotFound() {
    assertError(404, Errors.SUBJECT_NOT_FOUND_ERROR_CODE,
        () -> byVersion("nope-value", "1", "1", false));
  }

  @Test
  public void anUnknownVersionIsNotFound() throws Exception {
    register(SUBJECT, record(field("id", "int")));
    assertError(404, Errors.VERSION_NOT_FOUND_ERROR_CODE,
        () -> byVersion(SUBJECT, "1", "9", false));
  }

  @Test
  public void aSchemaIdFromAnotherSubjectIsNotFoundDistinctly() throws Exception {
    int v1 = register(SUBJECT, record(field("id", "int")));
    int other = register("other-value", record(field("x", "string")));

    // Distinct from every other 404, so a reader knows to stop asking and read without it.
    assertError(404, Errors.SCHEMA_ID_NOT_IN_SUBJECT_ERROR_CODE,
        () -> restApp.restClient.getProvenanceById(
            RestService.DEFAULT_REQUEST_PROPERTIES, SUBJECT, other, v1, false, false, null));
  }

  @Test
  public void anInvalidVersionIsRejected() throws Exception {
    register(SUBJECT, record(field("id", "int")));
    assertError(422, Errors.INVALID_VERSION_ERROR_CODE,
        () -> byVersion(SUBJECT, "1", "abc", false));
  }

  @Test
  public void aHistoryWhoseAliasesDetermineNoSingleIdentityHasNoProvenance() throws Exception {
    register(SUBJECT, record(field("a", "int")));
    register(SUBJECT, record("{\"name\":\"b\",\"type\":\"int\",\"aliases\":[\"a\"]}",
        "{\"name\":\"c\",\"type\":\"int\",\"aliases\":[\"a\"]}"));
    assertError(422, Errors.AMBIGUOUS_PROVENANCE_ERROR_CODE,
        () -> byVersion(SUBJECT, "1", "2", false));
  }

  @Test
  public void aRecursiveSchemaHasNoProvenance() throws Exception {
    register(SUBJECT, "{\"type\":\"record\",\"name\":\"Node\",\"fields\":["
        + "{\"name\":\"next\",\"type\":[\"null\",\"Node\"],\"default\":null}]}");
    // Asked again, as the failure is not cached, and never logged by the cache holding it.
    List<LogRecord> logged = new CopyOnWriteArrayList<>();
    Handler handler = new Handler() {
      @Override
      public void publish(LogRecord record) {
        logged.add(record);
      }

      @Override
      public void flush() {
      }

      @Override
      public void close() {
      }
    };
    Logger caffeine = Logger.getLogger("com.github.benmanes.caffeine");
    caffeine.addHandler(handler);
    try {
      for (int i = 0; i < 2; i++) {
        assertError(422, Errors.RECURSIVE_SCHEMA_ERROR_CODE,
            () -> byVersion(SUBJECT, "1", "1", false));
      }
    } finally {
      caffeine.removeHandler(handler);
    }
    assertEquals(Collections.emptyList(), logged);
  }

  @Test
  public void aMapWithNoValueSchemaHasNoLogicalForm() throws Exception {
    restApp.restClient.registerSchema("{\"type\":\"object\",\"properties\":"
        + "{\"m\":{\"type\":\"object\",\"connect.type\":\"map\"}}}",
        JsonSchema.TYPE, Collections.emptyList(), SUBJECT);
    assertError(422, Errors.INVALID_SCHEMA_ERROR_CODE,
        () -> byVersion(SUBJECT, "1", "1", false));
  }

  @Test
  public void aBrokenVersionOutsideTheRangeDoesNotFailIt() throws Exception {
    register(SUBJECT, "{\"type\":\"record\",\"name\":\"Node\",\"fields\":["
        + "{\"name\":\"next\",\"type\":[\"null\",\"Node\"],\"default\":null}]}");
    register(SUBJECT, record(field("a", "int")));
    register(SUBJECT, record(field("a", "int"), field("b", "int")));

    assertEquals(Arrays.asList(2, 3), versions(byVersion(SUBJECT, "2", "3", false)));
  }

  @Test
  public void eachEndOfTheRangeMustBeNamedOnce() throws Exception {
    register(SUBJECT, record(field("id", "int")));
    for (String query : Arrays.asList(
        "",                                     // neither
        "?fromVersion=1",                       // one end
        "?fromId=1",                            // one end
        "?fromVersion=1&fromId=1&toVersion=1",  // one end both ways
        "?fromVersion=1&toVersion=1&fromId=1&toId=1")) {  // both ends both ways
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
        RestService.DEFAULT_REQUEST_PROPERTIES, SUBJECT, id, id, false, false, null);
    SchemaProvenance multi = restApp.restClient.getProvenanceById(
        RestService.DEFAULT_REQUEST_PROPERTIES, SUBJECT, id, id, false, true, null);

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
            RestService.DEFAULT_REQUEST_PROPERTIES, SUBJECT, id, id, false, false, null),
        restApp.restClient.getProvenanceById(
            RestService.DEFAULT_REQUEST_PROPERTIES, SUBJECT, id, id, false, true, null));
  }

  @Test
  public void theResponseNamesTheAlgorithmThatComputedIt() throws Exception {
    int id = register(SUBJECT, record(field("id", "int")));

    SchemaProvenance latest = restApp.restClient.getProvenanceById(
        RestService.DEFAULT_REQUEST_PROPERTIES, SUBJECT, id, id, false, false, null);
    SchemaProvenance v1 = restApp.restClient.getProvenanceById(
        RestService.DEFAULT_REQUEST_PROPERTIES, SUBJECT, id, id, false, false, "v1");
    SchemaProvenance named = restApp.restClient.getProvenanceById(
        RestService.DEFAULT_REQUEST_PROPERTIES, SUBJECT, id, id, false, false, "latest");

    assertEquals("v1", latest.getAlgorithm());
    assertEquals(latest, v1);
    assertEquals(latest, named);
  }

  @Test
  public void anUnknownAlgorithmIsRejected() throws Exception {
    int id = register(SUBJECT, record(field("id", "int")));

    assertError(422, Errors.UNKNOWN_PROVENANCE_ALGORITHM_ERROR_CODE,
        () -> restApp.restClient.getProvenanceById(
            RestService.DEFAULT_REQUEST_PROPERTIES, SUBJECT, id, id, false, false, "v9"));
  }

  @Test
  public void dynamicComputesAsV1AndSaysSo() throws Exception {
    register(SUBJECT, record(field("id", "int"), field("name", "string")));
    register(SUBJECT, record(field("id", "int"),
        "{\"name\":\"full_name\",\"type\":\"string\",\"aliases\":[\"name\"]}"));

    SchemaProvenance v1 = restApp.restClient.getProvenanceByVersion(
        RestService.DEFAULT_REQUEST_PROPERTIES, SUBJECT, "1", "2", true, false, "v1");
    SchemaProvenance dynamic = restApp.restClient.getProvenanceByVersion(
        RestService.DEFAULT_REQUEST_PROPERTIES, SUBJECT, "1", "2", true, false, "dynamic");

    // Every version is registered after v1's epoch, so each transition is matched by v1.
    assertEquals("dynamic", dynamic.getAlgorithm());
    assertEquals(v1.getVersions(), dynamic.getVersions());
  }

  @Test
  public void eachEndOfTheRangeMayBeNamedEitherWay() throws Exception {
    int v1 = register(SUBJECT, record(field("id", "int")));
    int v2 = register(SUBJECT, record(field("id", "int"), field("name", "string")));

    SchemaProvenance toVersion = restApp.restClient.getProvenanceToVersion(
        RestService.DEFAULT_REQUEST_PROPERTIES, SUBJECT, v1, 2, false, false, null);
    SchemaProvenance fromVersion = restApp.restClient.httpRequest("/subjects/" + SUBJECT
        + "/provenance?fromVersion=2&toId=" + v1, "GET", null,
        RestService.DEFAULT_REQUEST_PROPERTIES, PROVENANCE);

    assertEquals(Arrays.asList(1, 2), versions(toVersion));
    assertEquals(Arrays.asList(v1, v2), schemaIds(toVersion));
    assertEquals(Arrays.asList(1, 2), versions(fromVersion));
  }

  @Test
  public void aReaderPinnedByIdOrVersionIsThatVersionAfterARollback() throws Exception {
    // A rollback: v1's schema registered again with version -1 is v4, equal to v1 but for its
    // confluent:version, so under a new id. note was dropped at v3, so it is new at v4.
    String v1 = record(field("id", "int"),
        "{\"name\":\"note\",\"type\":\"string\",\"default\":\"\"}");
    String v2 = record(field("id", "int"),
        "{\"name\":\"note\",\"type\":\"string\",\"default\":\"\"}",
        "{\"name\":\"extra\",\"type\":\"int\",\"default\":0}");
    int v1Id = register(SUBJECT, v1);
    int v2Id = register(SUBJECT, v2);
    register(SUBJECT, record(field("id", "int")));
    RegisterSchemaResponse v4 = restApp.restClient.registerSchema(
        v1, AvroSchema.TYPE, Collections.emptyList(), SUBJECT, -1, -1);
    assertEquals(4, v4.getVersion());
    assertNotEquals(v1Id, v4.getId());

    SchemaRegistryClient client = new CachedSchemaRegistryClient(restApp.restClient, 10);
    Map<String, Object> config = new HashMap<>();
    config.put("schema.registry.url", "bogus");
    config.put("auto.register.schemas", false);
    config.put("use.schema.id", v2Id);
    org.apache.avro.Schema writer = new org.apache.avro.Schema.Parser().parse(v2);
    byte[] bytes = new KafkaAvroSerializer(client, config).serialize("orders",
        new GenericRecordBuilder(writer).set("id", 7).set("note", "ada").set("extra", 1).build());
    config.remove("use.schema.id");
    config.put("provenance.algorithm", "v1");
    KafkaAvroDeserializer deserializer = new KafkaAvroDeserializer(client, config);
    // Flink's reader: the pinned v1 with other metadata merged on, registered nowhere.
    AvroSchema merged = new AvroSchema(v1).copy(
        new Metadata(null, Collections.singletonMap("owner", "flink"), null), null);

    GenericRecord byStructure = (GenericRecord) deserializer.deserializeWithReaderSchema(
        "orders", new RecordHeaders(), bytes, w -> ReaderSchema.of(merged), false).getValue();
    GenericRecord byId = (GenericRecord) deserializer.deserializeWithReaderSchema("orders",
        new RecordHeaders(), bytes, w -> ReaderSchema.of(merged, v1Id), false).getValue();
    GenericRecord byVersion = (GenericRecord) deserializer.deserializeWithReaderSchema("orders",
        new RecordHeaders(), bytes, w -> ReaderSchema.of(merged, SUBJECT, 1), false).getValue();
    assertEquals("", byStructure.get("note").toString());
    assertEquals("ada", byId.get("note").toString());
    assertEquals("ada", byVersion.get("note").toString());
  }

  @Test
  public void aSoftDeletedReaderIsStillProjectedByTheDeserializer() throws Exception {
    assertEquals("new", readReAddedColumn(true, "v1"));
  }

  @Test
  public void aRegisteredReaderIsProjectedByTheDeserializer() throws Exception {
    assertEquals("new", readReAddedColumn(false, "v1"));
  }

  @Test
  public void aDeserializerProjectsUnderDynamic() throws Exception {
    assertEquals("new", readReAddedColumn(false, "dynamic"));
  }

  private String readReAddedColumn(boolean deleteReader, String algorithm) throws Exception {
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
    config.put("provenance.algorithm", algorithm);

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
      boolean includeInterior) throws Exception {
    return restApp.restClient.getProvenanceByVersion(
        RestService.DEFAULT_REQUEST_PROPERTIES, subject, from, to, includeInterior, false,
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
