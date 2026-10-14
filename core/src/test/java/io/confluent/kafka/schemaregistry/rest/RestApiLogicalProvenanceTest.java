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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.confluent.kafka.schemaregistry.CompatibilityLevel;
import io.confluent.kafka.schemaregistry.RestApp;
import io.confluent.kafka.schemaregistry.avro.AvroSchema;
import io.confluent.kafka.schemaregistry.client.rest.RestService;
import io.confluent.kafka.schemaregistry.client.rest.entities.Metadata;
import io.confluent.kafka.schemaregistry.client.rest.entities.ProvenanceVersion;
import io.confluent.kafka.schemaregistry.client.rest.entities.SchemaProvenance;
import io.confluent.kafka.schemaregistry.client.rest.entities.requests.ConfigUpdateRequest;
import io.confluent.kafka.schemaregistry.client.rest.entities.requests.RegisterSchemaRequest;
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;
import io.confluent.kafka.schemaregistry.json.JsonSchema;
import io.confluent.kafka.schemaregistry.protobuf.ProtobufSchema;
import io.confluent.kafka.schemaregistry.rest.exceptions.Errors;
import io.confluent.kafka.schemaregistry.storage.SchemaKey;
import io.confluent.kafka.schemaregistry.storage.SchemaValue;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

/**
 * {@code compatible} on the provenance endpoint: what registration under LOGICAL will say once
 * LOGICAL pairs columns by provenance check id, while registration and the compatibility checks
 * still pair by name. A rename continues a column; a field given a new identity is a deletion and
 * an addition.
 */
@Tag("IntegrationTest")
public abstract class RestApiLogicalProvenanceTest {

  protected RestApp restApp = null;

  public void setRestApp(RestApp restApp) {
    this.restApp = restApp;
  }

  // --- Renames continue ------------------------------------------------------------------------

  @Test
  public void testAnAvroRenameByAliasIsCompatibleByProvenance() throws Exception {
    String subject = "avro_rename";
    String renamed = avro(field("b", "\"string\"", "\"aliases\":[\"a\"]"));
    history(subject, AvroSchema.TYPE, avro(field("a", "\"string\"")));
    assertNameVerdict(subject, AvroSchema.TYPE, renamed, CompatibilityLevel.BACKWARD,
        "FIELD_DELETED");
    register(subject, AvroSchema.TYPE, renamed);
    logical(subject, CompatibilityLevel.BACKWARD);
    assertFlag(subject, 2, null);
  }

  @Test
  public void testAProtobufRenameKeepingItsNumberIsCompatibleByProvenance() throws Exception {
    // JSON has no renames: a property's name is its identity.
    String subject = "proto_rename";
    String renamed = proto("p", "string b = 2;");
    history(subject, ProtobufSchema.TYPE, proto("p", "string a = 2;"));
    assertNameVerdict(subject, ProtobufSchema.TYPE, renamed, CompatibilityLevel.BACKWARD,
        "FIELD_DELETED");
    register(subject, ProtobufSchema.TYPE, renamed);
    logical(subject, CompatibilityLevel.BACKWARD);
    assertFlag(subject, 2, null);
  }

  @Test
  public void testARenameAfterASoftDeletedLatestIsCompatibleByProvenance() throws Exception {
    // v=2, soft-deleted, renamed a to b; v=3 renames b to c (aliasing a too, so that natively
    // it reads v=1 as well) and continues it.
    String subject = "avro_rename_soft_deleted";
    String renamed = avro(field("c", "\"string\"", "\"aliases\":[\"b\",\"a\"]"));
    history(subject, AvroSchema.TYPE, avro(field("a", "\"string\"")),
        avro(field("b", "\"string\"", "\"aliases\":[\"a\"]")));
    restApp.restClient.deleteSchemaVersion(RestService.DEFAULT_REQUEST_PROPERTIES, subject, "2");
    assertNameVerdict(subject, AvroSchema.TYPE, renamed, CompatibilityLevel.BACKWARD,
        "FIELD_DELETED");
    register(subject, AvroSchema.TYPE, renamed);
    logical(subject, CompatibilityLevel.BACKWARD);
    assertFlag(subject, 2, null);
    assertFlag(subject, 3, null);
  }

  // --- A new identity is a deletion and an addition -------------------------------------------

  @Test
  public void testAProtobufRenumberIsIncompatibleByProvenance() throws Exception {
    String subject = "proto_renumber";
    String renumbered = proto("p", "optional string a = 3;");
    history(subject, ProtobufSchema.TYPE, proto("p", "optional string a = 2;"));
    assertNameVerdict(subject, ProtobufSchema.TYPE, renumbered, CompatibilityLevel.BACKWARD,
        null);
    register(subject, ProtobufSchema.TYPE, renumbered);
    logical(subject, CompatibilityLevel.BACKWARD);
    assertFlag(subject, 2, "FIELD_DELETED");
  }

  @Test
  public void testAProtobufPackageMoveIsIncompatibleByProvenance() throws Exception {
    // A type is identified by its full name, so every field of a moved message is new.
    String subject = "proto_package";
    String moved = proto("q", "optional string a = 2;");
    history(subject, ProtobufSchema.TYPE, proto("p", "optional string a = 2;"));
    // Natively a package change is incompatible too; by name the columns continue.
    assertNameVerdictWithout(subject, ProtobufSchema.TYPE, moved, CompatibilityLevel.BACKWARD,
        "FIELD_DELETED");
    register(subject, ProtobufSchema.TYPE, moved);
    logical(subject, CompatibilityLevel.BACKWARD);
    assertFlag(subject, 2, "FIELD_DELETED");
  }

  @Test
  public void testAFieldReAddedAcrossAGapIsIncompatibleTransitively() throws Exception {
    // a is dropped at v=2 and re-added at v=3 (with z, so v=3 is no repeat of v=1): a new column,
    // a deletion of v=1's. Only a transitive level compares v=1; by name a continues.
    String[][] histories = {
        {AvroSchema.TYPE, avro(field("a", "[\"null\",\"int\"]", "\"default\":null")), avro(),
            avro(field("a", "[\"null\",\"int\"]", "\"default\":null"),
                field("z", "[\"null\",\"int\"]", "\"default\":null"))},
        {ProtobufSchema.TYPE, proto("p", "optional int32 a = 2;"), proto("p", ""),
            proto("p", "optional int32 a = 2;\n  optional int32 z = 3;")},
        // Closed, as natively adding a property to an open content model is incompatible.
        {JsonSchema.TYPE, closedJson("\"a\":{\"type\":\"integer\"}"), closedJson(""),
            closedJson("\"a\":{\"type\":\"integer\"},\"z\":{\"type\":\"integer\"}")}};
    for (String[] history : histories) {
      String type = history[0];
      for (CompatibilityLevel level : new CompatibilityLevel[] {CompatibilityLevel.BACKWARD,
          CompatibilityLevel.BACKWARD_TRANSITIVE}) {
        String subject = "gap_" + type + "_" + level;
        history(subject, type, history[1], history[2]);
        assertNameVerdict(subject, type, history[3], level, null);
        register(subject, type, history[3]);
        logical(subject, level);
        assertFlag(subject, 3,
            level == CompatibilityLevel.BACKWARD ? null : "FIELD_DELETED");
      }
    }
  }

  @Test
  public void testAKindChangeIsIncompatibleByNameAndByProvenance() throws Exception {
    String[][] histories = {
        {AvroSchema.TYPE, avro(field("a", "[\"null\",\"int\"]", "\"default\":null")),
            avro(field("a", "[\"null\",{\"type\":\"record\",\"name\":\"M\",\"fields\":"
                + "[{\"name\":\"x\",\"type\":\"int\"}]}]", "\"default\":null"))},
        {ProtobufSchema.TYPE, proto("p", "optional int32 a = 2;"),
            proto("p", "M a = 2;\n  message M { int32 x = 1; }")},
        {JsonSchema.TYPE, json("\"a\":{\"type\":\"integer\"}"),
            json("\"a\":{\"type\":\"object\",\"properties\":{\"x\":{\"type\":\"integer\"}}}")}};
    for (String[] history : histories) {
      String subject = "kind_" + history[0];
      history(subject, history[0], history[1]);
      assertNameVerdict(subject, history[0], history[2], CompatibilityLevel.BACKWARD,
          "TYPE_MISMATCH");
      register(subject, history[0], history[2]);
      logical(subject, CompatibilityLevel.BACKWARD);
      assertFlag(subject, 2, "TYPE_MISMATCH");
    }
  }

  // --- Where names still pair -----------------------------------------------------------------

  @Test
  public void testAnEarlierBlockingVersionPairsByNameTransitively() throws Exception {
    // v=2, imported, aliases two fields of v=1, so no provenance reaches back past it: the
    // transitive answer pairs by name, as registration will, while BACKWARD needs only v=2.
    for (CompatibilityLevel level : new CompatibilityLevel[] {CompatibilityLevel.BACKWARD,
        CompatibilityLevel.BACKWARD_TRANSITIVE}) {
      String subject = "imported_block_" + level;
      restApp.restClient.setMode("IMPORT", subject);
      // The same content in both subjects, so the same ids.
      int id = 9001;
      restApp.restClient.registerSchema(
          avro(field("a", "\"int\""), field("b", "\"int\"")), subject, 1, id);
      restApp.restClient.registerSchema(avro(field("c", "[\"null\",\"int\"]",
          "\"default\":null,\"aliases\":[\"a\",\"b\"]")), subject, 2, id + 1);
      restApp.restClient.setMode("READWRITE", subject);
      logical(subject, CompatibilityLevel.NONE);
      register(subject, AvroSchema.TYPE,
          avro(field("d", "[\"null\",\"int\"]", "\"default\":null,\"aliases\":[\"c\"]")));
      logical(subject, level);
      assertFlag(subject, 3, level == CompatibilityLevel.BACKWARD ? null : "FIELD_DELETED");
    }
  }

  // --- Compatibility groups ------------------------------------------------------------------

  @Test
  public void testWithACompatibilityGroupARenameIsCompatibleByProvenance() throws Exception {
    // A group selects the versions compared; provenance, as the endpoint's ids, ignores groups.
    String subject = "avro_rename_group";
    history(subject, AvroSchema.TYPE, avro(field("a", "\"string\"")),
        avro(field("b", "\"string\"", "\"aliases\":[\"a\"]")));
    restApp.restClient.updateConfig(groupConfig(CompatibilityLevel.BACKWARD), subject);
    assertFlag(subject, 2, null);
  }

  @Test
  public void testAFieldAnotherGroupDroppedIsIncompatibleByProvenance() throws Exception {
    // v=2, group 2, drops a; v=3, group 1, re-adds it (with z, so it is no repeat of v=1). Only
    // v=1 is compared, so by name a continues; provenance walks through v=2, where a ends.
    String subject = "group_interleaved";
    String readded = avro(field("a", "\"string\""),
        field("z", "[\"null\",\"int\"]", "\"default\":null"));
    restApp.restClient.updateConfig(groupConfig(CompatibilityLevel.NONE), subject);
    restApp.restClient.registerSchema(inGroup(avro(field("a", "\"string\"")), "1"), subject, false);
    restApp.restClient.registerSchema(inGroup(avro(), "2"), subject, false);
    restApp.restClient.updateConfig(groupConfig(CompatibilityLevel.BACKWARD), subject);
    assertEquals(List.of(), restApp.restClient.testCompatibility(inGroup(readded, "1"), subject,
        null, false, true));
    restApp.restClient.updateConfig(groupConfig(CompatibilityLevel.NONE), subject);
    restApp.restClient.registerSchema(inGroup(readded, "1"), subject, false);
    restApp.restClient.updateConfig(groupConfig(CompatibilityLevel.BACKWARD), subject);
    assertFlag(subject, 3, "FIELD_DELETED");
  }

  // --- What a flag names ----------------------------------------------------------------------

  @Test
  public void testAFlagAfterANumberingGapNamesTheVersionItJudges() throws Exception {
    // Imported as versions 1 and 5; v=5 aliases two fields of v=1, so no provenance reaches it.
    String subject = "imported_gap";
    restApp.restClient.setMode("IMPORT", subject);
    restApp.restClient.registerSchema(
        avro(field("a", "\"int\""), field("b", "\"int\"")), subject, 1, 9101);
    restApp.restClient.registerSchema(avro(field("c", "[\"null\",\"int\"]",
        "\"default\":null,\"aliases\":[\"a\",\"b\"]")), subject, 5, 9102);
    restApp.restClient.setMode("READWRITE", subject);
    logical(subject, CompatibilityLevel.BACKWARD);
    assertFlag(subject, 5, "PROVENANCE_UNAVAILABLE");
    String messages = flag(subject, 5).getMessages().toString();
    assertTrue(messages.contains("at version 5"), messages);
  }

  @Test
  public void testAFlagPastAnEarlierVersionThatNoLongerParsesIsA42214() throws Exception {
    // v=1 rewritten to name a type that does not exist, as if a reference no longer resolved.
    String subject = "unparseable_earlier";
    history(subject, AvroSchema.TYPE, avro(field("a", "\"string\"")),
        avro(field("a", "\"string\""), field("b", "[\"null\",\"int\"]", "\"default\":null")));
    restApp.schemaRegistry().getKafkaStore().put(new SchemaKey(subject, 1),
        new SchemaValue(subject, 1, 9201, avro(field("x", "\"ns.Missing\"")), false));
    logical(subject, CompatibilityLevel.BACKWARD);
    RestClientException e = assertThrows(RestClientException.class, () -> flag(subject, 2));
    assertEquals(Errors.UNRESOLVABLE_REFERENCE_ERROR_CODE, e.getErrorCode());
  }

  @Test
  public void testAVersionWithManyMessagesListsAtMostOneHundred() throws Exception {
    // v=2 drops 120 fields of v=1: a FIELD_DELETED each, more than a verdict lists.
    String subject = "many_messages";
    String[] fields = new String[120];
    for (int i = 0; i < fields.length; i++) {
      fields[i] = field("f" + i, "[\"null\",\"int\"]", "\"default\":null");
    }
    history(subject, AvroSchema.TYPE, avro(fields), avro());
    logical(subject, CompatibilityLevel.BACKWARD);
    ProvenanceVersion flagged = flag(subject, 2);
    assertFalse(flagged.getCompatible());
    List<String> messages = flagged.getMessages();
    assertEquals(100, messages.size());
    assertTrue(messages.get(98).contains("FIELD_DELETED"), messages.get(98));
    assertTrue(messages.get(99).endsWith(" more messages not listed"), messages.get(99));
  }

  // --- The request ----------------------------------------------------------------------------

  @Test
  public void testFlagsAreAbsentUnlessAskedForAndOnlyLogicalIsAccepted() throws Exception {
    String subject = "flags_request";
    history(subject, AvroSchema.TYPE, avro(field("a", "\"string\"")));
    SchemaProvenance plain = restApp.restClient.getProvenanceByVersion(
        RestService.DEFAULT_REQUEST_PROPERTIES, subject, "1", "1", true, false);
    assertNull(plain.getVersions().get(0).getCompatible());
    assertNull(plain.getVersions().get(0).getMessages());
    for (String policy : new String[] {"STRICT", "", "lenient"}) {
      RestClientException e = assertThrows(RestClientException.class,
          () -> restApp.restClient.getProvenanceByVersion(RestService.DEFAULT_REQUEST_PROPERTIES,
              subject, "1", "1", true, false, policy), policy);
      assertEquals(Errors.INVALID_PROVENANCE_REQUEST_ERROR_CODE, e.getErrorCode(), policy);
    }
    assertTrue(restApp.restClient.getProvenanceByVersion(RestService.DEFAULT_REQUEST_PROPERTIES,
        subject, "1", "1", true, false, "logical").getVersions().get(0).getCompatible());
  }

  @Test
  public void testARequestComparingTooManyVersionsIsA42219() throws Exception {
    // At most 3 comparisons (the cluster test's limit): at a transitive level each version is
    // compared with every earlier one, else with the one before it. One version is always answered.
    String subject = "too_many_comparisons";
    List<String> fields = new ArrayList<>();
    List<String> versions = new ArrayList<>();
    for (int i = 0; i < 5; i++) {
      fields.add(field("f" + i, "[\"null\",\"int\"]", "\"default\":null"));
      versions.add(avro(fields.toArray(new String[0])));
    }
    history(subject, AvroSchema.TYPE, versions.toArray(new String[0]));
    logical(subject, CompatibilityLevel.BACKWARD_TRANSITIVE);
    assertComparisonsRejected(subject, "1", "5", true);
    assertComparisonsRejected(subject, "1", "5", false);
    assertComparisonsRejected(subject, "3", "4", true);
    assertEquals(2, flags(subject, "2", "3", true).getVersions().size());
    assertFlag(subject, 5, null);
    logical(subject, CompatibilityLevel.BACKWARD);
    assertComparisonsRejected(subject, "1", "5", true);
    assertEquals(3, flags(subject, "2", "4", true).getVersions().size());
  }

  // -------------------------------------------------------------------------------------------

  // Registers each version under LOGICAL at NONE, so only validity is checked: no comparison
  // stops a version the flag is meant to judge.
  private void history(String subject, String type, String... versions) throws Exception {
    logical(subject, CompatibilityLevel.NONE);
    for (String version : versions) {
      register(subject, type, version);
    }
  }

  private void logical(String subject, CompatibilityLevel level) throws Exception {
    restApp.restClient.updateConfig(logicalConfig(level), subject);
  }

  private static ConfigUpdateRequest logicalConfig(CompatibilityLevel level) {
    ConfigUpdateRequest config = new ConfigUpdateRequest();
    config.setCompatibilityLevel(level.name);
    config.setCompatibilityPolicy("LOGICAL");
    return config;
  }

  private static ConfigUpdateRequest groupConfig(CompatibilityLevel level) {
    ConfigUpdateRequest config = logicalConfig(level);
    config.setCompatibilityGroup("app.major");
    return config;
  }

  // An Avro schema in group major of app.major.
  private static RegisterSchemaRequest inGroup(String schema, String major) {
    RegisterSchemaRequest request = new RegisterSchemaRequest();
    request.setSchema(schema);
    request.setSchemaType(AvroSchema.TYPE);
    request.setMetadata(new Metadata(null, Map.of("app.major", major), null));
    return request;
  }

  private void register(String subject, String type, String schema) throws Exception {
    restApp.restClient.registerSchema(schema, type, Collections.emptyList(), subject);
  }

  // What registration says today, by name, at level under LOGICAL: compatible when rule is
  // null, else incompatible naming rule. Leaves the subject at NONE for the next registration.
  private void assertNameVerdict(String subject, String type, String schema,
      CompatibilityLevel level, String rule) throws Exception {
    logical(subject, level);
    RegisterSchemaRequest request = new RegisterSchemaRequest();
    request.setSchema(schema);
    request.setSchemaType(type);
    List<String> messages =
        restApp.restClient.testCompatibility(request, subject, null, false, true);
    if (rule == null) {
      assertEquals(List.of(), messages);
    } else {
      assertTrue(messages.toString().contains("errorType:\"" + rule + "\""), messages.toString());
    }
    logical(subject, CompatibilityLevel.NONE);
  }

  // As assertNameVerdict, asserting only that rule is not reported.
  private void assertNameVerdictWithout(String subject, String type, String schema,
      CompatibilityLevel level, String rule) throws Exception {
    logical(subject, level);
    RegisterSchemaRequest request = new RegisterSchemaRequest();
    request.setSchema(schema);
    request.setSchemaType(type);
    List<String> messages =
        restApp.restClient.testCompatibility(request, subject, null, false, true);
    assertFalse(messages.toString().contains("errorType:\"" + rule + "\""), messages.toString());
    logical(subject, CompatibilityLevel.NONE);
  }

  // The flag on version at the subject's current level, the policy taken as LOGICAL: compatible
  // when rule is null, else incompatible naming rule. Asked of that version alone, as a range
  // across an earlier blocking version has no provenance.
  private void assertFlag(String subject, int version, String rule) throws Exception {
    ProvenanceVersion flagged = flag(subject, version);
    assertEquals(version, flagged.getVersion().intValue());
    if (rule == null) {
      assertTrue(flagged.getCompatible(), flagged.toString());
      assertNull(flagged.getMessages(), flagged.toString());
    } else {
      assertFalse(flagged.getCompatible(), flagged.toString());
      assertTrue(flagged.getMessages().toString().contains("errorType:\"" + rule + "\""),
          flagged.toString());
    }
  }

  private ProvenanceVersion flag(String subject, int version) throws Exception {
    String v = String.valueOf(version);
    return flags(subject, v, v, true).getVersions().get(0);
  }

  private SchemaProvenance flags(String subject, String from, String to, boolean interior)
      throws Exception {
    return restApp.restClient.getProvenanceByVersion(RestService.DEFAULT_REQUEST_PROPERTIES,
        subject, from, to, interior, false, "LOGICAL");
  }

  private void assertComparisonsRejected(String subject, String from, String to,
      boolean interior) {
    RestClientException e = assertThrows(RestClientException.class,
        () -> flags(subject, from, to, interior), from + ".." + to);
    assertEquals(Errors.PROVENANCE_RANGE_TOO_LONG_ERROR_CODE, e.getErrorCode(), from + ".." + to);
  }

  // A record R with id and the given fields.
  private static String avro(String... fields) {
    StringBuilder all = new StringBuilder("{\"name\":\"id\",\"type\":\"int\"}");
    for (String field : fields) {
      all.append(',').append(field);
    }
    return "{\"type\":\"record\",\"name\":\"R\",\"namespace\":\"ns\",\"fields\":[" + all + "]}";
  }

  private static String field(String name, String type, String... extra) {
    return "{\"name\":\"" + name + "\",\"type\":" + type
        + (extra.length > 0 ? "," + String.join(",", extra) : "") + "}";
  }

  // A message R in the given package with id and the given body.
  private static String proto(String pkg, String body) {
    return "syntax = \"proto3\";\npackage " + pkg + ";\nmessage R {\n  int32 id = 1;\n  " + body
        + "\n}\n";
  }

  // An object with id and the given properties.
  private static String json(String properties) {
    return "{\"type\":\"object\",\"properties\":{\"id\":{\"type\":\"integer\"}"
        + (properties.isEmpty() ? "" : "," + properties) + "}}";
  }

  // As json, with no other properties allowed.
  private static String closedJson(String properties) {
    String open = json(properties);
    return open.substring(0, open.length() - 1) + ",\"additionalProperties\":false}";
  }
}
