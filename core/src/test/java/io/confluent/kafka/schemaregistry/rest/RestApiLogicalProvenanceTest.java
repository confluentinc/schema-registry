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
import io.confluent.kafka.schemaregistry.CompatibilityPolicy;
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
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

/**
 * {@code compatible} on the provenance endpoint: whether each version is a valid backward step
 * from the one before it under LOGICAL, columns paired by provenance check id, while registration
 * and the compatibility checks still pair by name. A rename continues a column; a field given a
 * new identity is a deletion and an addition.
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
        "FIELD_RENAMED");
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
        "FIELD_RENAMED");
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
        "FIELD_RENAMED");
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
        "FIELD_DELETED");
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
  public void testAFlagPastAVersionWithNoLogicalFormIsIncompatible() throws Exception {
    // v=2 is recursive, so no provenance pairs v=3 with it; v=3 drops b of v=1. The one message
    // is LOGICAL's: no subject config is appended.
    String subject = "past_recursive";
    String node = "{\"type\":\"record\",\"name\":\"N\",\"fields\":[{\"name\":\"next\","
        + "\"type\":[\"null\",\"N\"],\"default\":null}]}";
    history(subject, AvroSchema.TYPE, avro(field("a", "\"int\""), field("b", "\"int\"")),
        avro(field("a", "\"int\""), field("n", node)), avro(field("a", "\"int\"")));
    assertFlag(subject, 3, "PROVENANCE_UNAVAILABLE");
    List<String> messages = flag(subject, 3).getMessages();
    assertEquals(1, messages.size(), messages.toString());
    assertTrue(messages.get(0).contains("through an earlier version"), messages.toString());
  }

  @Test
  public void testAFlagOnAVersionRepeatingAnEarlierSchemaIdIsIncompatible() throws Exception {
    // v=1 soft-deleted and registered again as v=3, with its id, outside LOGICAL; v=3 is a valid
    // step from v=2. Flagged with the one message, whether v=1 is in the range or not.
    String subject = "repeated_id";
    String first = avro(field("e", enumOf("X", "Y")));
    history(subject, AvroSchema.TYPE, first, avro(field("e", enumOf("X"))));
    restApp.restClient.deleteSchemaVersion(RestService.DEFAULT_REQUEST_PROPERTIES, subject, "1");
    register(subject, AvroSchema.TYPE, first);
    for (String from : new String[] {"1", "3"}) {
      List<ProvenanceVersion> versions = flags(subject, from, "3", true).getVersions();
      ProvenanceVersion repeated = versions.get(versions.size() - 1);
      assertEquals(3, repeated.getVersion().intValue());
      assertFalse(repeated.getCompatible(), repeated.toString());
      assertEquals(1, repeated.getMessages().size(), repeated.toString());
      assertTrue(repeated.getMessages().get(0).contains("errorType:\"SCHEMA_ID_REPEATED\""),
          repeated.toString());
      assertTrue(repeated.getMessages().get(0).contains("as version 1"), repeated.toString());
    }
    assertTrue(flag(subject, 1).getCompatible());
  }

  @Test
  public void testAFlagIgnoresAnotherSubjectsEarlierVersionWithTheSameSchemaId()
      throws Exception {
    // The same schema, and so the same id, is version 1 of the other subject and version 2 here;
    // only an id lookup leaking across subjects would flag version 2.
    String shared = avro(field("e", enumOf("X", "Y")));
    history("other_subject", AvroSchema.TYPE, shared);
    history("shared_id", AvroSchema.TYPE, avro(field("e", enumOf("X"))), shared);
    assertTrue(flag("shared_id", 2).getCompatible());
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

  // --- The subject's config ------------------------------------------------------------------

  @ParameterizedTest
  @ValueSource(strings = {"BACKWARD", "NONE", "FORWARD", "FULL", "BACKWARD_TRANSITIVE", "GROUP"})
  public void testAFlagJudgesEachStepFromTheOneBeforeWhateverTheConfig(String config)
      throws Exception {
    // v=2 changes a's type; v=3 adds a symbol, valid backward from v=2 only. v=2 is soft-deleted
    // and in another group, and still the version v=3 is compared with.
    String subject = "config_" + config;
    none(subject);
    restApp.restClient.registerSchema(inGroup(avro(field("a", "\"int\""),
        field("e", enumOf("X"))), "1"), subject, false);
    restApp.restClient.registerSchema(inGroup(avro(field("a", "\"string\""),
        field("e", enumOf("X"))), "2"), subject, false);
    restApp.restClient.registerSchema(inGroup(avro(field("a", "\"string\""),
        field("e", enumOf("X", "Y"))), "1"), subject, false);
    restApp.restClient.deleteSchemaVersion(RestService.DEFAULT_REQUEST_PROPERTIES, subject, "2");
    ConfigUpdateRequest update = new ConfigUpdateRequest();
    if (config.equals("GROUP")) {
      update.setCompatibilityLevel(CompatibilityLevel.BACKWARD.name);
      update.setCompatibilityGroup("app.major");
    } else {
      update.setCompatibilityLevel(config);
    }
    restApp.restClient.updateConfig(update, subject);
    List<Boolean> compatible = new ArrayList<>();
    for (ProvenanceVersion version : flags(subject, "1", "3", true).getVersions()) {
      compatible.add(version.getCompatible());
    }
    assertEquals(List.of(true, false, true), compatible);
  }

  // -------------------------------------------------------------------------------------------

  // Registers each version at NONE, under no LOGICAL, so no check stops a version the flag is
  // meant to judge.
  private void history(String subject, String type, String... versions) throws Exception {
    none(subject);
    for (String version : versions) {
      register(subject, type, version);
    }
  }

  private void none(String subject) throws Exception {
    ConfigUpdateRequest config = new ConfigUpdateRequest();
    config.setCompatibilityLevel(CompatibilityLevel.NONE.name);
    config.setCompatibilityPolicy(CompatibilityPolicy.STRICT.name);
    restApp.restClient.updateConfig(config, subject);
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
  // null, else incompatible naming rule. Leaves the subject as none() does.
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
    none(subject);
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
    none(subject);
  }

  // The flag on version: compatible when rule is null, else incompatible naming rule. Asked of
  // that version alone, as a range across an earlier blocking version has no provenance.
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

  // A record R with id and the given fields.
  private static String avro(String... fields) {
    StringBuilder all = new StringBuilder("{\"name\":\"id\",\"type\":\"int\"}");
    for (String field : fields) {
      all.append(',').append(field);
    }
    return "{\"type\":\"record\",\"name\":\"R\",\"namespace\":\"ns\",\"fields\":[" + all + "]}";
  }

  private static String enumOf(String... symbols) {
    return "{\"type\":\"enum\",\"name\":\"E\",\"symbols\":[\""
        + String.join("\",\"", symbols) + "\"]}";
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
}
