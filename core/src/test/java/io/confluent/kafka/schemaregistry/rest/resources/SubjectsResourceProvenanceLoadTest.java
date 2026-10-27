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

package io.confluent.kafka.schemaregistry.rest.resources;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import io.confluent.kafka.schemaregistry.avro.AvroSchema;
import io.confluent.kafka.schemaregistry.client.rest.entities.ProvenanceVersion;
import io.confluent.kafka.schemaregistry.client.rest.entities.Schema;
import io.confluent.kafka.schemaregistry.client.rest.entities.SchemaProvenance;
import io.confluent.kafka.schemaregistry.exceptions.SchemaRegistryStoreException;
import io.confluent.kafka.schemaregistry.rest.SchemaRegistryConfig;
import io.confluent.kafka.schemaregistry.rest.exceptions.Errors;
import io.confluent.kafka.schemaregistry.storage.LookupFilter;
import io.confluent.kafka.schemaregistry.storage.SchemaKey;
import io.confluent.kafka.schemaregistry.storage.SchemaRegistry;
import io.confluent.kafka.schemaregistry.storage.SchemaValue;
import io.confluent.kafka.schemaregistry.utils.QualifiedSubject;
import io.confluent.rest.exceptions.RestException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * What a provenance request reads: the versions of its range, and none outside it.
 */
class SubjectsResourceProvenanceLoadTest {

  private static final String SUBJECT = "s";

  private SchemaRegistry registry;
  private SubjectsResource resource;

  @BeforeEach
  void setUp() throws Exception {
    registry = mock(SchemaRegistry.class);
    SchemaRegistryConfig config = mock(SchemaRegistryConfig.class);
    when(config.getInt(SchemaRegistryConfig.PROVENANCE_INTERIOR_MAX_VERSIONS_CONFIG))
        .thenReturn(100);
    when(registry.config()).thenReturn(config);
    when(registry.tenant()).thenReturn(QualifiedSubject.DEFAULT_TENANT);
    when(registry.parseSchema(any(Schema.class), eq(false), eq(false)))
        .thenAnswer(call -> new AvroSchema(((Schema) call.getArgument(0)).getSchema()));
    // Any version not stubbed fails the request, so reading it is caught.
    when(registry.get(eq(SUBJECT), anyInt(), anyBoolean()))
        .thenThrow(new SchemaRegistryStoreException("read outside the range"));
    // No earlier version with any ID unless a test says so; a mock's Integer would be 0.
    when(registry.earliestVersionWithId(anyInt(), any())).thenReturn(null);
    resource = new SubjectsResource(registry);
  }

  @Test
  void aRangeReadsOnlyItsOwnVersions() throws Exception {
    versions(1, 2, 3, 4, 5);
    stub(2, 3, 4);

    assertEquals(Arrays.asList(2, 3, 4), versionsOf(byVersion("2", "4", true)));
    verify(registry, never()).get(eq(SUBJECT), eq(1), anyBoolean());
    verify(registry, never()).get(eq(SUBJECT), eq(5), anyBoolean());
    verify(registry, never()).getAllVersions(any(), any());
  }

  @Test
  void aSchemaIdEndIsResolvedInTheSubjectAlone() throws Exception {
    stub(3, 4);
    when(registry.latestVersionWithId(103, SUBJECT)).thenReturn(3);
    when(registry.getLatestVersion(SUBJECT)).thenReturn(schema(4));

    SchemaProvenance provenance =
        resource.getProvenance(SUBJECT, null, "latest", 103, null, false, false, null);

    // Other subjects sharing the ID are never read.
    assertEquals(Arrays.asList(3, 4), versionsOf(provenance));
    verify(registry, never()).listVersionsForId(anyInt(), any(), anyBoolean());
    verify(registry, never()).getAllVersions(any(), any());
  }

  @Test
  void aNumberNoVersionHasInsideTheRangeIsSkipped() throws Exception {
    stub(1, 2, 4);
    doReturn(null).when(registry).get(SUBJECT, 3, true);

    assertEquals(Arrays.asList(1, 2, 4), versionsOf(byVersion("1", "4", true)));
  }

  @Test
  void aSpanLongerThanAnInteriorRangeReadsOnlyTheVersionsListed() throws Exception {
    SchemaRegistryConfig config = registry.config();
    when(config.getInt(SchemaRegistryConfig.PROVENANCE_INTERIOR_MAX_VERSIONS_CONFIG))
        .thenReturn(3);
    versions(1, 10);
    stub(1, 10);

    assertEquals(Arrays.asList(1, 10), versionsOf(byVersion("1", "10", false)));
    verify(registry, times(2)).get(eq(SUBJECT), anyInt(), anyBoolean());
  }

  @Test
  void theFlagJudgesEachVersionAfterTheOneBeforeIt() throws Exception {
    stub(2, 3, 4);
    when(registry.getSchemaValue(new SchemaKey(SUBJECT, 1))).thenReturn(value(1));
    when(registry.isLogicallyCompatibleByProvenance(eq(SUBJECT), anyInt(), anyInt()))
        .thenReturn(Collections.emptyList());

    resource.getProvenance(SUBJECT, "2", "4", null, null, true, false, "LOGICAL");

    verify(registry).isLogicallyCompatibleByProvenance(SUBJECT, 2, 1);
    verify(registry).isLogicallyCompatibleByProvenance(SUBJECT, 3, 2);
    verify(registry).isLogicallyCompatibleByProvenance(SUBJECT, 4, 3);
    verify(registry, never()).getAllVersions(any(), any());
  }

  @Test
  void theFlagLooksPastANumberNoVersionHasBeforeTheRange() throws Exception {
    versions(1, 3, 4);
    stub(3, 4);
    when(registry.isLogicallyCompatibleByProvenance(eq(SUBJECT), anyInt(), anyInt()))
        .thenReturn(Collections.emptyList());

    resource.getProvenance(SUBJECT, "3", "4", null, null, false, false, "LOGICAL");

    verify(registry).isLogicallyCompatibleByProvenance(SUBJECT, 3, 1);
    verify(registry).isLogicallyCompatibleByProvenance(SUBJECT, 4, 3);
  }

  @Test
  void theRepeatedIdCheckLooksInTheSubjectAlone() throws Exception {
    stub(2, 3);
    when(registry.getSchemaValue(new SchemaKey(SUBJECT, 1))).thenReturn(value(1));
    // v3's schema id was first used by v1, outside the range.
    when(registry.earliestVersionWithId(102, SUBJECT)).thenReturn(2);
    when(registry.earliestVersionWithId(103, SUBJECT)).thenReturn(1);
    when(registry.isLogicallyCompatibleByProvenance(eq(SUBJECT), anyInt(), anyInt()))
        .thenReturn(Collections.emptyList());

    List<ProvenanceVersion> flagged = resource
        .getProvenance(SUBJECT, "2", "3", null, null, true, false, "LOGICAL").getVersions();

    assertTrue(flagged.get(0).getCompatible());
    assertFalse(flagged.get(1).getCompatible());
    assertTrue(flagged.get(1).getMessages().get(0).contains("SCHEMA_ID_REPEATED"));
    verify(registry, never()).isLogicallyCompatibleByProvenance(SUBJECT, 3, 2);
    verify(registry, never()).listVersionsForId(anyInt(), any(), anyBoolean());
  }

  @Test
  void aMissingEndOfAnEmptySubjectIsTheSubjectNotFound() throws Exception {
    versions();
    doReturn(null).when(registry).get(SUBJECT, 1, true);

    RestException e = assertThrows(RestException.class, () -> byVersion("1", "1", false));
    assertEquals(Errors.SUBJECT_NOT_FOUND_ERROR_CODE, e.getErrorCode());
  }

  private SchemaProvenance byVersion(String from, String to, boolean includeInterior) {
    return resource.getProvenance(SUBJECT, from, to, null, null, includeInterior, false, null);
  }

  private void versions(int... numbers) throws Exception {
    List<SchemaKey> keys = new ArrayList<>();
    for (int version : numbers) {
      keys.add(new SchemaKey(SUBJECT, version));
    }
    when(registry.getAllVersions(SUBJECT, LookupFilter.INCLUDE_DELETED))
        .thenAnswer(call -> keys.iterator());
  }

  private void stub(int... numbers) throws Exception {
    for (int version : numbers) {
      doReturn(schema(version)).when(registry).get(SUBJECT, version, true);
    }
  }

  // Version n is a record of n int fields, under schema id 100 + n.
  private static Schema schema(int version) {
    return new Schema(SUBJECT, version, 100 + version, new AvroSchema(record(version)));
  }

  private static SchemaValue value(int version) {
    return new SchemaValue(SUBJECT, version, 100 + version, record(version), false);
  }

  private static String record(int fields) {
    StringBuilder record = new StringBuilder("{\"type\":\"record\",\"name\":\"r\",\"fields\":[");
    for (int i = 1; i <= fields; i++) {
      record.append(i > 1 ? "," : "").append("{\"name\":\"f").append(i)
          .append("\",\"type\":\"int\"}");
    }
    return record.append("]}").toString();
  }

  private static List<Integer> versionsOf(SchemaProvenance provenance) {
    return provenance.getVersions().stream().map(ProvenanceVersion::getVersion)
        .collect(Collectors.toList());
  }
}
