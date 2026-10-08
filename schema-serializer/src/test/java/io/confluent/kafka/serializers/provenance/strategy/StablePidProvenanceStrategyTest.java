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

package io.confluent.kafka.serializers.provenance.strategy;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

import io.confluent.kafka.schemaregistry.avro.AvroSchema;
import io.confluent.kafka.schemaregistry.client.SchemaMetadata;
import io.confluent.kafka.schemaregistry.client.rest.entities.ProvenanceField;
import io.confluent.kafka.schemaregistry.client.rest.entities.ProvenanceVersion;
import io.confluent.kafka.schemaregistry.client.rest.entities.SchemaProvenance;
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;
import io.confluent.kafka.schemaregistry.type.logical.provenance.ProvenanceMockSchemaRegistryClient;
import io.confluent.kafka.serializers.provenance.ProvenanceMapping;
import io.confluent.kafka.serializers.provenance.ProvenanceRejectedException;
import io.confluent.kafka.serializers.provenance.ProvenanceRetriableException;
import io.confluent.kafka.serializers.provenance.ProvenanceUnknownWriterException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.junit.Before;
import org.junit.Test;

/** Pairing by the Metastore's column ids, its names and kinds computed locally. */
public class StablePidProvenanceStrategyTest {

  private static final String SUBJECT = "orders-value";

  private Reregistering client;
  private int[] ids;

  @Before
  public void init() throws Exception {
    client = new Reregistering();
    // note dropped and re-added; a nested field renamed by alias.
    ids = new int[] {
        register("{\"name\":\"id\",\"type\":\"int\"},{\"name\":\"note\",\"type\":\"string\"},"
            + address("x", "")),
        register("{\"name\":\"id\",\"type\":\"int\"}," + address("y", ",\"aliases\":[\"x\"]")),
        register("{\"name\":\"id\",\"type\":\"int\"},{\"name\":\"note\",\"type\":\"string\"},"
            + address("y", ""))};
  }

  @Test
  public void everyPairJoinsAsTheRegistrysWithColumnIdsAsPids() {
    ColumnIds columnIds = ColumnIds.of(client, ids[0], 3);
    for (int[] pair : new int[][] {{1, 3}, {2, 3}, {3, 1}, {1, 2}}) {
      int writer = ids[pair[0] - 1];
      int reader = ids[pair[1] - 1];
      // The algorithm asked for is ignored: the column ids already decide the pairing.
      SchemaProvenance stable = columnIds.provenance(
          client, SUBJECT, writer, reader, false, false, "bogus");
      SchemaProvenance registry = new ClientProvenanceStrategy().provenance(
          client, SUBJECT, writer, reader, false, false, "v1");
      assertSameJoin(ProvenanceMapping.join(registry, writer, reader),
          ProvenanceMapping.join(stable, writer, reader));
      for (ProvenanceVersion version : stable.getVersions()) {
        for (ProvenanceField field : version.getFields()) {
          assertTrue(field.getPid() > 100);
        }
      }
    }
  }

  @Test
  public void aReAddedFieldHasANewColumnId() {
    SchemaProvenance stable = ColumnIds.of(client, ids[0], 3).provenance(
        client, SUBJECT, ids[0], ids[2], false, false, null);
    ProvenanceMapping mapping = ProvenanceMapping.join(stable, ids[0], ids[2]);
    assertEquals(null, mapping.writerPathOf(Arrays.asList(1)));
    assertEquals(Arrays.asList(0), mapping.writerPathOf(Arrays.asList(0)));
  }

  @Test
  public void aReaderPinnedByVersionIsThatVersion() {
    ColumnIds columnIds = ColumnIds.of(client, ids[0], 3);
    SchemaProvenance stable = columnIds.provenanceToVersion(
        client, SUBJECT, ids[0], 3, false, false, null);
    assertSameJoin(ProvenanceMapping.joinToVersion(new ClientProvenanceStrategy()
            .provenanceToVersion(client, SUBJECT, ids[0], 3, false, false, null), 3),
        ProvenanceMapping.joinToVersion(stable, 3));
    // A writer of the pinned version itself: one version, nothing to project.
    assertEquals(1, columnIds.provenanceToVersion(
        client, SUBJECT, ids[2], 3, false, false, null).getVersions().size());
  }

  @Test
  public void aVersionWithoutColumnIdsYetIsRetriable() {
    // The table has not been refreshed past version 2.
    ColumnIds columnIds = ColumnIds.of(client, ids[0], 2);
    assertThrows(ProvenanceRetriableException.class, () -> columnIds.provenance(
        client, SUBJECT, ids[2], ids[1], false, false, null));
    assertThrows(ProvenanceRetriableException.class, () -> columnIds.provenanceToVersion(
        client, SUBJECT, ids[0], 3, false, false, null));
  }

  @Test
  public void aSchemaIdUnderNoVersionIsAnUnknownWriter() {
    assertThrows(ProvenanceUnknownWriterException.class, () -> ColumnIds.of(client, ids[0], 3)
        .provenance(client, SUBJECT, 999, ids[2], false, false, null));
  }

  @Test
  public void aVersionTheRegistryLacksIsRejected() {
    assertThrows(ProvenanceRejectedException.class, () -> ColumnIds.of(client, ids[0], 3)
        .provenanceToVersion(client, SUBJECT, ids[0], 9, false, false, null));
  }

  @Test
  public void pathsProvenanceDoesNotReportAreIgnored() {
    ColumnIds columnIds = ColumnIds.of(client, ids[0], 3);
    columnIds.pids.get(3).put(Arrays.asList(2, 0, 0), 999);
    assertSameJoin(ProvenanceMapping.join(new ClientProvenanceStrategy().provenance(
            client, SUBJECT, ids[0], ids[2], false, false, null), ids[0], ids[2]),
        ProvenanceMapping.join(columnIds.provenance(
            client, SUBJECT, ids[0], ids[2], false, false, null), ids[0], ids[2]));
  }

  @Test
  public void aLocationWithoutAColumnIdBreaksTheContract() {
    ColumnIds columnIds = ColumnIds.of(client, ids[0], 3);
    columnIds.pids.get(3).remove(Arrays.asList(1));
    assertThrows(IllegalStateException.class, () -> columnIds.provenance(
        client, SUBJECT, ids[0], ids[2], false, false, null));
  }

  @Test
  public void anIdAlsoUnderAVersionWithoutColumnIdsWaitsForIt() throws Exception {
    // v2's schema id also sits under v4, which the table has not reached: the id stands for v4,
    // the latest carrying it, so its records wait for the refresh rather than read as v2.
    ColumnIds columnIds = ColumnIds.of(client, ids[0], 3);
    client.alsoUnder(4, ids[1]);
    assertThrows(ProvenanceRetriableException.class, () -> columnIds.provenance(
        client, SUBJECT, ids[1], ids[2], false, false, null));
  }

  private static void assertSameJoin(ProvenanceMapping expected, ProvenanceMapping actual) {
    assertEquals(expected.readerPaths(), actual.readerPaths());
    assertEquals(expected.writerPaths(), actual.writerPaths());
    for (List<Integer> path : expected.readerPaths()) {
      assertEquals(path.toString(), expected.writerPathOf(path), actual.writerPathOf(path));
      assertEquals(expected.readerNamesOf(path), actual.readerNamesOf(path));
      assertEquals(expected.readerKindOf(path), actual.readerKindOf(path));
    }
    for (List<Integer> path : expected.writerPaths()) {
      assertEquals(expected.writerNamesOf(path), actual.writerNamesOf(path));
      assertEquals(expected.writerKindOf(path), actual.writerKindOf(path));
    }
  }

  private int register(String fields) throws Exception {
    return client.register(SUBJECT, new AvroSchema("{\"type\":\"record\",\"name\":\"Order\","
        + "\"fields\":[" + fields + "]}"));
  }

  private static String address(String field, String aliases) {
    return "{\"name\":\"address\",\"type\":{\"type\":\"record\",\"name\":\"Address\",\"fields\":"
        + "[{\"name\":\"" + field + "\",\"type\":\"string\"" + aliases + "}]}}";
  }

  // A schema id that can also sit under one later version, as after a re-registration.
  private static final class Reregistering extends ProvenanceMockSchemaRegistryClient {

    private int extraVersion = -1;
    private int extraId;

    void alsoUnder(int version, int schemaId) {
      extraVersion = version;
      extraId = schemaId;
    }

    @Override
    public List<Integer> getAllVersions(String subject, boolean lookupDeletedSchema)
        throws IOException, RestClientException {
      List<Integer> versions = new ArrayList<>(super.getAllVersions(subject, lookupDeletedSchema));
      if (extraVersion > 0) {
        versions.add(extraVersion);
      }
      return versions;
    }

    @Override
    public SchemaMetadata getSchemaMetadata(String subject, int version,
        boolean lookupDeletedSchema) throws IOException, RestClientException {
      return version == extraVersion
          ? new SchemaMetadata(extraId, version, AvroSchema.TYPE, Collections.emptyList(), "")
          : super.getSchemaMetadata(subject, version, lookupDeletedSchema);
    }
  }

  // Column ids as the Metastore would assign them through version `last`: the registry's pids
  // over the whole history, offset so that they are not the registry's own.
  private static final class ColumnIds extends StablePidProvenanceStrategy {

    private final Map<Integer, Map<List<Integer>, Integer>> pids = new HashMap<>();

    static ColumnIds of(ProvenanceMockSchemaRegistryClient client, int firstId, int last) {
      ColumnIds columnIds = new ColumnIds();
      try {
        SchemaProvenance whole =
            client.getProvenanceToVersion(SUBJECT, firstId, last, true, false, null);
        for (ProvenanceVersion version : whole.getVersions()) {
          Map<List<Integer>, Integer> byPath = new HashMap<>();
          version.getFields().forEach(f -> byPath.put(f.getPath(), f.getPid() + 100));
          columnIds.pids.put(version.getVersion(), byPath);
        }
      } catch (Exception e) {
        throw new AssertionError(e);
      }
      return columnIds;
    }

    @Override
    protected Map<List<Integer>, Integer> pids(String subject, int version) {
      return pids.get(version);
    }
  }
}
