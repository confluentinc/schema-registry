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

package io.confluent.kafka.serializers.provenance;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

import io.confluent.kafka.schemaregistry.ParsedSchema;
import io.confluent.kafka.schemaregistry.avro.AvroSchema;
import io.confluent.kafka.schemaregistry.client.MockSchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.rest.entities.Metadata;
import io.confluent.kafka.schemaregistry.client.rest.entities.ProvenanceVersion;
import io.confluent.kafka.schemaregistry.client.rest.entities.SchemaProvenance;
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;
import io.confluent.kafka.serializers.schema.id.SchemaId;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import org.apache.kafka.common.errors.SerializationException;
import org.junit.Test;

public class ProvenanceProjectorTest {

  private static final String SUBJECT = "s-value";

  @Test
  public void anUnavailableOutcomeIsCachedWithoutATtl() throws Exception {
    CountingClient client = new CountingClient();
    ProvenanceProjector<String> projector = new ProvenanceProjector<>(client, "v1", 10, -1);
    ask(projector, client);
    ask(projector, client);
    assertEquals(1, client.asked);
  }

  @Test
  public void anExpiredOutcomeIsWorkedOutAfresh() throws Exception {
    CountingClient client = new CountingClient();
    ProvenanceProjector<String> projector = new ProvenanceProjector<>(client, "v1", 10, 0);
    ask(projector, client);
    ask(projector, client);
    assertEquals(2, client.asked);
  }

  @Test
  public void theCacheHoldsNoMoreThanItsSize() throws Exception {
    CountingClient client = new CountingClient();
    ProvenanceProjector<String> projector = new ProvenanceProjector<>(client, "v1", 1, -1);
    ask(projector, client, client.writer);
    ask(projector, client, client.otherWriter);
    ask(projector, client, client.writer);
    assertEquals(3, client.asked);
  }

  @Test
  public void aSuppliedReaderIdIsUsedAsIs() throws Exception {
    CountingClient client = new CountingClient();
    ProvenanceProjector<String> projector = new ProvenanceProjector<>(client, "v1", 10, -1);
    // Registered nowhere, as a reader with a writer's rules merged onto it is not.
    ParsedSchema merged = new AvroSchema("\"double\"");
    ParsedSchema handedOver = projector.readerSchemas(
        writer -> ReaderSchema.of(merged, client.readerId)).apply(client.writerSchema);

    projector.project(SUBJECT, new SchemaId(AvroSchema.TYPE, client.writer, (String) null),
        client.writerSchema, handedOver, false, mapping -> "built");
    assertEquals(1, client.asked);
    assertEquals(client.readerId, client.lastReaderId);
  }

  @Test
  public void aRejectedRequestFailsEveryRecordFromThatWriter() throws Exception {
    for (int[] rejection : new int[][] {{422, 42202}, {422, 42215}, {404, 40402}}) {
      CountingClient client = new CountingClient();
      client.failure = new RestClientException("rejected", rejection[0], rejection[1]);
      ProvenanceProjector<String> projector = new ProvenanceProjector<>(client, "v1", 10, -1);
      for (int record = 0; record < 2; record++) {
        SerializationException e = assertThrows(SerializationException.class,
            () -> ask(projector, client));
        assertTrue(e.getMessage(), e.getMessage().contains("rejected the provenance request"));
      }
      assertEquals(1, client.asked);
    }
  }

  @Test
  public void aServerErrorFailsTheRecordAndIsAskedAgain() throws Exception {
    CountingClient client = new CountingClient();
    client.failure = new RestClientException("boom", 500, 500);
    ProvenanceProjector<String> projector = new ProvenanceProjector<>(client, "v1", 10, -1);
    assertThrows(SerializationException.class, () -> ask(projector, client));
    assertThrows(SerializationException.class, () -> ask(projector, client));
    assertEquals(2, client.asked);
  }

  @Test
  public void aBuildThatFindsTheResponseInconsistentFailsEveryRecord() throws Exception {
    CountingClient client = new CountingClient();
    client.provenance = new SchemaProvenance(SUBJECT, Arrays.asList(
        new ProvenanceVersion(1, client.writer, Collections.emptyList()),
        new ProvenanceVersion(3, client.readerId, Collections.emptyList())));
    ProvenanceProjector<String> projector = new ProvenanceProjector<>(client, "v1", 10, -1);
    SchemaId id = new SchemaId(AvroSchema.TYPE, client.writer, (String) null);
    for (int record = 0; record < 2; record++) {
      assertThrows(SerializationException.class, () -> projector.project(SUBJECT, id,
          client.writerSchema, client.reader, false, mapping -> {
            throw new SerializationException("inconsistent");
          }));
    }
    assertEquals(1, client.asked);
  }

  @Test
  public void aWriterNamedByGuidIsAskedAboutByItsId() throws Exception {
    CountingClient client = new CountingClient();
    ProvenanceProjector<String> projector = new ProvenanceProjector<>(client, "v1", 10, -1);
    SchemaId byGuid = new SchemaId(AvroSchema.TYPE, null,
        client.getGuid(SUBJECT, client.writerSchema));
    for (int record = 0; record < 2; record++) {
      projector.project(SUBJECT, byGuid, client.writerSchema, client.reader, false, m -> "built");
    }
    assertEquals(1, client.asked);
    assertEquals(client.writer, client.lastWriterId);
  }

  @Test
  public void aWriterNamedByGuidOfNoVersionFallsBack() throws Exception {
    CountingClient client = new CountingClient();
    ParsedSchema foreign = new AvroSchema("\"boolean\"");
    client.register("other-value", foreign);
    ProvenanceProjector<String> projector = new ProvenanceProjector<>(client, "v1", 10, -1);
    Optional<String> built = projector.project(SUBJECT,
        new SchemaId(AvroSchema.TYPE, null, client.getGuid("other-value", foreign)), foreign,
        client.reader, false, m -> "built");
    assertFalse(built.isPresent());
    assertEquals(0, client.asked);
  }

  @Test
  public void aWriterIdUnderNoVersionIsMatchedByStructure() throws Exception {
    CountingClient client = new CountingClient();
    client.rejectsForeignWriters = true;
    // The writer's own schema is the subject's first version with other metadata on it.
    ParsedSchema withMetadata = client.writerSchema.copy(
        new Metadata(null, Collections.singletonMap("owner", "x"), null), null);
    int foreign = client.register("other-value", withMetadata);
    ProvenanceProjector<String> projector = new ProvenanceProjector<>(client, "v1", 10, -1);
    projector.project(SUBJECT, new SchemaId(AvroSchema.TYPE, foreign, (String) null),
        withMetadata, client.reader, false, m -> "built");
    assertEquals(2, client.asked);
    assertEquals(client.writer, client.lastWriterId);
  }

  private static void ask(ProvenanceProjector<String> projector, CountingClient client)
      throws Exception {
    ask(projector, client, client.writer);
  }

  private static void ask(ProvenanceProjector<String> projector, CountingClient client,
      int writerId) throws Exception {
    SchemaId id = new SchemaId(AvroSchema.TYPE, writerId, (String) null);
    Optional<String> built = projector.project(SUBJECT, id, client.writerSchema, client.reader,
        false, mapping -> "built");
    assertFalse(built.isPresent());
  }

  // Answers every provenance request as unavailable, or as set, counting how often it is asked.
  private static final class CountingClient extends MockSchemaRegistryClient {

    List<Integer> idsOf(String subject) throws IOException, RestClientException {
      List<Integer> ids = new ArrayList<>();
      for (int version : getAllVersions(subject)) {
        ids.add(getSchemaMetadata(subject, version).getId());
      }
      return ids;
    }

    final ParsedSchema writerSchema = new AvroSchema("\"int\"");
    final ParsedSchema reader = new AvroSchema("\"long\"");
    final int writer;
    final int otherWriter;
    final int readerId;
    int asked;
    int lastWriterId;
    int lastReaderId;
    boolean rejectsForeignWriters;
    RestClientException failure;
    SchemaProvenance provenance;

    CountingClient() throws Exception {
      writer = register(SUBJECT, writerSchema);
      otherWriter = register(SUBJECT, new AvroSchema("\"string\""));
      readerId = register(SUBJECT, reader);
    }

    @Override
    public SchemaProvenance getProvenanceById(String subject, int fromId, int toId,
        boolean includeInterior, boolean includeMultipleMessages,
        String algorithm) throws IOException, RestClientException {
      asked++;
      lastWriterId = fromId;
      lastReaderId = toId;
      if (rejectsForeignWriters && !idsOf(subject).contains(fromId)) {
        throw new RestClientException("not a version", 404, 40411);
      }
      if (failure != null) {
        throw failure;
      }
      if (provenance != null) {
        return provenance;
      }
      throw new UnsupportedOperationException("no provenance here");
    }
  }
}
