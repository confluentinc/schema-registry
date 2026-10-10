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
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

import io.confluent.kafka.schemaregistry.ParsedSchema;
import io.confluent.kafka.schemaregistry.avro.AvroSchema;
import io.confluent.kafka.schemaregistry.client.MockSchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.rest.entities.Metadata;
import io.confluent.kafka.schemaregistry.client.rest.entities.ProvenanceVersion;
import io.confluent.kafka.schemaregistry.client.rest.entities.SchemaProvenance;
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;
import io.confluent.kafka.serializers.provenance.strategy.ProvenanceStrategy;
import io.confluent.kafka.serializers.schema.id.SchemaId;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.kafka.common.errors.AuthenticationException;
import org.apache.kafka.common.errors.AuthorizationException;
import org.apache.kafka.common.errors.InterruptException;
import org.apache.kafka.common.errors.TimeoutException;
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
  public void pairsAskedAboutAtOnceAreAskedAboutOnce() throws Exception {
    // The first records of a pair, arriving together, share one request.
    CountingClient client = new CountingClient();
    client.gate = new CountDownLatch(1);
    ProvenanceProjector<String> projector = new ProvenanceProjector<>(client, "v1", 10, -1);
    ExecutorService pool = Executors.newFixedThreadPool(4);
    try {
      List<Future<?>> asks = new ArrayList<>();
      for (int i = 0; i < 4; i++) {
        asks.add(pool.submit(() -> {
          ask(projector, client);
          return null;
        }));
      }
      Thread.sleep(200);
      client.gate.countDown();
      for (Future<?> f : asks) {
        f.get(10, TimeUnit.SECONDS);
      }
    } finally {
      pool.shutdownNow();
    }
    assertEquals(1, client.askedAtOnce.get());
  }

  @Test
  public void theAlgorithmIsAskedForByName() throws Exception {
    CountingClient client = new CountingClient();
    ask(new ProvenanceProjector<>(client, "v1", 10, -1), client);
    assertEquals("v1", client.lastAlgorithm);
  }

  @Test
  public void aStrategyIsAskedInsteadOfTheClient() throws Exception {
    CountingClient client = new CountingClient();
    RecordingStrategy strategy = new RecordingStrategy();
    ask(new ProvenanceProjector<>(client, "dynamic", 10, -1, strategy), client);
    assertEquals(0, client.asked);
    assertEquals(Arrays.asList(SUBJECT, client.writer, client.readerId, false, "dynamic"),
        strategy.lastRequest);
    assertTrue(strategy.client == client);
  }

  @Test
  public void aStrategysRetriableFailureFailsTheRecordAndIsAskedAgain() throws Exception {
    CountingClient client = new CountingClient();
    RecordingStrategy strategy = new RecordingStrategy();
    strategy.failure = new ProvenanceRetriableException("down");
    ProvenanceProjector<String> projector =
        new ProvenanceProjector<>(client, "v1", 10, -1, strategy);
    assertThrows(SerializationException.class, () -> ask(projector, client));
    assertThrows(SerializationException.class, () -> ask(projector, client));
    assertEquals(2, strategy.asked);
  }

  @Test
  public void aStrategysNaturallyTransientFailureIsRetriedNotCached() throws Exception {
    for (RuntimeException transientFailure : Arrays.<RuntimeException>asList(
        new UncheckedIOException(new IOException("reset")), new TimeoutException("slow"),
        new InterruptException("stopped"))) {
      CountingClient client = new CountingClient();
      RecordingStrategy strategy = new RecordingStrategy();
      strategy.failure = transientFailure;
      ProvenanceProjector<String> projector =
          new ProvenanceProjector<>(client, "v1", 10, -1, strategy);
      try {
        assertThrows(SerializationException.class, () -> ask(projector, client));
        assertThrows(SerializationException.class, () -> ask(projector, client));
        assertEquals(2, strategy.asked);
      } finally {
        // InterruptException sets the thread's flag, as a real interruption would.
        Thread.interrupted();
      }
    }
  }

  @Test
  public void eachFailureSaysItsMessageOnceAndKeepsWhereItArose() throws Exception {
    // A cached failure with no cause of its own, then an uncached one wrapping a retriable cause.
    CountingClient client = new CountingClient();
    RecordingStrategy strategy = new RecordingStrategy();
    strategy.returnsNull = true;
    ProvenanceProjector<String> cached =
        new ProvenanceProjector<>(client, "v1", 10, -1, strategy);
    assertThrows(SerializationException.class, () -> ask(cached, client));
    SerializationException none = assertThrows(SerializationException.class,
        () -> ask(cached, client));
    assertTrue(none.getCause() instanceof SerializationException);
    assertEquals(none.getMessage(), none.getCause().getMessage());
    assertNull(none.getCause().getCause());

    strategy.returnsNull = false;
    strategy.failure = new ProvenanceRetriableException("down");
    ProvenanceProjector<String> uncached =
        new ProvenanceProjector<>(client, "v1", 10, -1, strategy);
    SerializationException retried = assertThrows(SerializationException.class,
        () -> ask(uncached, client));
    assertTrue(retried.getCause() instanceof ProvenanceRetriableException);
  }

  @Test
  public void aStrategysRejectionFailsEveryRecordFromThatWriter() throws Exception {
    CountingClient client = new CountingClient();
    RecordingStrategy strategy = new RecordingStrategy();
    strategy.failure = new ProvenanceRejectedException("unknown algorithm");
    ProvenanceProjector<String> projector =
        new ProvenanceProjector<>(client, "v1", 10, -1, strategy);
    for (int record = 0; record < 2; record++) {
      SerializationException e = assertThrows(SerializationException.class,
          () -> ask(projector, client));
      assertTrue(e.getMessage(), e.getMessage().contains("was rejected"));
    }
    assertEquals(1, strategy.asked);
  }

  @Test
  public void aStrategysUnavailableAnswerIsReadWithoutProvenanceAndCached() throws Exception {
    CountingClient client = new CountingClient();
    RecordingStrategy strategy = new RecordingStrategy();
    ProvenanceProjector<String> projector =
        new ProvenanceProjector<>(client, "v1", 10, -1, strategy);
    ask(projector, client);
    ask(projector, client);
    assertEquals(1, strategy.asked);
  }

  @Test
  public void aStrategysAuthFailurePassesThroughAndIsAskedAgain() throws Exception {
    // Thrown directly, as a strategy that is not the registry's client would.
    CountingClient client = new CountingClient();
    RecordingStrategy strategy = new RecordingStrategy();
    ProvenanceProjector<String> projector =
        new ProvenanceProjector<>(client, "v1", 10, -1, strategy);
    strategy.failure = new AuthenticationException("who");
    assertThrows(AuthenticationException.class, () -> ask(projector, client));
    strategy.failure = new AuthorizationException("no");
    assertThrows(AuthorizationException.class, () -> ask(projector, client));
    assertEquals(2, strategy.asked);
  }

  @Test
  public void aStrategyBreakingItsContractFailsEveryRecordFromThatWriter() throws Exception {
    for (RuntimeException broken : Arrays.asList(null,
        new UnsupportedOperationException("not here"), new IllegalStateException("bug"))) {
      CountingClient client = new CountingClient();
      RecordingStrategy strategy = new RecordingStrategy();
      strategy.failure = broken;
      strategy.returnsNull = broken == null;
      ProvenanceProjector<String> projector =
          new ProvenanceProjector<>(client, "v1", 10, -1, strategy);
      assertThrows(SerializationException.class, () -> ask(projector, client));
      assertThrows(SerializationException.class, () -> ask(projector, client));
      assertEquals(1, strategy.asked);
    }
  }

  @Test
  public void aStrategysUnknownWriterIsMatchedByStructure() throws Exception {
    CountingClient client = new CountingClient();
    RecordingStrategy strategy = new RecordingStrategy();
    strategy.knownIn = client;
    ParsedSchema withMetadata = client.writerSchema.copy(
        new Metadata(null, Collections.singletonMap("owner", "x"), null), null);
    int foreign = client.register("other-value", withMetadata);
    ProvenanceProjector<String> projector =
        new ProvenanceProjector<>(client, "v1", 10, -1, strategy);
    projector.project(SUBJECT, new SchemaId(AvroSchema.TYPE, foreign, (String) null),
        withMetadata, client.reader, false, m -> "built");
    assertEquals(2, strategy.asked);
    assertEquals(client.writer, strategy.lastRequest.get(1));
  }

  @Test
  public void aClientThatCannotListSoftDeletedVersionsMatchesNoReaderByStructure()
      throws Exception {
    // Without its soft-deleted versions a match could settle on a later version: read as written.
    CountingClient client = new CountingClient();
    client.listsDeletedVersions = false;
    ParsedSchema respelled = client.reader.copy(
        new Metadata(null, Collections.singletonMap("owner", "x"), null), null);
    ProvenanceProjector<String> projector = new ProvenanceProjector<>(client, "v1", 10, -1);
    assertEquals(Optional.empty(), projector.project(SUBJECT,
        new SchemaId(AvroSchema.TYPE, client.writer, (String) null), client.writerSchema,
        respelled, false, m -> "built"));
    assertEquals(0, client.asked);
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
  public void equalReadersSuppliedWithDifferentIdsKeepTheirOwn() throws Exception {
    // Equal as schemas, as two Avro versions differing only in a doc are, but pinned to
    // different versions: each is asked about by its own.
    CountingClient client = new CountingClient();
    ProvenanceProjector<String> projector = new ProvenanceProjector<>(client, "v1", 10, -1);
    ParsedSchema first = projector.readerSchemas(writer -> ReaderSchema.of(
        new AvroSchema("\"double\""), client.readerId)).apply(client.writerSchema);
    projector.readerSchemas(writer -> ReaderSchema.of(
        new AvroSchema("\"double\""), client.otherWriter)).apply(client.writerSchema);

    projector.project(SUBJECT, new SchemaId(AvroSchema.TYPE, client.writer, (String) null),
        client.writerSchema, first, false, mapping -> "built");
    assertEquals(client.readerId, client.lastReaderId);
  }

  @Test
  public void aReaderPinnedToAVersionIsAskedAboutByIt() throws Exception {
    // One schema id may sit under several versions: the version, not the id, says which.
    CountingClient client = new CountingClient();
    client.provenance = new SchemaProvenance(SUBJECT, Arrays.asList(
        new ProvenanceVersion(1, client.writer, "STRUCT", Collections.emptyList()),
        new ProvenanceVersion(3, client.readerId, "STRUCT", Collections.emptyList())));
    ProvenanceProjector<String> projector = new ProvenanceProjector<>(client, "v1", 10, -1);
    ParsedSchema handedOver = projector.readerSchemas(
        writer -> ReaderSchema.of(client.reader, SUBJECT, 3)).apply(client.writerSchema);

    for (int record = 0; record < 2; record++) {
      Optional<String> built = projector.project(SUBJECT,
          new SchemaId(AvroSchema.TYPE, client.writer, (String) null), client.writerSchema,
          handedOver, false, mapping -> "built");
      assertEquals(Optional.of("built"), built);
    }
    assertEquals(1, client.asked);
    assertEquals(client.writer, client.lastWriterId);
    assertEquals(3, client.lastReaderVersion);
    assertTrue(projector.isPinned(handedOver));
  }

  @Test
  public void aWriterOfThePinnedVersionItselfIsReadAsWritten() throws Exception {
    CountingClient client = new CountingClient();
    client.provenance = new SchemaProvenance(SUBJECT, Collections.singletonList(
        new ProvenanceVersion(3, client.readerId, "STRUCT", Collections.emptyList())));
    ProvenanceProjector<String> projector = new ProvenanceProjector<>(client, "v1", 10, -1);
    ParsedSchema handedOver = projector.readerSchemas(
        writer -> ReaderSchema.of(client.reader, SUBJECT, 3)).apply(client.reader);
    Optional<String> built = projector.project(SUBJECT,
        new SchemaId(AvroSchema.TYPE, client.readerId, (String) null), client.reader,
        handedOver, false, mapping -> "built", () -> "same");
    assertEquals(Optional.of("same"), built);
  }

  @Test
  public void aReaderPinnedToAnotherSubjectsVersionFailsEveryRecord() throws Exception {
    CountingClient client = new CountingClient();
    ProvenanceProjector<String> projector = new ProvenanceProjector<>(client, "v1", 10, -1);
    ParsedSchema handedOver = projector.readerSchemas(
        writer -> ReaderSchema.of(client.reader, "other-value", 3)).apply(client.writerSchema);
    SchemaId id = new SchemaId(AvroSchema.TYPE, client.writer, (String) null);
    for (int record = 0; record < 2; record++) {
      SerializationException e = assertThrows(SerializationException.class, () ->
          projector.project(SUBJECT, id, client.writerSchema, handedOver, false, m -> "built"));
      assertTrue(e.getMessage(), e.getMessage().contains("other-value"));
    }
    assertEquals(0, client.asked);
  }

  @Test
  public void aPinnedVersionTheRegistryDoesNotHaveFailsEveryRecord() throws Exception {
    CountingClient client = new CountingClient();
    client.failure = new RestClientException("Version 9 not found.", 404, 40402);
    ProvenanceProjector<String> projector = new ProvenanceProjector<>(client, "v1", 10, -1);
    ParsedSchema handedOver = projector.readerSchemas(
        writer -> ReaderSchema.of(client.reader, SUBJECT, 9)).apply(client.writerSchema);
    SchemaId id = new SchemaId(AvroSchema.TYPE, client.writer, (String) null);
    for (int record = 0; record < 2; record++) {
      assertThrows(SerializationException.class, () ->
          projector.project(SUBJECT, id, client.writerSchema, handedOver, false, m -> "built"));
    }
    assertEquals(1, client.asked);
  }

  @Test
  public void aStrategyThatCannotPinAVersionFailsEveryRecord() throws Exception {
    // Rather than read against whichever version carries the reader's schema id.
    CountingClient client = new CountingClient();
    ProvenanceStrategy byIdOnly = new ProvenanceStrategy() {
      @Override
      public void configure(Map<String, ?> configs) {
      }

      @Override
      public SchemaProvenance provenance(SchemaRegistryClient c, String subject, int fromId,
          int toId, boolean includeMultipleMessages, String algorithm) {
        throw new AssertionError("asked by schema id");
      }
    };
    ProvenanceProjector<String> projector =
        new ProvenanceProjector<>(client, "v1", 10, -1, byIdOnly);
    ParsedSchema handedOver = projector.readerSchemas(
        writer -> ReaderSchema.of(client.reader, SUBJECT, 3)).apply(client.writerSchema);
    SchemaId id = new SchemaId(AvroSchema.TYPE, client.writer, (String) null);
    SerializationException e = assertThrows(SerializationException.class, () ->
        projector.project(SUBJECT, id, client.writerSchema, handedOver, false, m -> "built"));
    assertTrue(e.getMessage(), e.getMessage().contains("cannot pin"));
  }

  @Test
  public void aPinnedReaderWhoseVersionCannotBeFetchedFailsEveryRecord() throws Exception {
    // The structure check fetches the pinned version: a failure the registry will repeat must
    // fail the records, never fall back to reading without provenance.
    CountingClient client = new CountingClient();
    client.provenance = new SchemaProvenance(SUBJECT, Arrays.asList(
        new ProvenanceVersion(1, client.writer, "STRUCT", Collections.emptyList()),
        new ProvenanceVersion(3, client.readerId, "STRUCT", Collections.emptyList())));
    client.schemaByIdFailure = new RestClientException("Schema not found", 404, 40403);
    ProvenanceProjector<String> projector = new ProvenanceProjector<>(client, "v1", 10, -1);
    ParsedSchema handedOver = projector.readerSchemas(
        writer -> ReaderSchema.of(client.reader, SUBJECT, 3)).apply(client.writerSchema);
    SchemaId id = new SchemaId(AvroSchema.TYPE, client.writer, (String) null);
    assertThrows(SerializationException.class, () ->
        projector.project(SUBJECT, id, client.writerSchema, handedOver, false, m -> "built"));
  }

  @Test
  public void aPinnedVersionMustBeAVersionNumber() {
    assertThrows(IllegalArgumentException.class,
        () -> ReaderSchema.of(new AvroSchema("\"int\""), SUBJECT, -1));
    assertThrows(NullPointerException.class,
        () -> ReaderSchema.of(new AvroSchema("\"int\""), null, 1));
    assertThrows(IllegalArgumentException.class,
        () -> ReaderSchema.of(new AvroSchema("\"int\""), " ", 1));
  }

  @Test
  public void aRejectedRequestFailsEveryRecordFromThatWriter() throws Exception {
    for (int[] rejection : new int[][] {{422, 42202}, {422, 42215}, {404, 40402},
        {422, 42216}}) {
      CountingClient client = new CountingClient();
      client.failure = new RestClientException("rejected", rejection[0], rejection[1]);
      ProvenanceProjector<String> projector = new ProvenanceProjector<>(client, "v1", 10, -1);
      for (int record = 0; record < 2; record++) {
        SerializationException e = assertThrows(SerializationException.class,
            () -> ask(projector, client));
        assertTrue(e.getMessage(), e.getMessage().contains("was rejected"));
      }
      assertEquals(1, client.asked);
    }
  }

  @Test
  public void eachRecordFailingFromOneCachedFailureGetsItsOwnException() throws Exception {
    // A cached failure is rethrown to every record; one instance shared by all would collect
    // whatever each caller added to it.
    CountingClient client = new CountingClient();
    client.failure = new RestClientException("rejected", 422, 42202);
    ProvenanceProjector<String> projector = new ProvenanceProjector<>(client, "v1", 10, -1);
    SerializationException first = assertThrows(SerializationException.class,
        () -> ask(projector, client));
    SerializationException second = assertThrows(SerializationException.class,
        () -> ask(projector, client));
    assertTrue(first != second);
    assertEquals(first.getMessage(), second.getMessage());
    // Said once: the cause is the failure's own, not a copy of the record's.
    assertTrue(first.getCause() instanceof ProvenanceRejectedException);
  }

  @Test
  public void anAuthFailureFailsTheRecordAsASchemaFetchWouldAndIsAskedAgain() throws Exception {
    // Not a fallback: a principal without access to the endpoint must not read without it.
    CountingClient client = new CountingClient();
    client.failure = new RestClientException("unauthorized", 401, 401);
    ProvenanceProjector<String> projector = new ProvenanceProjector<>(client, "v1", 10, -1);
    assertThrows(AuthenticationException.class, () -> ask(projector, client));
    client.failure = new RestClientException("forbidden", 403, 40301);
    assertThrows(AuthorizationException.class, () -> ask(projector, client));
    assertEquals(2, client.asked);
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
        new ProvenanceVersion(1, client.writer, "STRUCT", Collections.emptyList()),
        new ProvenanceVersion(3, client.readerId, "STRUCT", Collections.emptyList())));
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
  public void aStackOverflowInABuildFailsTheRecordAndOtherErrorsOfTheJvmPass() throws Exception {
    // A deep schema may overflow the stack: the record fails, named. An out of memory is the JVM's.
    CountingClient client = new CountingClient();
    client.provenance = new SchemaProvenance(SUBJECT, Arrays.asList(
        new ProvenanceVersion(1, client.writer, "STRUCT", Collections.emptyList()),
        new ProvenanceVersion(3, client.readerId, "STRUCT", Collections.emptyList())));
    ProvenanceProjector<String> projector = new ProvenanceProjector<>(client, "v1", 10, -1);
    SchemaId id = new SchemaId(AvroSchema.TYPE, client.writer, (String) null);
    SerializationException e = assertThrows(SerializationException.class, () ->
        projector.project(SUBJECT, id, client.writerSchema, client.reader, false, mapping -> {
          throw new StackOverflowError();
        }));
    assertTrue(e.getMessage(), e.getMessage().contains("StackOverflowError"));
    assertThrows(OutOfMemoryError.class, () ->
        projector.project(SUBJECT, id, client.writerSchema, client.reader, true, mapping -> {
          throw new OutOfMemoryError();
        }));
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

  // Records what it is asked; fails as set, or answers that provenance is unavailable.
  private static final class RecordingStrategy implements ProvenanceStrategy {

    SchemaRegistryClient client;
    List<Object> lastRequest;
    RuntimeException failure;
    boolean returnsNull;
    // Where set, a writer id under no version of the subject there is unknown.
    CountingClient knownIn;
    int asked;

    @Override
    public void configure(Map<String, ?> configs) {
    }

    @Override
    public SchemaProvenance provenance(SchemaRegistryClient client, String subject, int fromId,
        int toId, boolean includeMultipleMessages, String algorithm) {
      asked++;
      this.client = client;
      lastRequest = Arrays.asList(subject, fromId, toId, includeMultipleMessages, algorithm);
      try {
        if (knownIn != null && !knownIn.idsOf(subject).contains(fromId)) {
          throw new ProvenanceUnknownWriterException("not a version");
        }
      } catch (IOException | RestClientException e) {
        throw new IllegalStateException(e);
      }
      if (failure != null) {
        throw failure;
      }
      if (returnsNull) {
        return null;
      }
      throw new ProvenanceUnavailableException("no provenance here");
    }
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
    int lastReaderVersion;
    String lastAlgorithm;
    boolean rejectsForeignWriters;
    // As a client implementing only the basic lookups, without soft-deleted versions.
    boolean listsDeletedVersions = true;
    RestClientException failure;
    RestClientException schemaByIdFailure;
    SchemaProvenance provenance;
    // Holds every request until released, counting them across threads.
    CountDownLatch gate;
    final AtomicInteger askedAtOnce = new AtomicInteger();

    CountingClient() throws Exception {
      writer = register(SUBJECT, writerSchema);
      otherWriter = register(SUBJECT, new AvroSchema("\"string\""));
      readerId = register(SUBJECT, reader);
    }

    @Override
    public List<Integer> getAllVersions(String subject, boolean lookupDeletedSchema)
        throws IOException, RestClientException {
      if (!listsDeletedVersions) {
        throw new UnsupportedOperationException();
      }
      return super.getAllVersions(subject, lookupDeletedSchema);
    }

    @Override
    public SchemaProvenance getProvenanceById(String subject, int fromId, int toId,
        boolean includeInterior, boolean includeMultipleMessages,
        String algorithm) throws IOException, RestClientException {
      asked++;
      askedAtOnce.incrementAndGet();
      if (gate != null) {
        try {
          gate.await(10, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
        }
      }
      lastWriterId = fromId;
      lastReaderId = toId;
      lastAlgorithm = algorithm;
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

    @Override
    public ParsedSchema getSchemaBySubjectAndId(String subject, int id)
        throws IOException, RestClientException {
      if (schemaByIdFailure != null) {
        throw schemaByIdFailure;
      }
      return super.getSchemaBySubjectAndId(subject, id);
    }

    @Override
    public SchemaProvenance getProvenanceToVersion(String subject, int fromId, int toVersion,
        boolean includeInterior, boolean includeMultipleMessages, String algorithm)
        throws IOException, RestClientException {
      asked++;
      lastWriterId = fromId;
      lastReaderVersion = toVersion;
      lastAlgorithm = algorithm;
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
