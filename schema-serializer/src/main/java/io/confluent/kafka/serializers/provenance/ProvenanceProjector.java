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

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import io.confluent.kafka.schemaregistry.ParsedSchema;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.rest.entities.Metadata;
import io.confluent.kafka.schemaregistry.client.rest.entities.RuleSet;
import io.confluent.kafka.schemaregistry.client.rest.entities.SchemaProvenance;
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;
import io.confluent.kafka.serializers.schema.id.SchemaId;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import org.apache.kafka.common.errors.SerializationException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Builds, once per writer schema and reader, whatever a deserializer needs to read the writer
 * paired with the reader by provenance, and caches it.
 *
 * <p>Provenance pairs registered versions by schema id. A reader's id is the one its caller
 * supplied, else the one it is registered under; a writer's is the one its record carries. Where
 * that is missing — a record naming its schema by GUID — or names no version of the subject, it
 * is looked up the same way. A schema matching no version exactly takes the latest version it
 * equals once metadata, rules and inline tags are set aside: provenance depends on structure alone.
 *
 * <p>Failures are told apart by what asking again could change. A transient one — the registry
 * unreachable, a 5xx, 408 or 429 — fails the record and is never cached. Provenance that is
 * unavailable for the pair — any other error, a reader that is not a version of the subject, a
 * pairing no single schema can express — is cached and warned about, and the writer is read as
 * without provenance. Anything else the build throws fails the record, and is cached so that one
 * unreadable writer costs one build rather than one per record. Cached outcomes expire after
 * {@code provenance.cache.ttl.sec} and are then worked out afresh, warning included, so a fallback
 * is not permanent.
 *
 * @param <T> what the build produces
 */
public final class ProvenanceProjector<T> {

  private static final Logger log = LoggerFactory.getLogger(ProvenanceProjector.class);

  // The registry's answer for a schema id with no version under the subject.
  private static final int SCHEMA_ID_NOT_IN_SUBJECT = 40411;

  private final SchemaRegistryClient client;
  private final String algorithm;
  private final Cache<List<Object>, Outcome<T>> outcomes;
  // Schemas' registered ids under a subject, as looked up.
  private final Cache<List<Object>, Optional<Integer>> registeredIds;
  // Readers whose registered version the caller named, by the schema handed over.
  private final Cache<ParsedSchema, Integer> suppliedReaderIds;

  /**
   * A projector asking {@code client} by {@code algorithm}, caching up to {@code cacheSize}
   * entries of each kind for {@code cacheTtlSec} seconds, or indefinitely when that is negative.
   */
  public ProvenanceProjector(SchemaRegistryClient client, String algorithm, int cacheSize,
      int cacheTtlSec) {
    this.client = client;
    this.algorithm = algorithm;
    this.outcomes = cache(cacheSize, cacheTtlSec);
    this.registeredIds = cache(cacheSize, cacheTtlSec);
    this.suppliedReaderIds = cache(cacheSize, cacheTtlSec);
  }

  /**
   * {@code readers} as the reader function the deserializers take, remembering the registered id
   * each reader comes with so it is used instead of being looked up.
   */
  public Function<ParsedSchema, ParsedSchema> readerSchemas(
      Function<ParsedSchema, ReaderSchema> readers) {
    return writer -> {
      ReaderSchema reader = readers.apply(writer);
      if (reader == null) {
        return null;
      }
      if (reader.getId() != null) {
        suppliedReaderIds.put(reader.getSchema(), reader.getId());
      }
      return reader.getSchema();
    };
  }

  private static <K, V> Cache<K, V> cache(int size, int ttlSec) {
    CacheBuilder<Object, Object> builder = CacheBuilder.newBuilder().maximumSize(size);
    if (ttlSec >= 0) {
      builder = builder.expireAfterWrite(ttlSec, TimeUnit.SECONDS);
    }
    return builder.build();
  }

  /**
   * What {@code build} makes of the pairing of {@code writer} with {@code reader}, or empty when
   * the writer is to be read without provenance.
   *
   * @throws SerializationException if provenance could not be had now but might later, or the
   *     build failed
   */
  public Optional<T> project(String subject, SchemaId writerId, ParsedSchema writer,
      ParsedSchema reader, boolean includeMultipleMessages,
      Function<ProvenanceMapping, T> build) {
    if (subject == null || reader == null || !names(writerId)) {
      return Optional.empty();
    }
    // A writer named by GUID alone is keyed by it; its schema id is looked up when computed. A
    // supplied reader id is part of the question: the same schema may stand for either version.
    // So is the reader's name: Protobuf schemas are equal whichever message of the file they name.
    List<Object> key = Arrays.asList(subject,
        writerId.getId() != null ? writerId.getId() : writerId.getGuid(), reader, reader.name(),
        includeMultipleMessages, suppliedReaderIds.getIfPresent(reader));
    Outcome<T> outcome = outcomes.getIfPresent(key);
    if (outcome == null) {
      outcome = compute(subject, writerId, writer, reader, includeMultipleMessages, build);
      outcomes.put(key, outcome);
    }
    return outcome.get();
  }

  private Outcome<T> compute(String subject, SchemaId writerSchemaId, ParsedSchema writer,
      ParsedSchema reader, boolean includeMultipleMessages, Function<ProvenanceMapping, T> build) {
    String written = writerSchemaId.getId() != null
        ? "schema id " + writerSchemaId.getId() : "schema GUID " + writerSchemaId.getGuid();
    Integer writerId = writerSchemaId.getId();
    Integer readerId = null;
    try {
      if (writerId == null) {
        writerId = registeredId(subject, writer);
        if (writerId == null) {
          throw new ProvenanceUnavailableException(
              "The writer schema is not a version of subject " + subject);
        }
      }
      readerId = readerId(subject, reader);
      if (readerId == null) {
        throw new ProvenanceUnavailableException(
            "The reader schema is not a version of subject " + subject);
      }
      SchemaProvenance provenance;
      try {
        provenance = provenance(subject, writerId, readerId, includeMultipleMessages);
      } catch (RestClientException e) {
        // A writer id under no version of the subject: the writer's schema may still equal one.
        Integer equal = e.getErrorCode() == SCHEMA_ID_NOT_IN_SUBJECT
            ? structuralMatch(subject, writer) : null;
        if (equal == null || equal.equals(writerId)) {
          throw e;
        }
        writerId = equal;
        provenance = provenance(subject, writerId, readerId, includeMultipleMessages);
      }
      if (provenance == null) {
        return Outcome.unavailable();
      }
      return Outcome.of(build.apply(ProvenanceMapping.join(provenance, writerId, readerId)));
    } catch (IOException e) {
      throw new SerializationException(
          "Could not reach Schema Registry for the provenance of " + written, e);
    } catch (RestClientException e) {
      if (isTransient(e.getStatus())) {
        throw new SerializationException(
            "Schema Registry could not serve provenance for " + written, e);
      }
      if (isRejectedRequest(e)) {
        // The request is always two schema ids, well formed: a rejection of it cannot be the
        // schemas' doing.
        return failed(subject, written, new SerializationException("Schema Registry rejected "
            + "the provenance request for schema ids " + writerId + " and " + readerId + ": "
            + e.getMessage(), e));
      }
      return unavailable(subject, written, e);
    } catch (ProvenanceUnavailableException | UnsupportedOperationException e) {
      return unavailable(subject, written, e);
    } catch (RuntimeException e) {
      return failed(subject, written, e instanceof SerializationException
          ? (SerializationException) e
          : new SerializationException("Could not project " + written
              + " by provenance: " + e.getMessage(), e));
    }
  }

  /**
   * Whether {@code schemaId} names a schema, by id or by GUID.
   */
  private static boolean names(SchemaId schemaId) {
    return schemaId != null && (schemaId.getId() != null || schemaId.getGuid() != null);
  }

  /**
   * The provenance pairing two versions; null when they are one and the same.
   */
  private SchemaProvenance provenance(String subject, int writerId, int readerId,
      boolean includeMultipleMessages) throws IOException, RestClientException {
    return writerId == readerId ? null : client.getProvenanceById(
        subject, writerId, readerId, false, includeMultipleMessages, algorithm);
  }

  private Outcome<T> failed(String subject, String written, SerializationException e) {
    // Logged here, where the outcome is cached, so once per writer schema, not per record.
    log.error("Records of {} of subject {} cannot be read by provenance: {}",
        written, subject, e.getMessage());
    return Outcome.failed(e);
  }

  /** A rejection of a request the projector never makes: a bad version, request or range. */
  private static boolean isRejectedRequest(RestClientException e) {
    return e.getErrorCode() == 42202 || e.getErrorCode() == 42215 || e.getErrorCode() == 40402;
  }

  private Outcome<T> unavailable(String subject, String written, Exception e) {
    // Logged here, where the outcome is cached, so once per writer schema, not per record.
    log.warn("No provenance for {} of subject {}; reading it without provenance. {}",
        written, subject, e.getMessage());
    return Outcome.unavailable();
  }

  /**
   * The schema id of {@code reader} under {@code subject}: the one its caller supplied, else the
   * one it is registered under. A reader with a writer's rules merged onto it matches no version
   * exactly, but has the structure of the version it was pinned to.
   */
  private Integer readerId(String subject, ParsedSchema reader)
      throws IOException, RestClientException {
    Integer supplied = suppliedReaderIds.getIfPresent(reader);
    return supplied != null ? supplied : registeredId(subject, reader);
  }

  /**
   * The schema id {@code schema} is registered under in {@code subject}, or else the latest
   * version it equals once metadata, rules and inline tags are set aside; null when none does.
   */
  private Integer registeredId(String subject, ParsedSchema schema)
      throws IOException, RestClientException {
    List<Object> key = Arrays.asList(subject, schema);
    Optional<Integer> cached = registeredIds.getIfPresent(key);
    if (cached != null) {
      return cached.orElse(null);
    }
    Integer id;
    try {
      id = client.getId(subject, schema);
    } catch (RestClientException e) {
      if (e.getStatus() != 404) {
        throw e;
      }
      id = structuralMatch(subject, schema);
    }
    registeredIds.put(key, Optional.ofNullable(id));
    return id;
  }

  private Integer structuralMatch(String subject, ParsedSchema schema)
      throws IOException, RestClientException {
    List<Integer> versions;
    try {
      versions = client.getAllVersions(subject, true);
    } catch (UnsupportedOperationException e) {
      versions = client.getAllVersions(subject);
    }
    Integer id = structuralMatch(subject, versions, schema, false);
    // A schema derived from a generated class spells what its text leaves implicit — qualified
    // type names, map entries, option order — so a Protobuf reader is also matched normalized.
    return id == null && "PROTOBUF".equals(schema.schemaType())
        ? structuralMatch(subject, versions, schema, true) : id;
  }

  private Integer structuralMatch(String subject, List<Integer> versions, ParsedSchema schema,
      boolean normalized) throws IOException, RestClientException {
    String wanted = structure(schema, normalized);
    for (int i = versions.size() - 1; i >= 0; i--) {
      int id = schemaIdOf(subject, versions.get(i));
      if (wanted.equals(structure(client.getSchemaBySubjectAndId(subject, id), normalized))) {
        return id;
      }
    }
    return null;
  }

  // A soft-deleted version is still a version: old records and pinned readers use them.
  private int schemaIdOf(String subject, int version) throws IOException, RestClientException {
    try {
      return client.getSchemaMetadata(subject, version, true).getId();
    } catch (UnsupportedOperationException e) {
      return client.getSchemaMetadata(subject, version).getId();
    }
  }

  private static String structure(ParsedSchema schema, boolean normalized) {
    ParsedSchema bare = schema.copy((Metadata) null, (RuleSet) null);
    bare = bare.copy(Collections.emptyMap(), bare.inlineTaggedEntities());
    return (normalized ? bare.normalize() : bare).canonicalString();
  }

  static boolean isTransient(int status) {
    return status >= 500 || status == 408 || status == 429;
  }

  private static final class Outcome<T> {
    private final T value;
    private final SerializationException failure;

    private Outcome(T value, SerializationException failure) {
      this.value = value;
      this.failure = failure;
    }

    static <T> Outcome<T> of(T value) {
      return new Outcome<>(value, null);
    }

    static <T> Outcome<T> unavailable() {
      return new Outcome<>(null, null);
    }

    static <T> Outcome<T> failed(SerializationException failure) {
      return new Outcome<>(null, failure);
    }

    Optional<T> get() {
      if (failure != null) {
        throw failure;
      }
      return Optional.ofNullable(value);
    }
  }
}
