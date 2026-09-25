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
import io.confluent.kafka.schemaregistry.client.SchemaMetadata;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.rest.entities.Metadata;
import io.confluent.kafka.schemaregistry.client.rest.entities.RuleSet;
import io.confluent.kafka.schemaregistry.client.rest.entities.SchemaProvenance;
import io.confluent.kafka.schemaregistry.client.rest.entities.SchemaReference;
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;
import io.confluent.kafka.serializers.schema.id.SchemaId;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
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
 * equals once metadata, rules and inline tags are set aside, importing the same schemas:
 * provenance depends on structure alone. A Protobuf schema is also compared normalized, since a
 * generated class spells out what its text leaves implicit. A reader derived from a generated
 * class skips the exact lookup, as its text is synthesized: it is the latest version it equals —
 * normalized for Protobuf, as Avro looks one up for Avro.
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
  // Readers whose registered version the caller named, by the schema handed over, by identity:
  // an equal reader may stand for another version (Avro's equality ignores docs), or for none.
  // A deserializer handing over a copy says so (sameReader).
  private final Cache<ParsedSchema, Integer> suppliedReaderInstances =
      CacheBuilder.newBuilder().weakKeys().build();
  // Readers derived from a generated class, by identity: matched to the latest version they equal.
  private final Cache<ParsedSchema, Boolean> derivedReaders =
      CacheBuilder.newBuilder().weakKeys().build();

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
        suppliedReaderInstances.put(reader.getSchema(), reader.getId());
      }
      return reader.getSchema();
    };
  }

  /**
   * Whether {@code reader} came with the registered id of the version it stands for.
   */
  public boolean suppliesId(ParsedSchema reader) {
    return suppliedId(reader) != null;
  }

  private Integer suppliedId(ParsedSchema reader) {
    return suppliedReaderInstances.getIfPresent(reader);
  }

  /**
   * {@code copy}, which a deserializer made of {@code reader}, standing for the same version:
   * any id supplied with {@code reader} is {@code copy}'s too. The Protobuf deserializer shares one
   * copy among equal readers; equal Protobuf schemas are one registered version, so the id holds
   * for each.
   */
  public ParsedSchema sameReader(ParsedSchema reader, ParsedSchema copy) {
    Integer id = suppliedId(reader);
    if (id != null && copy != reader) {
      suppliedReaderInstances.put(copy, id);
    }
    return copy;
  }

  /**
   * {@code reader}, marked as derived from a generated class. Its text is synthesized from the
   * class, so it is matched to the latest version it equals — normalized for Protobuf, as Avro
   * looks one up for Avro — never by an exact spelling an older version may share by accident.
   */
  public ParsedSchema derivedReader(ParsedSchema reader) {
    derivedReaders.put(reader, Boolean.TRUE);
    return reader;
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
    // A reader derived from a class equals its text, so which of the two it is counts too.
    boolean derived = derivedReaders.getIfPresent(reader) != null;
    List<Object> key = Arrays.asList(subject,
        writerId.getId() != null ? writerId.getId() : writerId.getGuid(), reader, reader.name(),
        includeMultipleMessages, derived ? null : suppliedId(reader), derived);
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
    // Whether a registry error answers the provenance request itself, not a lookup before it.
    boolean requesting = false;
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
        requesting = true;
        provenance = provenance(subject, writerId, readerId, includeMultipleMessages);
      } catch (RestClientException e) {
        // A writer id under no version of the subject: the writer's schema may still equal one.
        requesting = false;
        Integer equal = e.getErrorCode() == SCHEMA_ID_NOT_IN_SUBJECT
            ? structuralMatch(subject, writer, false) : null;
        requesting = true;
        if (equal == null || equal.equals(writerId)) {
          throw e;
        }
        writerId = equal;
        provenance = provenance(subject, writerId, readerId, includeMultipleMessages);
      }
      requesting = false;
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
      if (requesting && isRejectedRequest(e)) {
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
    // An id supplied for an equal text reader is not the class's: it is the latest version it
    // equals.
    boolean derived = derivedReaders.getIfPresent(reader) != null;
    Integer supplied = derived ? null : suppliedId(reader);
    return supplied != null ? supplied : registeredId(subject, reader, derived);
  }

  private Integer registeredId(String subject, ParsedSchema schema)
      throws IOException, RestClientException {
    return registeredId(subject, schema, false);
  }

  /**
   * The schema id {@code schema} is registered under in {@code subject}, or else the latest
   * version it equals once metadata, rules and inline tags are set aside; null when none does. A
   * {@code derived} schema skips the first: only the latest version it equals will do.
   */
  private Integer registeredId(String subject, ParsedSchema schema, boolean derived)
      throws IOException, RestClientException {
    List<Object> key = Arrays.asList(subject, schema, derived);
    Optional<Integer> cached = registeredIds.getIfPresent(key);
    if (cached != null) {
      return cached.orElse(null);
    }
    Integer id = null;
    if (!derived) {
      try {
        id = client.getId(subject, schema);
      } catch (RestClientException e) {
        if (e.getStatus() != 404) {
          throw e;
        }
      }
    }
    if (id == null) {
      id = structuralMatch(subject, schema, derived);
    }
    registeredIds.put(key, Optional.ofNullable(id));
    return id;
  }

  private Integer structuralMatch(String subject, ParsedSchema schema, boolean derived)
      throws IOException, RestClientException {
    List<Integer> versions;
    try {
      versions = client.getAllVersions(subject, true);
    } catch (UnsupportedOperationException e) {
      versions = client.getAllVersions(subject);
    }
    // A schema derived from a generated class spells what its text leaves implicit — qualified
    // type names, map entries, option order — so a Protobuf reader is also matched normalized;
    // a derived one only so, in one pass, so the latest version it equals wins.
    boolean protobuf = "PROTOBUF".equals(schema.schemaType());
    if (derived && !protobuf) {
      return lookupMatch(subject, versions, schema);
    }
    Integer id = derived && protobuf ? null : structuralMatch(subject, versions, schema, false);
    return id == null && protobuf ? structuralMatch(subject, versions, schema, true) : id;
  }

  private Integer structuralMatch(String subject, List<Integer> versions, ParsedSchema schema,
      boolean normalized) throws IOException, RestClientException {
    String wanted = structure(schema, normalized);
    for (int i = versions.size() - 1; i >= 0; i--) {
      int id = schemaIdOf(subject, versions.get(i));
      ParsedSchema version = client.getSchemaBySubjectAndId(subject, id);
      // The text leaves out what it imports: versions alike but for their imports differ.
      if (wanted.equals(structure(version, normalized))
          && imports(schema).equals(imports(version))) {
        return id;
      }
    }
    return null;
  }

  /**
   * The latest version a class-derived {@code schema} can look up, metadata, rules and inline tags
   * set aside: the class's text inlines what a version may reference, and leaves out what a
   * version adds besides.
   */
  private Integer lookupMatch(String subject, List<Integer> versions, ParsedSchema schema)
      throws IOException, RestClientException {
    ParsedSchema wanted = bare(schema);
    for (int i = versions.size() - 1; i >= 0; i--) {
      int id = schemaIdOf(subject, versions.get(i));
      if (wanted.canLookup(bare(client.getSchemaBySubjectAndId(subject, id)), client)) {
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

  /**
   * What {@code schema} imports, by import name: each reference's schema text, a latest version
   * resolved, so references in another order, as {@code -1} or through another subject, match.
   */
  private Map<String, String> imports(ParsedSchema schema)
      throws IOException, RestClientException {
    Map<String, String> imports = new HashMap<>();
    for (SchemaReference reference : schema.references()) {
      SchemaMetadata imported = reference.getVersion() == -1
          ? client.getLatestSchemaMetadata(reference.getSubject())
          : client.getSchemaMetadata(reference.getSubject(), reference.getVersion());
      imports.put(reference.getName(), imported.getSchema());
    }
    return imports;
  }

  private static String structure(ParsedSchema schema, boolean normalized) {
    ParsedSchema bare = bare(schema);
    return (normalized ? bare.normalize() : bare).canonicalString();
  }

  private static ParsedSchema bare(ParsedSchema schema) {
    ParsedSchema bare = schema.copy((Metadata) null, (RuleSet) null);
    return bare.copy(Collections.emptyMap(), bare.inlineTaggedEntities());
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
        // A fresh one each time: a caller may add to it, and it is shared by every record.
        throw new SerializationException(failure.getMessage(), failure);
      }
      return Optional.ofNullable(value);
    }
  }
}
