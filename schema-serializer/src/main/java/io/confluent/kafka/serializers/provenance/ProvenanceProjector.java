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
import com.google.common.util.concurrent.ExecutionError;
import com.google.common.util.concurrent.UncheckedExecutionException;
import io.confluent.kafka.schemaregistry.ParsedSchema;
import io.confluent.kafka.schemaregistry.avro.AvroSchema;
import io.confluent.kafka.schemaregistry.client.SchemaMetadata;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.rest.entities.SchemaProvenance;
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;
import io.confluent.kafka.schemaregistry.type.logical.LogicalType;
import io.confluent.kafka.schemaregistry.type.logical.SchemaType;
import io.confluent.kafka.schemaregistry.type.logical.provenance.ProvenanceHistory;
import io.confluent.kafka.schemaregistry.utils.QualifiedSubject;
import io.confluent.kafka.serializers.provenance.strategy.ClientProvenanceStrategy;
import io.confluent.kafka.serializers.provenance.strategy.ProvenanceStrategy;
import io.confluent.kafka.serializers.schema.id.SchemaId;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.function.Supplier;
import org.apache.kafka.common.errors.AuthenticationException;
import org.apache.kafka.common.errors.AuthorizationException;
import org.apache.kafka.common.errors.InterruptException;
import org.apache.kafka.common.errors.RetriableException;
import org.apache.kafka.common.errors.SerializationException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Builds, once per writer schema and reader, whatever a deserializer needs to read the writer
 * paired with the reader by provenance, and caches it.
 *
 * <p>Provenance pairs registered versions by schema id. A reader's id is the one its caller
 * supplied, else the one it is registered under; a writer's is the one its record carries. A
 * reader its caller pinned to a subject version is asked about by that version instead, as one
 * schema id may sit under several versions. Where
 * that is missing — a record naming its schema by GUID — or names no version of the subject, it
 * is looked up the same way. A schema matching no version exactly takes the latest version of its
 * own schema type whose logical type is equivalent to its own under that type's rules
 * ({@link LogicalType#equivalent}): provenance depends on the data alone, not on docs, options,
 * metadata, rules, the order members are declared in, or how references are spelled. A reader
 * derived from a generated class skips the exact lookup, as its text is synthesized: it is the
 * latest version equivalent to it. A writer read by a reader of its own version asks nothing of
 * the registry.
 *
 * <p>Provenance comes from a {@link ProvenanceStrategy}, by default Schema Registry, whose failures
 * are typed as its contract says. Failures are told apart by what asking again could change. A
 * transient one — the source or the registry unreachable, a 5xx, 408 or 429 — fails the record and
 * is never cached, so every record asks again, even for a server failure that recurs; so does a
 * 401 or 403, as the {@code AuthenticationException} or {@code AuthorizationException} a schema
 * fetch gives. A rejected request, such as for an unknown algorithm, fails every record of the
 * pair. Provenance that is unavailable for the pair — a reader that is not a version of the
 * subject, a pairing no single schema can express — is cached and warned about, and the writer is
 * read as without provenance. Anything else the build or the strategy throws fails the record,
 * and is cached so that one unreadable writer costs one build rather than one per record. Cached
 * outcomes expire after {@code provenance.cache.ttl.sec} and are then worked out afresh, warning
 * included, so a fallback is not permanent; with no TTL, a fallback or failure lasts until the
 * deserializer is reconfigured.
 *
 * @param <T> what the build produces
 */
public final class ProvenanceProjector<T> {

  private static final Logger log = LoggerFactory.getLogger(ProvenanceProjector.class);

  private final SchemaRegistryClient client;
  private final String algorithm;
  private final Cache<List<Object>, Outcome<T>> outcomes;
  // Schemas' registered ids under a subject, as looked up.
  private final Cache<List<Object>, Optional<Integer>> registeredIds;
  // Which readers were pinned or derived: the deserializer's, outliving this projector. A
  // deserializer handing over a copy of a pinned reader of its own says so (sameReader).
  private final ProvenanceReaderMarks marks;
  // Where provenance comes from.
  private final ProvenanceStrategy strategy;
  // Schemas' logical types, by identity, as compared to find the version a schema stands for;
  // empty for one with no logical form.
  private final Cache<ParsedSchema, Optional<LogicalType>> logicalTypes =
      CacheBuilder.newBuilder().weakKeys().build();

  /**
   * A projector asking {@code client} by {@code algorithm}, caching up to {@code cacheSize}
   * entries of each kind for {@code cacheTtlSec} seconds, or indefinitely when that is negative.
   */
  public ProvenanceProjector(SchemaRegistryClient client, String algorithm, int cacheSize,
      int cacheTtlSec) {
    this(client, algorithm, cacheSize, cacheTtlSec, null);
  }

  /**
   * As {@link #ProvenanceProjector(SchemaRegistryClient, String, int, int)}, getting provenance
   * from {@code strategy} rather than from Schema Registry.
   */
  public ProvenanceProjector(SchemaRegistryClient client, String algorithm, int cacheSize,
      int cacheTtlSec, ProvenanceStrategy strategy) {
    this(client, algorithm, cacheSize, cacheTtlSec, strategy, new ProvenanceReaderMarks());
  }

  /**
   * As {@link #ProvenanceProjector(SchemaRegistryClient, String, int, int, ProvenanceStrategy)},
   * keeping which readers were pinned or derived in {@code marks}, which a deserializer keeps
   * across reconfigures.
   */
  public ProvenanceProjector(SchemaRegistryClient client, String algorithm, int cacheSize,
      int cacheTtlSec, ProvenanceStrategy strategy, ProvenanceReaderMarks marks) {
    this.client = client;
    this.marks = marks;
    this.strategy = strategy != null ? strategy : new ClientProvenanceStrategy();
    this.algorithm = algorithm;
    this.outcomes = cache(cacheSize, cacheTtlSec);
    this.registeredIds = cache(cacheSize, cacheTtlSec);
  }

  /**
   * {@code readers} as the reader function the deserializers take, remembering the registered id
   * or version each reader comes with so it is used instead of being looked up.
   */
  public Function<ParsedSchema, ParsedSchema> readerSchemas(
      Function<ParsedSchema, ReaderSchema> readers) {
    return writer -> {
      ReaderSchema reader = readers.apply(writer);
      if (reader == null) {
        return null;
      }
      if (reader.getId() == null && reader.getVersion() == null) {
        return reader.getSchema();
      }
      return marks.pinnedCopy(reader.getSchema(),
          new Pin(reader.getId(), reader.getSubject(), reader.getVersion()));
    };
  }

  /**
   * Whether {@code reader} came with the registered id or version it stands for.
   */
  public boolean isPinned(ParsedSchema reader) {
    return pinOf(reader) != null;
  }

  private Pin pinOf(ParsedSchema reader) {
    return marks.pinOf(reader);
  }

  /**
   * {@code copy}, which a deserializer made of {@code reader}, as it stands for the same version:
   * pinned as {@code reader} is, through a copy of it kept for that pin. The Protobuf deserializer
   * shares one copy among equal readers, pinned otherwise or not at all, so it is never marked.
   */
  public ParsedSchema sameReader(ParsedSchema reader, ParsedSchema copy) {
    Pin pin = pinOf(reader);
    // The deserializer may share copy among equal readers, pinned otherwise or not at all: the
    // pin goes on a copy of it kept for this pin.
    return pin == null || copy == reader ? copy : marks.pinnedCopy(copy, pin);
  }


  /**
   * {@code reader}, marked as derived from an application class (an Avro or Protobuf generated
   * class, a JSON Schema POJO). Its text is synthesized from the class, so it is matched to the
   * latest version whose logical type is equivalent to it, never by an exact spelling an older
   * version may share by accident.
   */
  public ParsedSchema derivedReader(ParsedSchema reader) {
    marks.markDerived(reader);
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
   * @throws AuthenticationException if the request was not authenticated
   * @throws AuthorizationException if it was not authorized
   */
  public Optional<T> project(String subject, SchemaId writerId, ParsedSchema writer,
      ParsedSchema reader, boolean includeMultipleMessages,
      Function<ProvenanceMapping, T> build) {
    return project(subject, writerId, writer, reader, includeMultipleMessages, build, null);
  }

  /**
   * As {@link #project(String, SchemaId, ParsedSchema, ParsedSchema, boolean, Function)}, with a
   * writer read by a reader of its own version read as {@code sameVersion} makes it, asking
   * nothing of the registry; with none, or when it makes none, read as written.
   */
  public Optional<T> project(String subject, SchemaId writerId, ParsedSchema writer,
      ParsedSchema reader, boolean includeMultipleMessages,
      Function<ProvenanceMapping, T> build, Supplier<T> sameVersion) {
    if (subject == null || reader == null || !names(writerId)) {
      return Optional.empty();
    }
    // A writer named by GUID alone is keyed by it; its schema id is looked up when computed. A
    // supplied reader id or version is part of the question: the same schema may stand for
    // either version.
    // So are the writer's and reader's names: a Protobuf file's messages share its schema id, and
    // its schemas are equal whichever message they name. A reader derived from a class equals its
    // text, so which of the two it is counts too.
    boolean derived = marks.isDerived(reader);
    // Read once: the key and the computation must see the same pin.
    Pin pin = derived ? null : pinOf(reader);
    List<Object> key = Arrays.asList(subject,
        writerId.getId() != null ? writerId.getId() : writerId.getGuid(),
        writer != null ? writer.name() : null, reader, reader.name(),
        includeMultipleMessages, pin, derived);
    Outcome<T> outcome;
    try {
      // Loaded atomically: the first records of a pair, however many at once, ask once.
      outcome = outcomes.get(key, () -> compute(subject, writerId, writer, reader, pin,
          includeMultipleMessages, build, sameVersion));
    } catch (ExecutionException | UncheckedExecutionException | ExecutionError e) {
      throw fresh(e.getCause());
    }
    return outcome.get();
  }

  /**
   * What a load that compute failed throws to the record: a fresh exception for each, as every
   * thread waiting on the load is handed the same cause. A stack overflow, as a deep schema may
   * cause, fails the record; any other error of the JVM is its own.
   */
  private static RuntimeException fresh(Throwable cause) {
    if (cause instanceof VirtualMachineError && !(cause instanceof StackOverflowError)) {
      throw (VirtualMachineError) cause;
    }
    if (cause instanceof AuthenticationException) {
      return new AuthenticationException(cause.getMessage(), cause);
    }
    if (cause instanceof AuthorizationException) {
      return new AuthorizationException(cause.getMessage(), cause);
    }
    // A failure of our own lends its cause, so the chain says the message once.
    Throwable under = cause instanceof SerializationException && cause.getCause() != null
        ? cause.getCause() : cause;
    return new SerializationException(cause.getMessage() != null ? cause.getMessage()
        : "Could not project by provenance: " + cause, under);
  }

  private Outcome<T> compute(String subject, SchemaId writerSchemaId, ParsedSchema writer,
      ParsedSchema reader, Pin pin, boolean includeMultipleMessages,
      Function<ProvenanceMapping, T> build, Supplier<T> sameVersion) {
    String written = writerSchemaId.getId() != null
        ? "schema id " + writerSchemaId.getId() : "schema GUID " + writerSchemaId.getGuid();
    Integer writerId = writerSchemaId.getId();
    try {
      if (writerId == null) {
        writerId = registeredId(subject, writer);
        if (writerId == null) {
          throw new ProvenanceUnavailableException(
              "The writer schema is not a version of subject " + subject);
        }
      }
      Integer readerVersion = pinnedVersion(subject, pin);
      Integer readerId = readerVersion != null ? null : readerId(subject, reader, pin);
      if (readerVersion == null && readerId == null) {
        throw new ProvenanceUnavailableException(
            "The reader schema is not a version of subject " + subject);
      }
      SchemaProvenance provenance;
      try {
        provenance = provenance(
            subject, writerId, readerId, readerVersion, includeMultipleMessages);
      } catch (ProvenanceUnknownWriterException e) {
        // A reader pinned to an id no version of the subject carries is the caller's mistake, as
        // a missing pinned version is: every record of the writer fails.
        if (pin != null && pin.id != null && !carriesId(subject, pin.id)) {
          throw new SerializationException("The reader is pinned to schema id " + pin.id
              + ", which no version of subject " + subject + " carries");
        }
        // A writer id under no version of the subject: the writer's schema may still equal one.
        Integer equal = structuralMatch(subject, writer);
        if (equal == null || equal.equals(writerId)) {
          throw e;
        }
        writerId = equal;
        provenance = provenance(
            subject, writerId, readerId, readerVersion, includeMultipleMessages);
      }
      if (pin != null) {
        requireStructureOf(subject, reader, pin);
      }
      ProvenanceMapping mapping = provenance == null ? null
          : readerVersion != null ? ProvenanceMapping.joinToVersion(provenance, readerVersion)
          : ProvenanceMapping.join(provenance, writerId, readerId);
      if (mapping == null) {
        // One and the same version: nothing to pair.
        return sameVersion != null ? Outcome.of(sameVersion.get()) : Outcome.unavailable();
      }
      return Outcome.of(build.apply(mapping));
    } catch (IOException e) {
      throw new SerializationException(
          "Could not reach Schema Registry for the provenance of " + written, e);
    } catch (RestClientException e) {
      if (ClientProvenanceStrategy.isTransient(e.getStatus())) {
        throw new SerializationException(
            "Schema Registry could not serve provenance for " + written, e);
      }
      if (e.getStatus() == 401) {
        throw new AuthenticationException(
            "Not authenticated to Schema Registry for the provenance of " + written, e);
      }
      if (e.getStatus() == 403) {
        throw new AuthorizationException(
            "Not authorized by Schema Registry for the provenance of " + written, e);
      }
      return unavailable(subject, written, e);
    } catch (ProvenanceRetriableException e) {
      throw new SerializationException(
          "Could not get the provenance of " + written + ": " + e.getMessage(), e);
    } catch (UncheckedIOException | RetriableException | InterruptException e) {
      // The deserializer's client failing transiently, as the strategy's contract counts it:
      // the record fails, uncached, and the next asks again.
      throw new SerializationException(
          "Could not reach Schema Registry for the provenance of " + written, e);
    } catch (AuthenticationException | AuthorizationException e) {
      throw e;
    } catch (ProvenanceUnavailableException | ProvenanceUnknownWriterException
        | UnsupportedOperationException e) {
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
   * The provenance pairing the writer's version with the reader's, the reader named by schema id
   * or, when pinned, by version; null when two schema ids are one and the same.
   *
   * @throws SerializationException if the request itself is rejected — a bad version, request or
   *     range, or an unknown algorithm — or the strategy breaks its contract: every record of the
   *     writer fails
   */
  private SchemaProvenance provenance(String subject, int writerId, Integer readerId,
      Integer readerVersion, boolean includeMultipleMessages) {
    if (readerVersion == null && writerId == readerId) {
      return null;
    }
    String pair = "schema id " + writerId + " and "
        + (readerVersion != null ? "version " + readerVersion : "schema id " + readerId);
    SchemaProvenance provenance;
    try {
      provenance = readerVersion != null
          ? strategy.provenanceToVersion(client, subject, writerId, readerVersion, false,
              includeMultipleMessages, algorithm)
          : strategy.provenance(
              client, subject, writerId, readerId, false, includeMultipleMessages, algorithm);
    } catch (ProvenanceRejectedException e) {
      throw new SerializationException(
          "The provenance request for " + pair + " was rejected: " + e.getMessage(), e);
    } catch (ProvenanceException | AuthenticationException | AuthorizationException e) {
      throw e;
    } catch (UncheckedIOException | RetriableException | InterruptException e) {
      // Transient by nature, whichever strategy threw it: the record fails, the next asks again.
      throw new ProvenanceRetriableException(e.getMessage(), e);
    } catch (RuntimeException e) {
      throw new SerializationException(
          "The provenance strategy failed for " + pair + ": " + e.getMessage(), e);
    }
    if (provenance == null) {
      throw new SerializationException("The provenance strategy returned none for " + pair);
    }
    return provenance;
  }

  private Outcome<T> failed(String subject, String written, SerializationException e) {
    // Logged here, where the outcome is cached, so once per writer schema, not per record.
    log.error("Records of {} of subject {} cannot be read by provenance: {}",
        written, subject, e.getMessage());
    return Outcome.failed(e);
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
  private Integer readerId(String subject, ParsedSchema reader, Pin pin)
      throws IOException, RestClientException {
    // A derived reader carries no pin: it is the latest version it equals.
    return pin != null && pin.id != null ? pin.id
        : registeredId(subject, reader, marks.isDerived(reader));
  }

  /**
   * Fails unless {@code reader} has the structure of the version it is pinned to, whatever its
   * docs, metadata, rules or member order: provenance pairs by that version, so a reader of
   * another structure would hand its own new fields that version's values.
   */
  private void requireStructureOf(String subject, ParsedSchema reader, Pin pin)
      throws IOException, RestClientException {
    ParsedSchema version;
    try {
      int id = pin.id != null ? pin.id : metadataOf(subject, pin.version).getId();
      version = client.getSchemaBySubjectAndId(subject, id);
    } catch (RestClientException e) {
      // Provenance was just had for this version: a failure that will recur fails the records
      // rather than read them without provenance.
      if (ClientProvenanceStrategy.isTransient(e.getStatus()) || e.getStatus() == 401
          || e.getStatus() == 403) {
        throw e;
      }
      throw new SerializationException("The version the reader is pinned to, " + pin
          + " of subject " + subject + ", could not be fetched: " + e.getMessage(), e);
    }
    Optional<LogicalType> pinned = logicalTypeOf(version);
    Optional<LogicalType> own = logicalTypeOf(reader);
    if (pinned.isPresent() && own.isPresent()
        && !own.get().equivalent(SchemaType.of(reader.schemaType()), pinned.get())) {
      throw new SerializationException("The reader is pinned to " + pin + " of subject "
          + subject + ", whose structure is not the reader's");
    }
  }

  // Whether some version of subject, soft-deleted or not, carries schema id id.
  private boolean carriesId(String subject, int id) throws IOException, RestClientException {
    for (int version : client.getAllVersions(subject, true)) {
      if (metadataOf(subject, version).getId() == id) {
        return true;
      }
    }
    return false;
  }

  /**
   * The version {@code pin} names, or null if it names none.
   *
   * @throws SerializationException if it is pinned to a version of another subject: every record
   *     of the writer fails
   */
  private Integer pinnedVersion(String subject, Pin pin) {
    if (pin == null || pin.version == null) {
      return null;
    }
    if (!namesSubject(pin.subject, subject)) {
      throw new SerializationException("The reader is pinned to version " + pin.version
          + " of subject " + pin.subject + ", but the record's subject is " + subject);
    }
    return pin.version;
  }

  // Whether pinned names the record's subject: in its context, or naming none, in the record's.
  private static boolean namesSubject(String pinned, String subject) {
    if (pinned.equals(subject)) {
      return true;
    }
    QualifiedSubject record = QualifiedSubject.create(QualifiedSubject.DEFAULT_TENANT, subject);
    if (record == null) {
      return false;
    }
    if (!pinned.startsWith(QualifiedSubject.CONTEXT_PREFIX)) {
      return pinned.equals(record.getSubject());
    }
    QualifiedSubject pin = QualifiedSubject.create(QualifiedSubject.DEFAULT_TENANT, pinned);
    return pin != null && pin.getContext().equals(record.getContext())
        && pin.getSubject().equals(record.getSubject());
  }

  private Integer registeredId(String subject, ParsedSchema schema)
      throws IOException, RestClientException {
    return registeredId(subject, schema, false);
  }

  /**
   * The schema id {@code schema} is registered under in {@code subject}, soft-deleted versions
   * included, or else the latest version whose logical type is equivalent to its own; null when
   * none is. A {@code derived} schema skips the first: only the latest version it equals will do.
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
      // By version, not by id: the registry's id lookup skips soft-deleted versions, which a
      // pinned reader or an old writer may still be.
      try {
        id = metadataOf(subject, client.getVersion(subject, schema)).getId();
      } catch (RestClientException e) {
        if (e.getStatus() != 404) {
          throw e;
        }
      }
    }
    if (id == null) {
      id = structuralMatch(subject, schema);
    }
    registeredIds.put(key, Optional.ofNullable(id));
    return id;
  }

  /**
   * The latest version of {@code subject} of {@code schema}'s type whose logical type is
   * equivalent to {@code schema}'s under that format's rules: the same data, whatever docs,
   * options, services or other annotations either spells out, in whatever order it declares its
   * members, and whatever it imports or inlines. Null when none is, or {@code schema} has no
   * logical form.
   */
  private Integer structuralMatch(String subject, ParsedSchema schema)
      throws IOException, RestClientException {
    Optional<LogicalType> wanted = logicalTypeOf(schema);
    if (!wanted.isPresent()) {
      return null;
    }
    SchemaType schemaType = SchemaType.of(schema.schemaType());
    // Soft-deleted versions included: without them a match could settle on a later version. A
    // client that cannot list them throws, and the pair is read without provenance.
    List<Integer> versions = client.getAllVersions(subject, true);
    for (int i = versions.size() - 1; i >= 0; i--) {
      SchemaMetadata metadata = metadataOf(subject, versions.get(i));
      // Another format's version is skipped unparsed: this client may have no provider for it.
      String type = metadata.getSchemaType() != null ? metadata.getSchemaType() : AvroSchema.TYPE;
      if (!schema.schemaType().equals(type)) {
        continue;
      }
      // Only its own format's versions are fetched, so a failure is the lookup's own: it fails
      // the lookup rather than let the scan settle on an older version the reader may not be.
      Optional<LogicalType> version =
          logicalTypeOf(client.getSchemaBySubjectAndId(subject, metadata.getId()));
      if (version.isPresent() && wanted.get().equivalent(schemaType, version.get())) {
        return metadata.getId();
      }
    }
    return null;
  }

  /**
   * {@code schema}'s logical type as provenance computes it; a Protobuf file's over all its
   * top-level messages, as a version stands for the whole file whichever message a reader names.
   */
  private Optional<LogicalType> logicalTypeOf(ParsedSchema schema) {
    Optional<LogicalType> known = logicalTypes.getIfPresent(schema);
    if (known == null) {
      try {
        known = Optional.of(ProvenanceHistory.logicalTypeOf(schema, true));
      } catch (RuntimeException e) {
        // No logical form, so no provenance either: it stands for no version.
        known = Optional.empty();
      }
      logicalTypes.put(schema, known);
    }
    return known;
  }

  // A soft-deleted version is still a version: old records and pinned readers use them.
  private SchemaMetadata metadataOf(String subject, int version)
      throws IOException, RestClientException {
    return client.getSchemaMetadata(subject, version, true);
  }

  // The registered version a caller named for a reader: a schema id, or a subject version.
  static final class Pin {
    private final Integer id;
    private final String subject;
    private final Integer version;

    Pin(Integer id, String subject, Integer version) {
      this.id = id;
      this.subject = subject;
      this.version = version;
    }

    @Override
    public boolean equals(Object o) {
      if (!(o instanceof Pin)) {
        return false;
      }
      Pin pin = (Pin) o;
      return Objects.equals(id, pin.id) && Objects.equals(subject, pin.subject)
          && Objects.equals(version, pin.version);
    }

    @Override
    public int hashCode() {
      return Objects.hash(id, subject, version);
    }

    @Override
    public String toString() {
      return version != null ? "version " + version : "schema id " + id;
    }
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
        // A fresh one each time: a caller may add to it, and it is shared by every record. Its
        // cause is the failure's own, so the chain says the message once; one without is kept.
        throw new SerializationException(failure.getMessage(),
            failure.getCause() != null ? failure.getCause() : failure);
      }
      return Optional.ofNullable(value);
    }
  }
}
