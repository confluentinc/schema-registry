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


  private final SchemaRegistryClient client;
  private final String algorithm;
  private final Cache<List<Object>, Outcome<T>> outcomes;
  private final Cache<List<Object>, Optional<Integer>> readerIds;

  /**
   * A projector asking {@code client} by {@code algorithm}, caching up to {@code cacheSize}
   * entries of each kind for {@code cacheTtlSec} seconds, or indefinitely when that is negative.
   */
  public ProvenanceProjector(SchemaRegistryClient client, String algorithm, int cacheSize,
      int cacheTtlSec) {
    this.client = client;
    this.algorithm = algorithm;
    this.outcomes = cache(cacheSize, cacheTtlSec);
    this.readerIds = cache(cacheSize, cacheTtlSec);
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
    if (subject == null || writerId == null || writerId.getId() == null || reader == null) {
      return Optional.empty();
    }
    List<Object> key = Arrays.asList(subject, writerId.getId(), reader, includeMultipleMessages);
    Outcome<T> outcome = outcomes.getIfPresent(key);
    if (outcome == null) {
      outcome = compute(subject, writerId.getId(), writer, reader, includeMultipleMessages, build);
      outcomes.put(key, outcome);
    }
    return outcome.get();
  }

  private Outcome<T> compute(String subject, int writerId, ParsedSchema writer,
      ParsedSchema reader, boolean includeMultipleMessages, Function<ProvenanceMapping, T> build) {
    try {
      Integer readerId = readerId(subject, reader);
      if (readerId == null) {
        throw new ProvenanceUnavailableException(
            "The reader schema is not a version of subject " + subject);
      }
      if (readerId == writerId) {
        return Outcome.unavailable();
      }
      SchemaProvenance provenance = client.getProvenanceById(
          subject, writerId, readerId, false, true, includeMultipleMessages, algorithm);
      return Outcome.of(build.apply(ProvenanceMapping.join(provenance, writerId, readerId)));
    } catch (IOException e) {
      throw new SerializationException(
          "Could not reach Schema Registry for the provenance of schema id " + writerId, e);
    } catch (RestClientException e) {
      if (isTransient(e.getStatus())) {
        throw new SerializationException(
            "Schema Registry could not serve provenance for schema id " + writerId, e);
      }
      return unavailable(subject, writerId, e);
    } catch (ProvenanceUnavailableException | UnsupportedOperationException e) {
      return unavailable(subject, writerId, e);
    } catch (RuntimeException e) {
      return Outcome.failed(e instanceof SerializationException
          ? (SerializationException) e
          : new SerializationException("Could not project schema id " + writerId
              + " by provenance: " + e.getMessage(), e));
    }
  }

  private Outcome<T> unavailable(String subject, int writerId, Exception e) {
    // Logged here, where the outcome is cached, so once per writer schema, not per record.
    log.warn("No provenance for schema id {} of subject {}; reading it without provenance. {}",
        writerId, subject, e.getMessage());
    return Outcome.unavailable();
  }

  /**
   * The schema id of {@code reader} under {@code subject}: the registered version it is, or else
   * the latest version it equals once metadata, rules and inline tags are set aside. A reader with
   * a writer's rules merged onto it has the same structure as the version it was pinned to, and
   * provenance depends on structure alone. Null when no version matches.
   */
  private Integer readerId(String subject, ParsedSchema reader)
      throws IOException, RestClientException {
    List<Object> key = Arrays.asList(subject, reader);
    Optional<Integer> cached = readerIds.getIfPresent(key);
    if (cached != null) {
      return cached.orElse(null);
    }
    Integer id;
    try {
      id = client.getId(subject, reader);
    } catch (RestClientException e) {
      if (e.getStatus() != 404) {
        throw e;
      }
      id = structuralMatch(subject, reader);
    }
    readerIds.put(key, Optional.ofNullable(id));
    return id;
  }

  private Integer structuralMatch(String subject, ParsedSchema reader)
      throws IOException, RestClientException {
    String wanted = structure(reader);
    List<Integer> versions;
    try {
      versions = client.getAllVersions(subject, true);
    } catch (UnsupportedOperationException e) {
      versions = client.getAllVersions(subject);
    }
    for (int i = versions.size() - 1; i >= 0; i--) {
      int id = schemaIdOf(subject, versions.get(i));
      if (wanted.equals(structure(client.getSchemaBySubjectAndId(subject, id)))) {
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

  private static String structure(ParsedSchema schema) {
    ParsedSchema bare = schema.copy((Metadata) null, (RuleSet) null);
    return bare.copy(Collections.emptyMap(), bare.inlineTaggedEntities()).canonicalString();
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
