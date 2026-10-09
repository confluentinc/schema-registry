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

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.rest.entities.ProvenanceField;
import io.confluent.kafka.schemaregistry.client.rest.entities.ProvenanceVersion;
import io.confluent.kafka.schemaregistry.client.rest.entities.SchemaProvenance;
import io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig;
import io.confluent.kafka.serializers.provenance.ProvenanceRetriableException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

/**
 * Pairs a writer with a reader by pids that are stable across versions, such as the Metastore's
 * column ids, rather than by Schema Registry's, which are comparable only within one response. The
 * registry still reports the versions, with their locations, names and kinds; a subclass looks up
 * each version's pids on their own, by path, and they replace the registry's.
 *
 * <p>The pids must be keyed by the paths provenance reports for the version in the mode asked
 * for, and be equal across versions exactly where a location continues. A Protobuf deserializer
 * picks the mode by its reader's file, so one subject may be asked in both: a single-message path
 * {@code p} is the multi-message path {@code [0] + p}, as each version is rooted at its file's
 * first message. Paths provenance does not report, such as collection nodes', are ignored. The
 * algorithm asked for is ignored too: the pids already decide every pairing, though
 * {@code provenance.algorithm} must still be set for a deserializer to read by provenance.
 *
 * <p>While a version waits for its pids, the registry's answer is kept, bounded by
 * {@code provenance.cache.size} and {@code provenance.cache.ttl.sec}, so a retried record asks
 * only the subclass again; once the pids arrive the registry is asked again, so the record pairs
 * by its answer of now. A subclass that overrides {@link #configure} calls
 * {@code super.configure}.
 */
public abstract class StablePidProvenanceStrategy extends ClientProvenanceStrategy {

  private Cache<List<Object>, SchemaProvenance> answers =
      cache(AbstractKafkaSchemaSerDeConfig.PROVENANCE_CACHE_SIZE_DEFAULT,
          AbstractKafkaSchemaSerDeConfig.PROVENANCE_CACHE_TTL_DEFAULT);

  @Override
  public void configure(Map<String, ?> configs) {
    Object size = configs.get(AbstractKafkaSchemaSerDeConfig.PROVENANCE_CACHE_SIZE);
    Object ttl = configs.get(AbstractKafkaSchemaSerDeConfig.PROVENANCE_CACHE_TTL);
    answers = cache(
        size != null ? Integer.parseInt(size.toString().trim())
            : AbstractKafkaSchemaSerDeConfig.PROVENANCE_CACHE_SIZE_DEFAULT,
        ttl != null ? Integer.parseInt(ttl.toString().trim())
            : AbstractKafkaSchemaSerDeConfig.PROVENANCE_CACHE_TTL_DEFAULT);
  }

  /**
   * The pid of each located path of {@code version} of {@code subject}, the paths as provenance
   * reports them with {@code includeMultipleMessages}; null if the version has none yet.
   *
   * @throws ProvenanceRetriableException if the pids cannot be looked up now: the record fails
   *     and the next asks again. Anything else means what {@link ProvenanceStrategy} lists: a
   *     ProvenanceUnavailableException reads the writer without provenance until the outcome
   *     expires, so it is never thrown for pids that are only late
   */
  protected abstract Map<List<Integer>, Integer> pids(String subject, int version,
      boolean includeMultipleMessages);

  @Override
  public SchemaProvenance provenance(SchemaRegistryClient client, String subject, int fromId,
      int toId, boolean includeInterior, boolean includeMultipleMessages, String algorithm) {
    // Only the ends are paired, and the registry's pids are replaced whatever its algorithm.
    return answered(Arrays.asList(subject, fromId, toId, null, includeMultipleMessages), subject,
        includeMultipleMessages, () -> super.provenance(
            client, subject, fromId, toId, false, includeMultipleMessages, null));
  }

  @Override
  public SchemaProvenance provenanceToVersion(SchemaRegistryClient client, String subject,
      int fromId, int toVersion, boolean includeInterior, boolean includeMultipleMessages,
      String algorithm) {
    return answered(Arrays.asList(subject, fromId, null, toVersion, includeMultipleMessages),
        subject, includeMultipleMessages, () -> super.provenanceToVersion(
            client, subject, fromId, toVersion, false, includeMultipleMessages, null));
  }

  // An answer is kept only while its versions wait for pids; once they have them, the registry is
  // asked again, so the record pairs by its answer of now, as one that never waited does.
  private SchemaProvenance answered(List<Object> key, String subject,
      boolean includeMultipleMessages, Supplier<SchemaProvenance> ask) {
    SchemaProvenance waiting = answers.getIfPresent(key);
    if (waiting != null) {
      restamped(subject, waiting, includeMultipleMessages);
      answers.invalidate(key);
    }
    SchemaProvenance answer = ask.get();
    try {
      return restamped(subject, answer, includeMultipleMessages);
    } catch (ProvenanceRetriableException e) {
      answers.put(key, answer);
      throw e;
    }
  }

  /**
   * {@code provenance} with each location's pid replaced by the one of its path, looked up by the
   * caller's {@code subject}: the registry answers with it normalized, such as without a default
   * context's prefix.
   *
   * @throws ProvenanceRetriableException if a version has no pids yet, as for a writer newer than
   *     the table: the record fails, and is read once the pids arrive
   * @throws IllegalStateException if a location has no pid: the subclass broke the contract, and
   *     every record of the writer fails
   */
  private SchemaProvenance restamped(String subject, SchemaProvenance provenance,
      boolean includeMultipleMessages) {
    List<ProvenanceVersion> versions = new ArrayList<>(provenance.getVersions().size());
    for (ProvenanceVersion version : provenance.getVersions()) {
      Map<List<Integer>, Integer> pids =
          pids(subject, version.getVersion(), includeMultipleMessages);
      if (pids == null) {
        throw new ProvenanceRetriableException(
            "Version " + version.getVersion() + " of subject " + subject + " has no pids yet");
      }
      List<ProvenanceField> fields = new ArrayList<>(version.getFields().size());
      for (ProvenanceField field : version.getFields()) {
        Integer pid = pids.get(field.getPath());
        if (pid == null) {
          throw new IllegalStateException("Location " + field.getPath() + " of version "
              + version.getVersion() + " of subject " + subject + " has no pid");
        }
        fields.add(new ProvenanceField(field.getPath(), field.getNames(), field.getKind(), pid));
      }
      versions.add(new ProvenanceVersion(
          version.getVersion(), version.getId(), version.getKind(), fields));
    }
    return new SchemaProvenance(subject, versions);
  }

  // As the deserializer's provenance caches: indefinitely when the TTL is negative.
  private static Cache<List<Object>, SchemaProvenance> cache(int size, int ttlSec) {
    CacheBuilder<Object, Object> builder = CacheBuilder.newBuilder().maximumSize(size);
    if (ttlSec >= 0) {
      builder = builder.expireAfterWrite(ttlSec, TimeUnit.SECONDS);
    }
    return builder.build();
  }
}
