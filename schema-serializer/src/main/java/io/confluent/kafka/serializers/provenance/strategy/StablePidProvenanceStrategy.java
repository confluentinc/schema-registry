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

import io.confluent.kafka.schemaregistry.ParsedSchema;
import io.confluent.kafka.schemaregistry.client.SchemaMetadata;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.rest.entities.ProvenanceField;
import io.confluent.kafka.schemaregistry.client.rest.entities.ProvenanceVersion;
import io.confluent.kafka.schemaregistry.client.rest.entities.SchemaProvenance;
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;
import io.confluent.kafka.schemaregistry.type.logical.ValidationException;
import io.confluent.kafka.schemaregistry.type.logical.provenance.ProvenanceHistory;
import io.confluent.kafka.schemaregistry.type.logical.provenance.RecursiveTypeException;
import io.confluent.kafka.schemaregistry.type.logical.provenance.TooManyLocationsException;
import io.confluent.kafka.serializers.provenance.ProvenanceRetriableException;
import io.confluent.kafka.serializers.provenance.ProvenanceUnavailableException;
import io.confluent.kafka.serializers.provenance.ProvenanceUnknownWriterException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.OptionalInt;

/**
 * Pairs a writer with a reader by pids that are stable across versions, such as the Metastore's
 * column ids, rather than by Schema Registry's, which are comparable only within one response. A
 * subclass looks up each version's pids on their own, by path; a schema id stands for the version
 * the registry resolves it to, and each version's locations, with their names and kinds, are
 * computed here, as Schema Registry's converters compute them.
 *
 * <p>The pids must be keyed by the paths provenance reports for the version, computed with the
 * {@code includeMultipleMessages} the deserializer uses, and be equal across versions exactly
 * where a location continues. Paths provenance does not report, such as collection nodes', are
 * ignored. The algorithm asked for is ignored too: the pids already decide every pairing, though
 * {@code provenance.algorithm} must still be set for a deserializer to read by provenance.
 */
public abstract class StablePidProvenanceStrategy implements ProvenanceStrategy {

  @Override
  public void configure(Map<String, ?> configs) {
  }

  /**
   * The pid of each located path of {@code version} of {@code subject}; null if the version has
   * none yet.
   */
  protected abstract Map<List<Integer>, Integer> pids(String subject, int version);

  @Override
  public SchemaProvenance provenance(SchemaRegistryClient client, String subject, int fromId,
      int toId, boolean includeInterior, boolean includeMultipleMessages, String algorithm) {
    String pair = "schema ids " + fromId + " and " + toId + " of subject " + subject;
    try {
      return between(client, subject, versionOf(client, subject, fromId),
          versionOf(client, subject, toId), includeMultipleMessages);
    } catch (IOException e) {
      throw new ProvenanceRetriableException("Could not reach Schema Registry", e);
    } catch (RestClientException e) {
      throw ClientProvenanceStrategy.translate(e, pair);
    }
  }

  @Override
  public SchemaProvenance provenanceToVersion(SchemaRegistryClient client, String subject,
      int fromId, int toVersion, boolean includeInterior, boolean includeMultipleMessages,
      String algorithm) {
    String pair = "schema id " + fromId + " and version " + toVersion + " of subject " + subject;
    try {
      return between(client, subject, versionOf(client, subject, fromId), toVersion,
          includeMultipleMessages);
    } catch (IOException e) {
      throw new ProvenanceRetriableException("Could not reach Schema Registry", e);
    } catch (RestClientException e) {
      throw ClientProvenanceStrategy.translate(e, pair);
    }
  }

  /**
   * The version carrying {@code schemaId}, as the endpoint resolves it: the latest, soft-deleted
   * versions included, whether or not it has pids yet.
   *
   * @throws ProvenanceUnknownWriterException if no version of the subject carries it
   */
  private static int versionOf(SchemaRegistryClient client, String subject, int schemaId)
      throws IOException, RestClientException {
    List<SchemaMetadata> history = new ArrayList<>();
    for (int version : client.getAllVersions(subject, true)) {
      history.add(client.getSchemaMetadata(subject, version, true));
    }
    OptionalInt version = ProvenanceHistory.versionCarrying(history, schemaId);
    if (!version.isPresent()) {
      throw new ProvenanceUnknownWriterException(
          "Schema id " + schemaId + " is no version of subject " + subject);
    }
    return version.getAsInt();
  }

  // Both ends in version order, or one when they are the same version, as the endpoint answers.
  private SchemaProvenance between(SchemaRegistryClient client, String subject, int from,
      int to, boolean includeMultipleMessages)
      throws IOException, RestClientException {
    List<ProvenanceVersion> versions = new ArrayList<>(2);
    if (from == to) {
      versions.add(located(client, subject, from, includeMultipleMessages));
    } else if (from < to) {
      versions.add(located(client, subject, from, includeMultipleMessages));
      versions.add(located(client, subject, to, includeMultipleMessages));
    } else {
      versions.add(located(client, subject, to, includeMultipleMessages));
      versions.add(located(client, subject, from, includeMultipleMessages));
    }
    return new SchemaProvenance(subject, versions);
  }

  /**
   * {@code version}'s locations, computed from its schema, each with the pid of its path.
   *
   * @throws ProvenanceRetriableException if the version has no pids yet, as for a writer newer
   *     than the table: the record fails, and is read once the pids arrive
   * @throws IllegalStateException if a location has no pid: the subclass broke the contract, and
   *     every record of the writer fails
   */
  private ProvenanceVersion located(SchemaRegistryClient client, String subject, int version,
      boolean includeMultipleMessages) throws IOException, RestClientException {
    int id = client.getSchemaMetadata(subject, version, true).getId();
    Map<List<Integer>, Integer> pids = pids(subject, version);
    if (pids == null) {
      throw new ProvenanceRetriableException(
          "Version " + version + " of subject " + subject + " has no pids yet");
    }
    ParsedSchema schema = client.getSchemaBySubjectAndId(subject, id);
    ProvenanceVersion computed;
    try {
      computed = ProvenanceHistory.locations(subject, version, id, schema,
          includeMultipleMessages);
    } catch (RecursiveTypeException | TooManyLocationsException | ValidationException e) {
      // As the endpoint's 42220, 42218 and 42201: no provenance for the version.
      throw new ProvenanceUnavailableException(e.getMessage(), e);
    }
    List<ProvenanceField> fields = new ArrayList<>(computed.getFields().size());
    for (ProvenanceField field : computed.getFields()) {
      Integer pid = pids.get(field.getPath());
      if (pid == null) {
        throw new IllegalStateException("Location " + field.getPath() + " of version " + version
            + " of subject " + subject + " has no pid");
      }
      fields.add(new ProvenanceField(field.getPath(), field.getNames(), field.getKind(), pid));
    }
    return new ProvenanceVersion(version, id, computed.getKind(), fields);
  }
}
