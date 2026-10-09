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

import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.rest.entities.ProvenanceField;
import io.confluent.kafka.schemaregistry.client.rest.entities.ProvenanceVersion;
import io.confluent.kafka.schemaregistry.client.rest.entities.SchemaProvenance;
import io.confluent.kafka.serializers.provenance.ProvenanceRetriableException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Pairs a writer with a reader by pids that are stable across versions, such as the Metastore's
 * column ids, rather than by Schema Registry's, which are comparable only within one response. The
 * registry still reports the versions, with their locations, names and kinds; a subclass looks up
 * each version's pids on their own, by path, and they replace the registry's.
 *
 * <p>The pids must be keyed by the paths provenance reports for the version, computed with the
 * {@code includeMultipleMessages} the deserializer uses, and be equal across versions exactly
 * where a location continues. Paths provenance does not report, such as collection nodes', are
 * ignored. The algorithm asked for is ignored too: the pids already decide every pairing, though
 * {@code provenance.algorithm} must still be set for a deserializer to read by provenance.
 */
public abstract class StablePidProvenanceStrategy extends ClientProvenanceStrategy {

  /**
   * The pid of each located path of {@code version} of {@code subject}; null if the version has
   * none yet.
   */
  protected abstract Map<List<Integer>, Integer> pids(String subject, int version);

  @Override
  public SchemaProvenance provenance(SchemaRegistryClient client, String subject, int fromId,
      int toId, boolean includeInterior, boolean includeMultipleMessages, String algorithm) {
    // Only the ends are paired, and the registry's pids are replaced whatever its algorithm.
    return restamped(super.provenance(
        client, subject, fromId, toId, false, includeMultipleMessages, null));
  }

  @Override
  public SchemaProvenance provenanceToVersion(SchemaRegistryClient client, String subject,
      int fromId, int toVersion, boolean includeInterior, boolean includeMultipleMessages,
      String algorithm) {
    return restamped(super.provenanceToVersion(
        client, subject, fromId, toVersion, false, includeMultipleMessages, null));
  }

  /**
   * {@code provenance} with each location's pid replaced by the one of its path.
   *
   * @throws ProvenanceRetriableException if a version has no pids yet, as for a writer newer than
   *     the table: the record fails, and is read once the pids arrive
   * @throws IllegalStateException if a location has no pid: the subclass broke the contract, and
   *     every record of the writer fails
   */
  private SchemaProvenance restamped(SchemaProvenance provenance) {
    String subject = provenance.getSubject();
    List<ProvenanceVersion> versions = new ArrayList<>(provenance.getVersions().size());
    for (ProvenanceVersion version : provenance.getVersions()) {
      Map<List<Integer>, Integer> pids = pids(subject, version.getVersion());
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
}
