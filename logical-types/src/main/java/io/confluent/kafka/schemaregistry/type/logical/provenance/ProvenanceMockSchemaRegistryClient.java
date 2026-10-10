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

package io.confluent.kafka.schemaregistry.type.logical.provenance;

import io.confluent.kafka.schemaregistry.ParsedSchema;
import io.confluent.kafka.schemaregistry.ParsedSchemaHolder;
import io.confluent.kafka.schemaregistry.SchemaProvider;
import io.confluent.kafka.schemaregistry.client.MockSchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.SchemaMetadata;
import io.confluent.kafka.schemaregistry.client.rest.entities.ProvenanceAlgorithm;
import io.confluent.kafka.schemaregistry.client.rest.entities.Schema;
import io.confluent.kafka.schemaregistry.client.rest.entities.SchemaProvenance;
import io.confluent.kafka.schemaregistry.client.rest.entities.requests.RegisterSchemaResponse;
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;
import io.confluent.kafka.schemaregistry.type.logical.TypeTooDeepException;
import io.confluent.kafka.schemaregistry.type.logical.ValidationException;
import io.confluent.kafka.schemaregistry.utils.JacksonMapper;
import io.confluent.kafka.schemaregistry.utils.QualifiedSubject;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.OptionalInt;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.concurrent.ConcurrentHashMap;

/**
 * A {@link MockSchemaRegistryClient} that also answers the provenance endpoint, as the registry
 * does: through {@link ProvenanceHistory}, with the registry's own error codes, so a caller tested
 * against it is tested against the registry's behaviour and failure modes.
 *
 * <p>It lives here rather than in the client because computing provenance needs this module, and
 * this module depends on the client. A soft-deleted version stays in the history, and is found
 * when deleted versions are looked up, as in the registry. The base mock numbers versions by the
 * live ones alone, so a version registered after the latest was deleted takes its number, and
 * replaces it here; a permanent delete forgets a soft-deleted version here, though the base
 * mock still resolves its schema id; and a deleted subject, soft or not, is forgotten entirely, as
 * the base mock forgets it and its schemas. A soft-deleted schema registered again keeps its id
 * and its soft-deleted version, which models neither of the registry's policies since #4658:
 * without LOGICAL the registry tombstones the soft-deleted version, and under LOGICAL it gives
 * the schema a new id.
 */
public class ProvenanceMockSchemaRegistryClient extends MockSchemaRegistryClient {

  // The registry's error codes, from its Errors class, which this module cannot see.
  private static final int SUBJECT_NOT_FOUND = 40401;
  private static final int VERSION_NOT_FOUND = 40402;
  private static final int SCHEMA_ID_NOT_IN_SUBJECT = 40411;
  private static final int INVALID_SCHEMA = 42201;
  private static final int INVALID_VERSION = 42202;
  private static final int RECURSIVE_SCHEMA = 42220;
  private static final int UNRESOLVABLE_REFERENCE = 42214;
  private static final int UNKNOWN_ALGORITHM = 42216;
  private static final int AMBIGUOUS_PROVENANCE = 42217;
  private static final int PROVENANCE_TOO_LARGE = 42218;
  private static final int PROVENANCE_RANGE_TOO_LONG = 42219;
  // The registry's default for provenance.interior.max.versions.
  private static final int INTERIOR_MAX_VERSIONS = 100;

  // The registry's provenance.interior.max.versions; a test may lower it to page short histories.
  private final int interiorMaxVersions;
  // The registry's generic server error carries its HTTP status as its error code.
  private static final int SERVER_ERROR = 500;

  // Soft-deleted versions, by subject: version to schema id. The base mock forgets them.
  private final Map<String, Map<Integer, Integer>> softDeleted = new ConcurrentHashMap<>();

  public ProvenanceMockSchemaRegistryClient() {
    this(INTERIOR_MAX_VERSIONS);
  }

  /**
   * As the registry with {@code provenance.interior.max.versions} set to
   * {@code interiorMaxVersions}.
   */
  public ProvenanceMockSchemaRegistryClient(int interiorMaxVersions) {
    super();
    this.interiorMaxVersions = interiorMaxVersions;
  }

  public ProvenanceMockSchemaRegistryClient(List<SchemaProvider> providers) {
    super(providers);
    this.interiorMaxVersions = INTERIOR_MAX_VERSIONS;
  }

  /**
   * As the registry answers a version the subject lacks: 40402, where the base mock says 40401,
   * the answer for a subject it lacks.
   */
  @Override
  public SchemaMetadata getSchemaMetadata(String subject, int version,
      boolean lookupDeletedSchema) throws IOException, RestClientException {
    try {
      return super.getSchemaMetadata(subject, version, lookupDeletedSchema);
    } catch (RestClientException e) {
      Integer deleted = lookupDeletedSchema ? softDeletedOf(subject).get(version) : null;
      if (deleted != null) {
        return new SchemaMetadata(new Schema(subject, version, deleted,
            getSchemaBySubjectAndId(subject, deleted)));
      }
      if (e.getErrorCode() == 40401 && !getAllVersions(subject).isEmpty()) {
        throw new RestClientException("Version " + version + " not found.", 404, 40402);
      }
      throw e;
    }
  }

  /**
   * As the registry lists them: with {@code lookupDeletedSchema}, soft-deleted ones too.
   */
  @Override
  public List<Integer> getAllVersions(String subject, boolean lookupDeletedSchema)
      throws IOException, RestClientException {
    if (!lookupDeletedSchema) {
      return getAllVersions(subject);
    }
    Set<Integer> versions = new TreeSet<>(softDeletedOf(subject).keySet());
    try {
      versions.addAll(getAllVersions(subject));
    } catch (RestClientException e) {
      if (versions.isEmpty()) {
        throw e;
      }
    }
    return new ArrayList<>(versions);
  }

  /**
   * As the registry looks a schema's id up: soft-deleted versions skipped, where the base mock
   * still finds them.
   */
  @Override
  public RegisterSchemaResponse getIdWithResponse(String subject, ParsedSchema schema,
      boolean normalize) throws IOException, RestClientException {
    RegisterSchemaResponse response = super.getIdWithResponse(subject, schema, normalize);
    if (softDeletedOf(subject).containsValue(response.getId())
        && liveVersion(subject, schema, normalize) == null) {
      throw new RestClientException("Schema not found", 404, 40403);
    }
    return response;
  }

  /**
   * As the registry looks a schema's version up: soft-deleted versions included, where the base
   * mock forgets them. Of several, the latest.
   */
  @Override
  public int getVersion(String subject, ParsedSchema schema, boolean normalize)
      throws IOException, RestClientException {
    Integer live = liveVersion(subject, schema, normalize);
    if (live != null) {
      return live;
    }
    int id = super.getIdWithResponse(subject, schema, normalize).getId();
    Integer found = null;
    for (Map.Entry<Integer, Integer> deleted : softDeletedOf(subject).entrySet()) {
      if (deleted.getValue() == id) {
        found = deleted.getKey();
      }
    }
    if (found == null) {
      throw new RestClientException("Subject Not Found", 404, 40401);
    }
    return found;
  }

  private Integer liveVersion(String subject, ParsedSchema schema, boolean normalize)
      throws IOException, RestClientException {
    try {
      return super.getVersion(subject, schema, normalize);
    } catch (RestClientException e) {
      if (e.getStatus() != 404) {
        throw e;
      }
      return null;
    }
  }

  @Override
  public synchronized Integer deleteSchemaVersion(Map<String, String> requestProperties,
      String subject, String version, boolean isPermanent) throws IOException, RestClientException {
    Map<Integer, Integer> deleted = softDeleted.computeIfAbsent(subject, s -> new TreeMap<>());
    int number = Integer.parseInt(version);
    Integer id = null;
    try {
      id = getByVersion(subject, number, false).getId();
    } catch (RuntimeException e) {
      // No live version: the base mock answers -1.
    }
    if (id == null && isPermanent && deleted.remove(number) != null) {
      return number;
    }
    Integer result = super.deleteSchemaVersion(requestProperties, subject, version, isPermanent);
    if (isPermanent) {
      return result;
    }
    if (id != null && result == number) {
      deleted.put(number, id);
    }
    return result;
  }

  @Override
  public synchronized List<Integer> deleteSubject(Map<String, String> requestProperties,
      String subject, boolean isPermanent) throws IOException, RestClientException {
    List<Integer> deleted = super.deleteSubject(requestProperties, subject, isPermanent);
    softDeleted.remove(subject);
    return deleted;
  }

  @Override
  public synchronized void reset() {
    super.reset();
    softDeleted.clear();
  }

  @Override
  public SchemaProvenance getProvenanceById(String subject, int fromId, int toId,
      boolean includeInterior, boolean includeMultipleMessages,
      String algorithm) throws IOException, RestClientException {
    // As the registry: the request checked before the history, the subject as it names it.
    checkAlgorithm(algorithm);
    List<SchemaMetadata> history = historyAsNamed(subject);
    subject = QualifiedSubject.normalize(QualifiedSubject.DEFAULT_TENANT, subject);
    return provenance(subject, history,
        carrying(history, fromId, subject), carrying(history, toId, subject),
        includeInterior, includeMultipleMessages, algorithm);
  }

  @Override
  public SchemaProvenance getProvenanceByVersion(String subject, String fromVersion,
      String toVersion, boolean includeInterior,
      boolean includeMultipleMessages, String algorithm) throws IOException, RestClientException {
    // As the registry: the request checked before the history, the subject as it names it.
    checkAlgorithm(algorithm);
    List<SchemaMetadata> history = historyAsNamed(subject);
    subject = QualifiedSubject.normalize(QualifiedSubject.DEFAULT_TENANT, subject);
    return provenance(subject, history, named(history, fromVersion), named(history, toVersion),
        includeInterior, includeMultipleMessages, algorithm);
  }

  @Override
  public SchemaProvenance getProvenanceToVersion(String subject, int fromId, int toVersion,
      boolean includeInterior, boolean includeMultipleMessages,
      String algorithm) throws IOException, RestClientException {
    // As the registry: the request checked before the history, the subject as it names it.
    checkAlgorithm(algorithm);
    List<SchemaMetadata> history = historyAsNamed(subject);
    subject = QualifiedSubject.normalize(QualifiedSubject.DEFAULT_TENANT, subject);
    return provenance(subject, history, carrying(history, fromId, subject),
        named(history, String.valueOf(toVersion)), includeInterior, includeMultipleMessages,
        algorithm);
  }

  // The subject's history as the registry names it, without the default context's prefix; else as
  // given, as the base mock keeps a subject registered under the prefix as spelled.
  private List<SchemaMetadata> historyAsNamed(String subject)
      throws IOException, RestClientException {
    String named = QualifiedSubject.normalize(QualifiedSubject.DEFAULT_TENANT, subject);
    try {
      return history(named);
    } catch (RestClientException e) {
      if (named.equals(subject) || e.getErrorCode() != SUBJECT_NOT_FOUND) {
        throw e;
      }
      return history(subject);
    }
  }

  private static void checkAlgorithm(String algorithm) throws RestClientException {
    if (!ProvenanceAlgorithm.isDynamic(algorithm)) {
      try {
        ProvenanceAlgorithm.of(algorithm);
      } catch (IllegalArgumentException e) {
        throw new RestClientException(e.getMessage(), 422, UNKNOWN_ALGORITHM);
      }
    }
  }

  private SchemaProvenance provenance(String subject, List<SchemaMetadata> history,
      int from, int to, boolean includeInterior,
      boolean includeMultipleMessages, String algorithm) throws IOException, RestClientException {
    List<SchemaMetadata> range = ProvenanceHistory.range(history, from, to);
    if (includeInterior && range.size() > interiorMaxVersions) {
      throw new RestClientException("The range covers " + range.size() + " versions, more than "
          + interiorMaxVersions + " with includeInterior", 422, PROVENANCE_RANGE_TOO_LONG);
    }
    // As the registry does: each version parsed when the computation reaches it.
    List<ParsedSchemaHolder> schemas = new ArrayList<>(range.size());
    for (SchemaMetadata entry : range) {
      schemas.add(new ParsedSchemaHolder() {
        @Override
        public ParsedSchema schema() {
          try {
            return getSchemaBySubjectAndId(subject, entry.getId());
          } catch (IOException | RestClientException | RuntimeException e) {
            throw new Unparsable("Version " + entry.getVersion() + " of subject " + subject
                + " could not be parsed: " + e.getMessage());
          }
        }

        @Override
        public void clear() {
        }
      });
    }
    SchemaProvenance whole;
    try {
      whole = compute(subject, range, schemas, includeMultipleMessages, includeInterior,
          algorithm);
    } catch (Unparsable e) {
      throw new RestClientException(e.getMessage(), 422, UNRESOLVABLE_REFERENCE);
    } catch (RecursiveTypeException e) {
      throw new RestClientException(e.getMessage(), 422, RECURSIVE_SCHEMA);
    } catch (AmbiguousProvenanceException e) {
      throw new RestClientException(e.getMessage(), 422, AMBIGUOUS_PROVENANCE);
    } catch (TooManyLocationsException | TypeTooDeepException e) {
      throw new RestClientException(e.getMessage(), 422, PROVENANCE_TOO_LARGE);
    } catch (UnsupportedProvenanceAlgorithmException e) {
      throw new RestClientException(e.getMessage(), 422, UNKNOWN_ALGORITHM);
    } catch (ValidationException e) {
      throw new RestClientException(e.getMessage(), 422, INVALID_SCHEMA);
    } catch (RuntimeException e) {
      // As the registry does: any other computation failure is a server error.
      throw new RestClientException(String.valueOf(e.getMessage()), 500, SERVER_ERROR);
    }
    return onTheWire(ProvenanceHistory.slice(whole, from, to, includeInterior));
  }

  // As a client receives it: through the JSON the registry serves, so a test sees what the wire
  // drops or keeps.
  private static SchemaProvenance onTheWire(SchemaProvenance provenance) throws IOException {
    return JacksonMapper.INSTANCE.readValue(
        JacksonMapper.INSTANCE.writeValueAsString(provenance), SchemaProvenance.class);
  }

  /**
   * The provenance of {@code range}, as the registry computes it; overridable so a test can make
   * the computation fail.
   */
  protected SchemaProvenance compute(String subject, List<SchemaMetadata> range,
      List<? extends ParsedSchemaHolder> schemas, boolean includeMultipleMessages,
      boolean includeInterior, String algorithm) {
    return ProvenanceHistory.compute(subject, range, schemas, includeMultipleMessages,
        includeInterior, algorithm);
  }

  // A version that could not be parsed, met during the computation.
  private static final class Unparsable extends RuntimeException {
    private static final long serialVersionUID = 1L;

    private Unparsable(String message) {
      super(message);
    }
  }

  private List<SchemaMetadata> history(String subject)
      throws IOException, RestClientException {
    Map<Integer, SchemaMetadata> entries = new TreeMap<>();
    for (Map.Entry<Integer, Integer> deleted : softDeletedOf(subject).entrySet()) {
      int id = deleted.getValue();
      Schema schema = new Schema(subject, deleted.getKey(), id,
          getSchemaBySubjectAndId(subject, id));
      schema.setDeleted(true);
      entries.put(deleted.getKey(), new SchemaMetadata(schema));
    }
    List<Integer> live;
    try {
      live = getAllVersions(subject);
    } catch (RestClientException e) {
      // Every version soft-deleted: the base mock knows none.
      live = Collections.emptyList();
    }
    for (int version : live) {
      Schema schema = getByVersion(subject, version, false);
      entries.put(version, new SchemaMetadata(schema));
    }
    List<SchemaMetadata> history = new ArrayList<>(entries.values());
    if (history.isEmpty()) {
      throw new RestClientException("Subject '" + subject + "' not found.", 404,
          SUBJECT_NOT_FOUND);
    }
    return history;
  }

  // A copy, taken under the lock deletes are made under.
  private synchronized Map<Integer, Integer> softDeletedOf(String subject) {
    return new TreeMap<>(softDeleted.getOrDefault(subject, Collections.emptyMap()));
  }

  private static int named(List<SchemaMetadata> history, String version)
      throws RestClientException {
    OptionalInt resolved;
    if ("latest".equalsIgnoreCase(version) || "-1".equals(version)) {
      resolved = ProvenanceHistory.latestVersion(history);
    } else {
      int number;
      try {
        number = Integer.parseInt(version);
      } catch (NumberFormatException e) {
        throw new RestClientException("The specified version '" + version
            + "' is not a valid version id.", 422, INVALID_VERSION);
      }
      if (number < 1) {
        throw new RestClientException("The specified version '" + version
            + "' is not a valid version id.", 422, INVALID_VERSION);
      }
      resolved = ProvenanceHistory.version(history, number);
    }
    if (!resolved.isPresent()) {
      throw new RestClientException("Version " + version + " not found.", 404,
          VERSION_NOT_FOUND);
    }
    return resolved.getAsInt();
  }

  private static int carrying(List<SchemaMetadata> history, int schemaId,
      String subject) throws RestClientException {
    OptionalInt version = ProvenanceHistory.versionCarrying(history, schemaId);
    if (!version.isPresent()) {
      throw new RestClientException("Schema id " + schemaId + " has no version under subject '"
          + subject + "'.", 404, SCHEMA_ID_NOT_IN_SUBJECT);
    }
    return version.getAsInt();
  }
}
