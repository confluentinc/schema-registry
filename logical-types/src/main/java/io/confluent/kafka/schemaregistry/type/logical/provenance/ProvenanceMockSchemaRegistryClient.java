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
import io.confluent.kafka.schemaregistry.SchemaProvider;
import io.confluent.kafka.schemaregistry.client.MockSchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.rest.entities.Schema;
import io.confluent.kafka.schemaregistry.client.rest.entities.SchemaProvenance;
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;
import io.confluent.kafka.schemaregistry.type.logical.ValidationException;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.OptionalInt;

/**
 * A {@link MockSchemaRegistryClient} that also answers the provenance endpoint, as the registry
 * does: through {@link ProvenanceHistory}, with the registry's own error codes, so a caller tested
 * against it is tested against the registry's behaviour and failure modes.
 *
 * <p>It lives here rather than in the client because computing provenance needs this module, and
 * this module depends on the client. The one difference from the registry is the mock's own: it
 * forgets a soft-deleted version entirely, so a history here never holds one.
 */
public class ProvenanceMockSchemaRegistryClient extends MockSchemaRegistryClient {

  // The registry's error codes, from its Errors class, which this module cannot see.
  private static final int SUBJECT_NOT_FOUND = 40401;
  private static final int VERSION_NOT_FOUND = 40402;
  private static final int SCHEMA_ID_NOT_IN_SUBJECT = 40411;
  private static final int INVALID_SCHEMA = 42201;
  private static final int INVALID_VERSION = 42202;
  private static final int RECURSIVE_SCHEMA = 42213;
  private static final int UNRESOLVABLE_REFERENCE = 42214;
  private static final int UNKNOWN_ALGORITHM = 42216;
  // The registry's generic server error carries its HTTP status as its error code.
  private static final int SERVER_ERROR = 500;

  public ProvenanceMockSchemaRegistryClient() {
    super();
  }

  public ProvenanceMockSchemaRegistryClient(List<SchemaProvider> providers) {
    super(providers);
  }

  @Override
  public SchemaProvenance getProvenanceById(String subject, int fromId, int toId,
      boolean includeInterior, boolean includeMultipleMessages,
      String algorithm) throws IOException, RestClientException {
    List<ProvenanceHistory.Entry> history = history(subject);
    return provenance(subject, history,
        carrying(history, fromId, subject), carrying(history, toId, subject),
        includeInterior, includeMultipleMessages, algorithm);
  }

  @Override
  public SchemaProvenance getProvenanceByVersion(String subject, String fromVersion,
      String toVersion, boolean includeInterior,
      boolean includeMultipleMessages, String algorithm) throws IOException, RestClientException {
    List<ProvenanceHistory.Entry> history = history(subject);
    return provenance(subject, history, named(history, fromVersion), named(history, toVersion),
        includeInterior, includeMultipleMessages, algorithm);
  }

  private SchemaProvenance provenance(String subject, List<ProvenanceHistory.Entry> history,
      int from, int to, boolean includeInterior,
      boolean includeMultipleMessages, String algorithm) throws IOException, RestClientException {
    ProvenanceAlgorithm version;
    try {
      version = ProvenanceAlgorithm.of(algorithm);
    } catch (IllegalArgumentException e) {
      throw new RestClientException(e.getMessage(), 422, UNKNOWN_ALGORITHM);
    }
    List<ProvenanceHistory.Entry> range = ProvenanceHistory.range(history, from, to);
    List<ParsedSchema> schemas = new ArrayList<>(range.size());
    for (ProvenanceHistory.Entry entry : range) {
      try {
        schemas.add(getSchemaBySubjectAndId(subject, entry.getSchemaId()));
      } catch (RuntimeException e) {
        throw new RestClientException("Version " + entry.getVersion() + " of subject " + subject
            + " could not be parsed: " + e.getMessage(), 422, UNRESOLVABLE_REFERENCE);
      }
    }
    SchemaProvenance whole;
    try {
      whole = compute(subject, range, schemas, includeMultipleMessages, version);
    } catch (RecursiveTypeException e) {
      throw new RestClientException(e.getMessage(), 422, RECURSIVE_SCHEMA);
    } catch (ValidationException e) {
      throw new RestClientException(e.getMessage(), 422, INVALID_SCHEMA);
    } catch (RuntimeException e) {
      // As the registry does: any other computation failure is a server error.
      throw new RestClientException(String.valueOf(e.getMessage()), 500, SERVER_ERROR);
    }
    return ProvenanceHistory.slice(whole, from, to, includeInterior);
  }

  /**
   * The provenance of {@code range}, as the registry computes it; overridable so a test can make
   * the computation fail.
   */
  protected SchemaProvenance compute(String subject, List<ProvenanceHistory.Entry> range,
      List<ParsedSchema> schemas, boolean includeMultipleMessages, ProvenanceAlgorithm algorithm) {
    return ProvenanceHistory.compute(subject, range, schemas, includeMultipleMessages, algorithm);
  }

  private List<ProvenanceHistory.Entry> history(String subject)
      throws IOException, RestClientException {
    List<ProvenanceHistory.Entry> history = new ArrayList<>();
    for (int version : getAllVersions(subject)) {
      Schema schema = getByVersion(subject, version, false);
      history.add(new ProvenanceHistory.Entry(version, schema.getId(), false));
    }
    if (history.isEmpty()) {
      throw new RestClientException("Subject '" + subject + "' not found.", 404,
          SUBJECT_NOT_FOUND);
    }
    return history;
  }

  private static int named(List<ProvenanceHistory.Entry> history, String version)
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
      resolved = ProvenanceHistory.version(history, number);
    }
    if (!resolved.isPresent()) {
      throw new RestClientException("Version " + version + " not found.", 404,
          VERSION_NOT_FOUND);
    }
    return resolved.getAsInt();
  }

  private static int carrying(List<ProvenanceHistory.Entry> history, int schemaId,
      String subject) throws RestClientException {
    OptionalInt version = ProvenanceHistory.versionCarrying(history, schemaId);
    if (!version.isPresent()) {
      throw new RestClientException("Schema id " + schemaId + " has no version under subject '"
          + subject + "'.", 404, SCHEMA_ID_NOT_IN_SUBJECT);
    }
    return version.getAsInt();
  }
}
